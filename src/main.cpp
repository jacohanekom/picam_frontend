/**
 * picam-frontend
 * ==============
 * Single web UI for viewing one or more picam-orchestrator backends (each
 * running on its own Pi). Reads a static list of Pi addresses from
 * config, serves the browser page, and proxies MJPEG frames + status
 * JSON from whichever Pi+stream the viewer has selected — the browser
 * only ever talks to this one process, never directly to a Pi.
 *
 * Routes:
 *   GET /                          → web UI
 *   GET /stream?pi=X&stream=Y      → proxies http://<pi X host>/stream?stream=Y
 *   GET /status.json?pi=X          → proxies http://<pi X host>/status.json
 *   GET /pis.json                  → list of configured Pi names (for the UI)
 *
 * Build:
 *   cmake -B build && cmake --build build -j$(nproc)
 *
 * Run:
 *   ./build/picam_frontend --config config.ini
 */

#include <algorithm>
#include <arpa/inet.h>
#include <atomic>
#include <cctype>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <netdb.h>
#include <netinet/in.h>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "config.hpp"

static std::atomic<bool> g_stop{false};
static void signal_handler(int) { g_stop = true; }

// ─────────────────────────────────────────────────────────────────────────────
// PiBackend — one configured Pi: a name (for the UI/URLs) and a host:port
// to reach its picam-orchestrator backend.
// ─────────────────────────────────────────────────────────────────────────────
struct PiBackend {
    std::string name;   // short identifier used in ?pi=NAME, e.g. "front", "back"
    std::string label;  // display name for the UI, e.g. "Front Yard"
    std::string host;
    int         port = 80;
};

static std::vector<PiBackend> g_backends;

static const PiBackend* findBackend(const std::string& name) {
    for (auto& b : g_backends) if (b.name == name) return &b;
    return nullptr;
}

// ─────────────────────────────────────────────────────────────────────────────
// Minimal HTTP helpers shared by both the inbound server and the outbound
// proxy client.
// ─────────────────────────────────────────────────────────────────────────────
namespace http {

static std::string urlDecode(const std::string& s) {
    std::string out;
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '%' && i + 2 < s.size() &&
            std::isxdigit(static_cast<unsigned char>(s[i+1])) &&
            std::isxdigit(static_cast<unsigned char>(s[i+2]))) {
            int v = std::stoi(s.substr(i+1, 2), nullptr, 16);
            out += static_cast<char>(v);
            i += 2;
        } else if (s[i] == '+') {
            out += ' ';
        } else {
            out += s[i];
        }
    }
    return out;
}

static std::string queryParam(const std::string& path, const std::string& key) {
    auto q = path.find('?');
    if (q == std::string::npos) return "";
    std::string qs = path.substr(q + 1);
    std::istringstream iss(qs);
    std::string pair;
    while (std::getline(iss, pair, '&')) {
        auto eq = pair.find('=');
        if (eq == std::string::npos) continue;
        if (pair.substr(0, eq) == key)
            return urlDecode(pair.substr(eq + 1));
    }
    return "";
}

// Strips CR/LF from a value before it's concatenated into an outbound
// HTTP request line. queryParam() already runs values through urlDecode,
// which means a value like "main%0d%0aX-Injected:%20evil" would decode
// to contain a literal CRLF — and every proxy route in this file builds
// its outbound GET line by directly concatenating a query param (stream
// name, pi name, the enabled= value) into a string. Without this, a
// crafted request to this frontend could inject arbitrary extra header
// lines into the request this frontend then sends to a backend Pi.
static std::string sanitizeForRequestLine(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (char c : s) if (c != '\r' && c != '\n') out += c;
    return out;
}

// Connect to host:port with a connect-timeout, returns -1 on failure.
// A bounded connect timeout matters here specifically because this is an
// outbound connection to another machine on the network — unlike the
// loopback connections elsewhere in this project, a real network call can
// genuinely hang far longer than is useful if a Pi is unreachable.
static int connectWithTimeout(const std::string& host, int port, int timeoutMs) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    // Resolve host (supports hostnames, not just IPs, since Pis on a LAN
    // are often reached by mDNS name like "picam-front.local").
    addrinfo hints{};
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* res = nullptr;
    if (getaddrinfo(host.c_str(), nullptr, &hints, &res) != 0 || !res) {
        ::close(fd);
        return -1;
    }
    sockaddr_in addr = *reinterpret_cast<sockaddr_in*>(res->ai_addr);
    addr.sin_port = htons(static_cast<uint16_t>(port));
    freeaddrinfo(res);

    // Non-blocking connect with select()-based timeout.
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    int rc = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (rc < 0 && errno != EINPROGRESS) { ::close(fd); return -1; }

    if (rc < 0) {
        fd_set wfds;
        FD_ZERO(&wfds);
        FD_SET(fd, &wfds);
        timeval tv{timeoutMs / 1000, (timeoutMs % 1000) * 1000};
        rc = ::select(fd + 1, nullptr, &wfds, nullptr, &tv);
        if (rc <= 0) { ::close(fd); return -1; }  // timeout or error
        int err = 0; socklen_t len = sizeof(err);
        getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
        if (err != 0) { ::close(fd); return -1; }
    }

    fcntl(fd, F_SETFL, flags);  // restore blocking mode for the actual transfer
    return fd;
}

} // namespace http

// ─────────────────────────────────────────────────────────────────────────────
// Proxy: fetch a complete HTTP response body from a backend (used for
// /status.json, which is small and short-lived).
// ─────────────────────────────────────────────────────────────────────────────
static bool fetchBackendJson(const PiBackend& pi, const std::string& path,
                             std::string& outBody, int timeoutMs = 3000) {
    int fd = http::connectWithTimeout(pi.host, pi.port, timeoutMs);
    if (fd < 0) return false;

    std::ostringstream req;
    req << "GET " << path << " HTTP/1.1\r\n"
        << "Host: " << pi.host << "\r\n"
        << "Connection: close\r\n\r\n";
    std::string r = req.str();
    if (::send(fd, r.data(), r.size(), MSG_NOSIGNAL) < 0) { ::close(fd); return false; }

    struct timeval tv{2, 0};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    std::string resp;
    char buf[4096];
    ssize_t n;
    while ((n = ::recv(fd, buf, sizeof(buf), 0)) > 0) {
        resp.append(buf, static_cast<size_t>(n));
        if (resp.size() > 1'000'000) break;  // sanity cap
    }
    ::close(fd);

    auto hdrEnd = resp.find("\r\n\r\n");
    if (hdrEnd == std::string::npos) return false;
    outBody = resp.substr(hdrEnd + 4);
    return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// Proxy: relay an MJPEG /stream connection from a backend straight through
// to the browser. This thread blocks for the entire lifetime of the
// connection, reading from the backend and writing to the browser — a
// pure byte pump, no decoding/re-encoding. Exits cleanly when either side
// closes or errors.
// ─────────────────────────────────────────────────────────────────────────────
static void proxyStream(const PiBackend& pi, const std::string& stream, int browserFd) {
    int backendFd = http::connectWithTimeout(pi.host, pi.port, 3000);
    if (backendFd < 0) {
        const char* msg =
            "HTTP/1.1 502 Bad Gateway\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n"
            "Could not reach backend\n";
        ::send(browserFd, msg, strlen(msg), MSG_NOSIGNAL);
        ::close(browserFd);
        return;
    }

    std::ostringstream req;
    req << "GET /stream?stream=" << stream << " HTTP/1.1\r\n"
        << "Host: " << pi.host << "\r\n"
        << "Connection: close\r\n\r\n";
    std::string r = req.str();
    if (::send(backendFd, r.data(), r.size(), MSG_NOSIGNAL) < 0) {
        ::close(backendFd); ::close(browserFd); return;
    }

    // Read the backend's response headers, then forward them to the
    // browser verbatim (Content-Type: multipart/x-mixed-replace etc.)
    // before switching to a pure byte-relay loop for the body.
    struct timeval tv{5, 0};
    ::setsockopt(backendFd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    std::string headerBuf;
    char buf[8192];
    size_t hdrEnd = std::string::npos;
    while (hdrEnd == std::string::npos) {
        ssize_t n = ::recv(backendFd, buf, sizeof(buf), 0);
        if (n <= 0) { ::close(backendFd); ::close(browserFd); return; }
        headerBuf.append(buf, static_cast<size_t>(n));
        hdrEnd = headerBuf.find("\r\n\r\n");
        if (headerBuf.size() > 8192) break;  // malformed/oversized headers
    }
    if (hdrEnd == std::string::npos) { ::close(backendFd); ::close(browserFd); return; }

    if (::send(browserFd, headerBuf.data(), headerBuf.size(), MSG_NOSIGNAL) < 0) {
        ::close(backendFd); ::close(browserFd); return;
    }

    // No read timeout from here — this is a long-lived stream and silence
    // between frames is normal, not an error.
    struct timeval notimeout{0, 0};
    ::setsockopt(backendFd, SOL_SOCKET, SO_RCVTIMEO, &notimeout, sizeof(notimeout));

    while (!g_stop) {
        ssize_t n = ::recv(backendFd, buf, sizeof(buf), 0);
        if (n <= 0) break;
        if (::send(browserFd, buf, static_cast<size_t>(n), MSG_NOSIGNAL) <= 0) break;
    }

    ::close(backendFd);
    ::close(browserFd);
}

// ─────────────────────────────────────────────────────────────────────────────
// FrontendHttpServer — accepts browser connections, routes to the proxy
// functions above or serves the static web UI.
// ─────────────────────────────────────────────────────────────────────────────
class FrontendHttpServer {
public:
    FrontendHttpServer(int port, std::string webDir)
        : port_(port), webDir_(std::move(webDir)) {}

    ~FrontendHttpServer() {
        running_ = false;
        // close() alone does not reliably unblock another thread that's
        // sitting inside accept() on this fd (confirmed via gdb: the
        // accept thread stayed parked in accept() even after close()
        // ran, leaving acceptThread_.join() below to hang the process
        // on shutdown). shutdown() first forces accept() to actually
        // return with an error before the fd is closed.
        if (fd_ >= 0) {
            ::shutdown(fd_, SHUT_RDWR);
            ::close(fd_);
        }
        if (acceptThread_.joinable()) acceptThread_.join();
    }

    void start() {
        running_ = true;
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int opt = 1;
        ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port        = htons(static_cast<uint16_t>(port_));
        if (::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            // Without this check, a failed bind() (e.g. EADDRINUSE because
            // something else — including another picam service on the
            // same machine — already has this port) was silently followed
            // by listen() on an UNBOUND socket. On Linux that triggers an
            // implicit bind to a random ephemeral port, so the process
            // kept running and even logged "Listening on http://0.0.0.0:80"
            // — a complete lie. The browser would then hit whatever else
            // was actually on port 80 (in one real case, a colliding
            // picam-orchestrator instance, whose generic 404 the user saw
            // and mistook for a picam-frontend bug). Fail loudly instead.
            std::cerr << "[Frontend] FATAL: bind() failed on port " << port_
                      << ": " << std::strerror(errno) << "\n"
                      << "[Frontend] Is something else already using this port? "
                      << "Check: sudo ss -tlnp | grep ':" << port_ << " '\n";
            ::close(fd_);
            fd_ = -1;
            running_ = false;
            std::exit(1);
        }
        ::listen(fd_, 32);
        acceptThread_ = std::thread(&FrontendHttpServer::acceptLoop, this);
        std::cerr << "[Frontend] Listening on http://0.0.0.0:" << port_ << "\n";
    }

private:
    void acceptLoop() {
        while (running_) {
            int cfd = ::accept(fd_, nullptr, nullptr);
            if (cfd < 0) { if (errno == EINTR) continue; break; }
            std::thread([this, cfd]{ handleClient(cfd); }).detach();
        }
    }

    void handleClient(int cfd) {
        struct timeval tv{5, 0};
        ::setsockopt(cfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        std::string req;
        char buf[4096];
        ssize_t n = ::recv(cfd, buf, sizeof(buf), 0);
        if (n > 0) req.assign(buf, static_cast<size_t>(n));

        std::istringstream iss(req);
        std::string method, fullPath, proto;
        iss >> method >> fullPath >> proto;
        std::string path = fullPath.substr(0, fullPath.find('?'));

        if (path == "/pis.json") {
            std::ostringstream json;
            json << "[";
            for (size_t i = 0; i < g_backends.size(); ++i) {
                if (i) json << ",";
                json << "{\"name\":\"" << g_backends[i].name << "\""
                     << ",\"label\":\"" << g_backends[i].label << "\"}";
            }
            json << "]";
            sendSimple(cfd, 200, "application/json", json.str());
            ::close(cfd);
            return;
        }

        if (path == "/status.json") {
            std::string piName = http::queryParam(fullPath, "pi");
            const PiBackend* pi = piName.empty() ? (g_backends.empty() ? nullptr : &g_backends[0])
                                                  : findBackend(piName);
            if (!pi) {
                sendSimple(cfd, 404, "text/plain", "Unknown pi");
                ::close(cfd);
                return;
            }
            std::string body;
            if (!fetchBackendJson(*pi, "/status.json", body)) {
                sendSimple(cfd, 502, "application/json",
                          "{\"error\":\"backend unreachable\"}");
            } else {
                sendSimple(cfd, 200, "application/json", body);
            }
            ::close(cfd);
            return;
        }

        if (path == "/osd") {
            // Proxies the on-the-fly OSD toggles through to the selected
            // Pi's picam-orchestrator backend. ?pi=NAME picks the backend.
            // camera_id= and time= are independent flags, forwarded
            // verbatim — picam-orchestrator itself validates/defaults
            // each, this is a pure relay. A legacy combined enabled= is
            // still forwarded too, for any caller still using the old
            // single-flag API.
            std::string piName = http::queryParam(fullPath, "pi");
            const PiBackend* pi = piName.empty() ? (g_backends.empty() ? nullptr : &g_backends[0])
                                                  : findBackend(piName);
            if (!pi) {
                sendSimple(cfd, 404, "text/plain", "Unknown pi");
                ::close(cfd);
                return;
            }
            std::string camIdVal = http::sanitizeForRequestLine(
                http::queryParam(fullPath, "camera_id"));
            std::string timeVal = http::sanitizeForRequestLine(
                http::queryParam(fullPath, "time"));
            std::string enabledVal = http::sanitizeForRequestLine(
                http::queryParam(fullPath, "enabled"));

            std::string backendPath = "/osd";
            std::vector<std::string> params;
            if (!camIdVal.empty())  params.push_back("camera_id=" + camIdVal);
            if (!timeVal.empty())   params.push_back("time=" + timeVal);
            if (!enabledVal.empty()) params.push_back("enabled=" + enabledVal);
            for (size_t i = 0; i < params.size(); ++i)
                backendPath += (i == 0 ? "?" : "&") + params[i];

            std::string body;
            if (!fetchBackendJson(*pi, backendPath, body)) {
                sendSimple(cfd, 502, "application/json",
                          "{\"error\":\"backend unreachable\"}");
            } else {
                sendSimple(cfd, 200, "application/json", body);
            }
            ::close(cfd);
            return;
        }

        if (path == "/annotate") {
            // Proxies the per-resolution annotation toggle through to the
            // selected Pi's picam-orchestrator backend. ?pi=NAME picks the
            // backend; ?lores= and/or &main= are forwarded independently —
            // picam-orchestrator validates/defaults each, this is a pure
            // relay, same pattern as /osd above.
            std::string piName = http::queryParam(fullPath, "pi");
            const PiBackend* pi = piName.empty() ? (g_backends.empty() ? nullptr : &g_backends[0])
                                                  : findBackend(piName);
            if (!pi) {
                sendSimple(cfd, 404, "text/plain", "Unknown pi");
                ::close(cfd);
                return;
            }
            std::string loresVal = http::sanitizeForRequestLine(
                http::queryParam(fullPath, "lores"));
            std::string mainVal = http::sanitizeForRequestLine(
                http::queryParam(fullPath, "main"));

            std::string annotatePath = "/annotate";
            std::vector<std::string> annotateParams;
            if (!loresVal.empty()) annotateParams.push_back("lores=" + loresVal);
            if (!mainVal.empty())  annotateParams.push_back("main=" + mainVal);
            for (size_t i = 0; i < annotateParams.size(); ++i)
                annotatePath += (i == 0 ? "?" : "&") + annotateParams[i];

            std::string annotateBody;
            if (!fetchBackendJson(*pi, annotatePath, annotateBody)) {
                sendSimple(cfd, 502, "application/json",
                          "{\"error\":\"backend unreachable\"}");
            } else {
                sendSimple(cfd, 200, "application/json", annotateBody);
            }
            ::close(cfd);
            return;
        }

        if (path == "/camera") {
            // Proxies a camera-switch request through to the selected
            // Pi's picam-orchestrator backend, which itself relays it on to
            // picam-raw's CommandServer. ?pi=NAME picks the Pi, ?id=N
            // selects the camera index — picam-orchestrator validates the id,
            // this is a pure relay.
            std::string piName = http::queryParam(fullPath, "pi");
            const PiBackend* pi = piName.empty() ? (g_backends.empty() ? nullptr : &g_backends[0])
                                                  : findBackend(piName);
            if (!pi) {
                sendSimple(cfd, 404, "text/plain", "Unknown pi");
                ::close(cfd);
                return;
            }
            std::string idVal = http::sanitizeForRequestLine(
                http::queryParam(fullPath, "id"));
            std::string backendPath = "/camera";
            if (!idVal.empty())
                backendPath += "?id=" + idVal;

            std::string body;
            if (!fetchBackendJson(*pi, backendPath, body)) {
                sendSimple(cfd, 502, "application/json",
                          "{\"error\":\"backend unreachable\"}");
            } else {
                sendSimple(cfd, 200, "application/json", body);
            }
            ::close(cfd);
            return;
        }

        if (path == "/stream") {
            std::string piName   = http::queryParam(fullPath, "pi");
            std::string streamParam = http::sanitizeForRequestLine(
                http::queryParam(fullPath, "stream"));
            if (streamParam.empty()) streamParam = "main";

            const PiBackend* pi = piName.empty() ? (g_backends.empty() ? nullptr : &g_backends[0])
                                                  : findBackend(piName);
            if (!pi) {
                sendSimple(cfd, 404, "text/plain", "Unknown pi");
                ::close(cfd);
                return;
            }
            // proxyStream takes ownership of cfd and runs for the entire
            // lifetime of the connection — does not return until it ends.
            proxyStream(*pi, streamParam, cfd);
            return;
        }

        if (path == "/" || path == "/index.html") {
            serveFile(cfd, webDir_ + "/index.html", "text/html");
            ::close(cfd);
            return;
        }

        sendSimple(cfd, 404, "text/plain", "Not found");
        ::close(cfd);
    }

    void serveFile(int cfd, const std::string& path, const std::string& mime) {
        std::ifstream f(path, std::ios::binary);
        if (!f) { sendSimple(cfd, 404, "text/plain", "Not found: " + path); return; }
        std::ostringstream ss; ss << f.rdbuf();
        sendSimple(cfd, 200, mime, ss.str());
    }

    static void sendSimple(int cfd, int code, const std::string& mime,
                           const std::string& body) {
        std::ostringstream resp;
        resp << "HTTP/1.1 " << code << " OK\r\n"
             << "Content-Type: " << mime << "\r\n"
             << "Content-Length: " << body.size() << "\r\n"
             << "Access-Control-Allow-Origin: *\r\n"
             << "Connection: close\r\n\r\n"
             << body;
        std::string s = resp.str();
        ::send(cfd, s.data(), s.size(), MSG_NOSIGNAL);
    }

    int               port_;
    std::string       webDir_;
    int               fd_ = -1;
    std::atomic<bool> running_{false};
    std::thread       acceptThread_;
};

// ─────────────────────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────────────────────
int main(int argc, char** argv) {
    std::signal(SIGINT,  signal_handler);
    std::signal(SIGTERM, signal_handler);
    std::signal(SIGPIPE, SIG_IGN);

    std::string cfg_path = "config.ini";
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if ((a == "--config" || a == "-c") && i + 1 < argc) cfg_path = argv[++i];
    }
    Config cfg(cfg_path);

    const int http_port = cfg.get_int("output.http_port", 80);
    const std::string web_dir = cfg.get_str("output.web_dir",
                                    "/usr/share/picam-frontend/web");

    // Parse the [pis] section: each key is a short name, value is
    // "host:port,Display Label" — e.g.
    //   front = 10.10.0.50:80,Front Yard
    //   back  = 10.10.0.51:80,Back Yard
    auto piEntries = cfg.get_section("pis");
    for (auto& [name, value] : piEntries) {
        PiBackend pi;
        pi.name = name;
        std::string rest = value;
        auto commaPos = rest.find(',');
        std::string hostPort = (commaPos == std::string::npos) ? rest : rest.substr(0, commaPos);
        pi.label = (commaPos == std::string::npos) ? name : rest.substr(commaPos + 1);

        auto colonPos = hostPort.find(':');
        if (colonPos == std::string::npos) {
            pi.host = hostPort;
            // 81 — picam-orchestrator's default port. An unqualified
            // host entry (no :port) in [pis] assumes the backend is
            // running with its own default, not picam-frontend's.
            pi.port = 81;
        } else {
            pi.host = hostPort.substr(0, colonPos);
            pi.port = std::atoi(hostPort.substr(colonPos + 1).c_str());
        }
        g_backends.push_back(pi);
    }

    if (g_backends.empty()) {
        std::cerr << "[Config] WARNING: no [pis] entries configured — "
                     "add at least one to /etc/picam-frontend/config.ini\n";
    }

    std::cerr << "[Config] viewer : http://0.0.0.0:" << http_port << "\n"
              << "[Config] pis    : " << g_backends.size() << " configured\n";
    for (auto& pi : g_backends)
        std::cerr << "  - " << pi.name << " (" << pi.label << ") -> "
                  << pi.host << ":" << pi.port << "\n";

    FrontendHttpServer server(http_port, web_dir);
    server.start();

    std::cerr << "[Main] Ready. Open http://<this-host>"
              << (http_port == 80 ? "" : ":" + std::to_string(http_port))
              << " in a browser.\n";

    while (!g_stop) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::cerr << "\n[Main] Shutting down.\n";
    return 0;
}
