/**
 * picam-frontend
 * ==============
 * Single web UI for viewing one or more picam-orchestrator backends (each
 * running on its own Pi). Reads a static list of Pi addresses from
 * config, serves the browser page, and proxies status JSON + WebRTC
 * media from whichever Pi+stream the viewer has selected — the browser
 * only ever talks to this one process, never directly to a Pi.
 *
 * WebRTC media is peer-to-peer by nature, which would otherwise break
 * that "browser only ever talks to picam-frontend" guarantee — so this
 * process is a small SFU-lite relay, not just a byte proxy for the media
 * path: it terminates WebRTC with each browser viewer AND with each
 * picam-orchestrator backend, forwarding raw RTP packets between them
 * (see UpstreamRelay below). One upstream PeerConnection per (pi, stream)
 * pair is shared across every browser watching that combination — the
 * orchestrator only ever sees one subscriber per resolution from this
 * frontend, however many browsers are actually watching.
 *
 * Routes:
 *   GET  /                              → web UI
 *   POST /webrtc/offer?pi=X&stream=Y    → WHEP-style signaling for a browser viewer
 *   GET  /status.json?pi=X              → proxies http://<pi X host>/status.json
 *   GET  /pis.json                      → list of configured Pi names (for the UI)
 *
 * Build:
 *   cmake -B build && cmake --build build -j$(nproc)
 *   (fetches libdatachannel from GitHub at configure time — see
 *   CMakeLists.txt; needs network access during the build)
 *
 * Run:
 *   ./build/picam_frontend --config config.ini
 */

#include <algorithm>
#include <arpa/inet.h>
#include <atomic>
#include <cctype>
#include <chrono>
#include <condition_variable>
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

#include <rtc/rtc.hpp>

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

// ICE port range for every PeerConnection this process creates (both
// upstream-to-orchestrator and downstream-to-browser legs). Set once in
// main() from config before the server starts, read-only after that —
// same pattern as g_backends above.
static uint16_t g_ice_port_min = 50000;
static uint16_t g_ice_port_max = 50200;

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

// Extracts a JSON string value for `key` from a small top-level object
// ({"sdp":"..."} is the only shape ever exchanged here). Handles the
// escape sequences JSON.stringify()/our own jsonEscapeString() produce
// for SDP text, which is itself full of \r\n line endings.
static std::string jsonExtractString(const std::string& json, const std::string& key) {
    std::string needle = "\"" + key + "\"";
    auto pos = json.find(needle);
    if (pos == std::string::npos) return "";
    pos = json.find(':', pos + needle.size());
    if (pos == std::string::npos) return "";
    pos = json.find('"', pos);
    if (pos == std::string::npos) return "";
    ++pos;
    std::string out;
    while (pos < json.size() && json[pos] != '"') {
        if (json[pos] == '\\' && pos + 1 < json.size()) {
            switch (json[pos + 1]) {
                case 'n':  out += '\n'; break;
                case 'r':  out += '\r'; break;
                case 't':  out += '\t'; break;
                case '"':  out += '"';  break;
                case '\\': out += '\\'; break;
                case '/':  out += '/';  break;
                default:   out += json[pos + 1]; break;
            }
            pos += 2;
        } else {
            out += json[pos];
            ++pos;
        }
    }
    return out;
}

static std::string jsonEscapeString(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n";  break;
            case '\r': out += "\\r";  break;
            case '\t': out += "\\t";  break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char esc[8];
                    snprintf(esc, sizeof(esc), "\\u%04x", c);
                    out += esc;
                } else {
                    out += c;
                }
        }
    }
    return out;
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
// Proxy: POST a JSON body to a backend and return its JSON response body.
// Same shape as fetchBackendJson but for /webrtc/offer's SDP-offer POST.
// ─────────────────────────────────────────────────────────────────────────────
static bool postBackendJson(const PiBackend& pi, const std::string& path,
                            const std::string& body, std::string& outBody,
                            int timeoutMs = 5000) {
    int fd = http::connectWithTimeout(pi.host, pi.port, timeoutMs);
    if (fd < 0) return false;

    std::ostringstream req;
    req << "POST " << path << " HTTP/1.1\r\n"
        << "Host: " << pi.host << "\r\n"
        << "Content-Type: application/json\r\n"
        << "Content-Length: " << body.size() << "\r\n"
        << "Connection: close\r\n\r\n"
        << body;
    std::string r = req.str();
    if (::send(fd, r.data(), r.size(), MSG_NOSIGNAL) < 0) { ::close(fd); return false; }

    struct timeval tv{static_cast<time_t>(timeoutMs / 1000), 0};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    std::string resp;
    char buf[8192];
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
// UpstreamRelay — one WebRTC PeerConnection from this frontend to a single
// picam-orchestrator (pi, stream) pair, shared by every browser currently
// watching that combination. Established lazily on the first browser
// subscriber, torn down when the last one leaves — mirrors
// picam-orchestrator's own "only encode if someone's watching" behavior,
// one level removed (the orchestrator only ever sees ONE subscriber from
// this frontend for a given resolution, however many browsers are
// actually watching it here).
//
// Media is relayed as raw RTP: the upstream track's onMessage() hands us
// already-packetized RTP packets straight from picam-orchestrator
// (recvonly, no MediaHandler attached), which we fan out verbatim via
// send() to every downstream browser track — no decode, no re-encode.
// This is the exact pattern demonstrated in libdatachannel's own
// examples/media-sfu, generalized from a 1-to-1 forward to 1-to-N.
// ─────────────────────────────────────────────────────────────────────────────
// A downstream viewer's PeerConnection must be kept alive here alongside
// its track — a bare Track shared_ptr does NOT keep its parent
// PeerConnection alive. Without this, the PeerConnection created in the
// /webrtc/offer handler would be destroyed the moment that request
// handler returns (its only other reference was a local variable),
// tearing down ICE/DTLS mid-handshake before the browser ever finished
// connecting. Confirmed the hard way: signaling succeeded (valid SDP
// answer generated) but the WebRTC connection itself never reached
// Connected — exactly the symptom of the PeerConnection dying underneath
// the still-in-progress handshake.
struct DownstreamViewer {
    std::shared_ptr<rtc::PeerConnection> pc;
    std::shared_ptr<rtc::Track>          track;
};

struct UpstreamRelay {
    std::shared_ptr<rtc::PeerConnection> pc;
    std::shared_ptr<rtc::Track>          track;  // recvonly, from picam-orchestrator
    std::mutex                           subscribersMu;
    std::vector<DownstreamViewer>        downstreamViewers;  // one per browser viewer
};

using RelayKey = std::pair<std::string, std::string>;  // (pi name, stream)
static std::mutex g_relaysMu;
static std::map<RelayKey, std::shared_ptr<UpstreamRelay>> g_relays;

// Establishes a new upstream PeerConnection to `pi`'s /webrtc/offer for
// `stream`, or returns nullptr on failure. Blocking — does the full
// WHEP-client offer/answer round trip (network I/O to the Pi).
static std::shared_ptr<UpstreamRelay> establishUpstream(const PiBackend& pi,
                                                        const std::string& stream) {
    auto relay = std::make_shared<UpstreamRelay>();

    rtc::Configuration config;  // LAN-only — no STUN/TURN needed for this leg
    config.portRangeBegin = g_ice_port_min;
    config.portRangeEnd   = g_ice_port_max;
    relay->pc = std::make_shared<rtc::PeerConnection>(config);

    std::mutex              gatherMu;
    std::condition_variable gatherCv;
    bool                    gatheringComplete = false;
    relay->pc->onGatheringStateChange([&](rtc::PeerConnection::GatheringState state) {
        if (state == rtc::PeerConnection::GatheringState::Complete) {
            std::lock_guard<std::mutex> lk(gatherMu);
            gatheringComplete = true;
            gatherCv.notify_one();
        }
    });

    rtc::Description::Video media("video", rtc::Description::Direction::RecvOnly);
    media.addVP8Codec(96);
    relay->track = relay->pc->addTrack(media);

    // Raw RTP fan-out to every currently-subscribed downstream browser
    // track — the core of the relay. weak_ptr avoids keeping the relay
    // (and thus this PeerConnection) alive purely because this callback
    // captured a shared_ptr to it.
    std::weak_ptr<UpstreamRelay> weakRelay = relay;
    relay->track->onMessage([weakRelay](rtc::message_variant msg) {
        if (!std::holds_alternative<rtc::binary>(msg)) return;
        auto self = weakRelay.lock();
        if (!self) return;
        const auto& rtp = std::get<rtc::binary>(msg);
        std::lock_guard<std::mutex> lk(self->subscribersMu);
        for (auto& dv : self->downstreamViewers) {
            if (dv.track->isOpen()) dv.track->send(rtp);
        }
    });

    relay->pc->setLocalDescription();  // we're the offerer (recvonly)
    {
        std::unique_lock<std::mutex> lk(gatherMu);
        gatherCv.wait_for(lk, std::chrono::seconds(5), [&] { return gatheringComplete; });
    }

    auto offer = relay->pc->localDescription();
    if (!offer) {
        std::cerr << "\n[Relay] ICE gathering timed out generating offer to " << pi.name << "\n";
        relay->pc->close();
        return nullptr;
    }

    std::string offerJson = "{\"sdp\":\"" + http::jsonEscapeString(std::string(*offer)) + "\"}";
    std::string respJson;
    std::string path = "/webrtc/offer?stream=" + http::sanitizeForRequestLine(stream);
    if (!postBackendJson(pi, path, offerJson, respJson)) {
        std::cerr << "\n[Relay] Could not reach " << pi.name << " for /webrtc/offer\n";
        relay->pc->close();
        return nullptr;
    }

    std::string answerSdp = http::jsonExtractString(respJson, "sdp");
    if (answerSdp.empty()) {
        std::cerr << "\n[Relay] " << pi.name << " returned no sdp in /webrtc/offer response: "
                  << respJson.substr(0, 200) << "\n";
        relay->pc->close();
        return nullptr;
    }

    // No manual setLocalDescription() call needed or wanted here — we're
    // the offerer, so this setRemoteDescription() just processes the
    // answer and settles the signaling state; auto-negotiation only
    // triggers a new local description on receiving an OFFER, not an
    // answer. See picam-orchestrator's /webrtc/offer handler for the
    // mirror-image bug this pattern avoids on the answerer side (a
    // redundant manual setLocalDescription() there silently replaced a
    // correct answer with a bogus second offer).
    relay->pc->setRemoteDescription(rtc::Description(answerSdp, rtc::Description::Type::Answer));

    return relay;
}

// Returns the existing upstream relay for (pi.name, stream), or
// establishes a new one. Holds g_relaysMu for the whole operation,
// including the network round trip on first establishment — simple and
// correct at this project's scale (up to 50 viewers, a handful of Pis),
// at the cost of briefly blocking unrelated (pi, stream) pairs' own
// first-subscriber setup if they happen to race. Not worth a per-key
// lock for this traffic level.
static std::shared_ptr<UpstreamRelay> getOrCreateUpstream(const PiBackend& pi,
                                                          const std::string& stream) {
    std::lock_guard<std::mutex> lk(g_relaysMu);
    RelayKey key{pi.name, stream};
    auto it = g_relays.find(key);
    if (it != g_relays.end()) return it->second;

    auto relay = establishUpstream(pi, stream);
    if (relay) g_relays[key] = relay;
    return relay;
}

// Called when a browser viewer disconnects. Removes its track from the
// relay's subscriber list; if that was the last subscriber, tears down
// the upstream PeerConnection entirely (no point keeping
// picam-orchestrator encoding for nobody).
static void unsubscribeFromRelay(const PiBackend& pi, const std::string& stream,
                                 const std::shared_ptr<rtc::Track>& downstreamTrack) {
    std::lock_guard<std::mutex> lk(g_relaysMu);
    RelayKey key{pi.name, stream};
    auto it = g_relays.find(key);
    if (it == g_relays.end()) return;
    auto relay = it->second;

    bool empty;
    {
        std::lock_guard<std::mutex> slk(relay->subscribersMu);
        auto& viewers = relay->downstreamViewers;
        // Detach onStateChange BEFORE erasing. Found the hard way (a
        // real, reproducible hang): erasing drops the last shared_ptr
        // to this viewer's PeerConnection, running ~PeerConnection(),
        // which synchronously calls closeTransports() ->
        // changeState(Closed) -> re-invokes onStateChange with Closed
        // as part of its OWN teardown — reentering this exact function,
        // on the same thread, while still inside std::remove_if's
        // element-shifting loop over this same vector. That reentrant
        // call then deadlocks trying to re-lock g_relaysMu above (a
        // plain std::mutex, not recursive) — confirmed via a `sample`
        // stack trace of the hung process while debugging. Clearing the
        // callback first makes that inevitable teardown callback a
        // no-op instead of a self-deadlock.
        for (auto& dv : viewers)
            if (dv.track == downstreamTrack) dv.pc->onStateChange(nullptr);
        viewers.erase(std::remove_if(viewers.begin(), viewers.end(),
                                     [&](const DownstreamViewer& dv) { return dv.track == downstreamTrack; }),
                      viewers.end());
        empty = viewers.empty();
    }
    if (empty) {
        std::cerr << "\n[Relay] Last viewer left " << pi.name << "/" << stream
                  << ", tearing down upstream\n";
        relay->pc->close();
        g_relays.erase(it);
    }
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

        // POST body support (only /webrtc/offer needs one — every other
        // endpoint here is GET-with-query-params). Content-Length-based:
        // the connection stays open for the response, so we can't rely
        // on read-until-EOF the way a one-shot protocol could.
        std::string body;
        if (method == "POST") {
            auto headerEnd = req.find("\r\n\r\n");
            size_t contentLength = 0;
            if (headerEnd != std::string::npos) {
                std::string headerBlock = req.substr(0, headerEnd);
                std::string lower = headerBlock;
                std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
                auto clPos = lower.find("content-length:");
                if (clPos != std::string::npos) {
                    try {
                        contentLength = static_cast<size_t>(
                            std::stoul(headerBlock.substr(clPos + 15)));
                    } catch (...) {}
                }
                body = req.substr(headerEnd + 4);
            }
            // Cap matches the SDP offer sizes this ever needs to carry —
            // a few KB even with a generous number of ICE candidates.
            contentLength = std::min(contentLength, static_cast<size_t>(65536));
            while (body.size() < contentLength) {
                ssize_t got = ::recv(cfd, buf, sizeof(buf), 0);
                if (got <= 0) break;
                body.append(buf, static_cast<size_t>(got));
            }
        }

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

        if (method == "POST" && path == "/webrtc/offer") {
            // WHEP-style signaling for a browser viewer: SDP offer in
            // (JSON body), SDP answer out, once — see UpstreamRelay above
            // for how the actual media then flows (relayed, not proxied).
            std::string piName   = http::queryParam(fullPath, "pi");
            std::string streamParam = http::sanitizeForRequestLine(
                http::queryParam(fullPath, "stream"));
            if (streamParam.empty()) streamParam = "main";

            const PiBackend* pi = piName.empty() ? (g_backends.empty() ? nullptr : &g_backends[0])
                                                  : findBackend(piName);
            if (!pi) {
                sendSimple(cfd, 404, "application/json", "{\"error\":\"unknown pi\"}");
                ::close(cfd);
                return;
            }

            std::string sdpOffer = http::jsonExtractString(body, "sdp");
            if (sdpOffer.empty()) {
                sendSimple(cfd, 400, "application/json", "{\"error\":\"missing sdp\"}");
                ::close(cfd);
                return;
            }

            auto relay = getOrCreateUpstream(*pi, streamParam);
            if (!relay) {
                sendSimple(cfd, 502, "application/json", "{\"error\":\"could not reach backend\"}");
                ::close(cfd);
                return;
            }

            // Everything below can throw on malformed input — most
            // notably rtc::Description's SDP parser and
            // setRemoteDescription()'s role validation (both confirmed
            // the hard way: a garbage "sdp" value crashed the whole
            // process via an uncaught std::invalid_argument, taking
            // down every other viewer's connection too, not just this
            // request). A malformed request from one client must not be
            // able to kill the service for everyone else.
            try {
                // disableAutoNegotiation matters here: with it left on
                // (the default), setRemoteDescription() below auto-generates
                // the answer *immediately*, before addTrack() has a chance
                // to register our track — found the hard way, via a raw SDP
                // dump, that the resulting answer ended up with the video
                // m-line TWICE (once correctly reciprocated with the full
                // codec list, once again — duplicated — for our own track
                // added moments later), which every browser correctly
                // rejects ("order of m-lines in answer doesn't match order
                // in offer"). Disabling auto-negotiation lets us control the
                // exact order: set remote description (no answer yet) ->
                // addTrack() -> manually trigger exactly one answer.
                rtc::Configuration config;
                config.portRangeBegin        = g_ice_port_min;
                config.portRangeEnd          = g_ice_port_max;
                config.disableAutoNegotiation = true;
                auto downstreamPc = std::make_shared<rtc::PeerConnection>(config);

                std::mutex              gatherMu;
                std::condition_variable gatherCv;
                bool                    gatheringComplete = false;
                downstreamPc->onGatheringStateChange([&](rtc::PeerConnection::GatheringState state) {
                    if (state == rtc::PeerConnection::GatheringState::Complete) {
                        std::lock_guard<std::mutex> lk(gatherMu);
                        gatheringComplete = true;
                        gatherCv.notify_one();
                    }
                });

                downstreamPc->setRemoteDescription(rtc::Description(sdpOffer, rtc::Description::Type::Offer));

                // The mid of our local video track MUST match the mid the
                // browser's offer video m-line uses, so libdatachannel's
                // answer-reciprocation matches it to the right m-line
                // (rather than appending it as an unrelated extra one) —
                // parse the now-set remote description and reuse whatever
                // mid it actually used, rather than assuming one.
                std::string videoMid = "video";
                if (auto remote = downstreamPc->remoteDescription()) {
                    for (int i = 0; i < remote->mediaCount(); ++i) {
                        auto entry = remote->media(i);
                        if (auto m = std::get_if<rtc::Description::Media*>(&entry)) {
                            videoMid = (*m)->mid();
                            break;
                        }
                    }
                }

                rtc::Description::Video media(videoMid, rtc::Description::Direction::SendOnly);
                media.addVP8Codec(96);
                auto downstreamTrack = downstreamPc->addTrack(media);

                // Forwards this browser's PLI (e.g. on reconnect / packet
                // loss) up to the shared upstream connection. No packetizer
                // attached — PLI handling coexists fine with raw send() (see
                // UpstreamRelay's class comment: an attached MediaHandler
                // with no outgoing() override is an inherited no-op
                // passthrough, confirmed against the vendored source).
                std::weak_ptr<UpstreamRelay> weakRelay = relay;
                auto pli = std::make_shared<rtc::PliHandler>([weakRelay]() {
                    if (auto self = weakRelay.lock()) self->track->requestKeyframe();
                });
                downstreamTrack->setMediaHandler(pli);

                PiBackend piCopy = *pi;  // captured by value below
                downstreamPc->onStateChange(
                    [piCopy, streamParam, downstreamTrack](rtc::PeerConnection::State state) {
                        if (state == rtc::PeerConnection::State::Disconnected ||
                            state == rtc::PeerConnection::State::Failed ||
                            state == rtc::PeerConnection::State::Closed) {
                            unsubscribeFromRelay(piCopy, streamParam, downstreamTrack);
                        }
                    });

                // Auto-negotiation is off, so this is the ONE and ONLY
                // place the answer gets generated — no ambiguity about
                // ordering relative to addTrack() above.
                downstreamPc->setLocalDescription(rtc::Description::Type::Answer);

                {
                    std::unique_lock<std::mutex> lk(gatherMu);
                    gatherCv.wait_for(lk, std::chrono::seconds(5), [&] { return gatheringComplete; });
                }

                auto answer = downstreamPc->localDescription();
                if (!answer) {
                    sendSimple(cfd, 500, "application/json", "{\"error\":\"failed to generate answer\"}");
                    downstreamPc->close();
                    ::close(cfd);
                    return;
                }

                {
                    std::lock_guard<std::mutex> lk(relay->subscribersMu);
                    relay->downstreamViewers.push_back({downstreamPc, downstreamTrack});
                }
                // A fresh viewer needs a keyframe promptly rather than
                // waiting for whatever the upstream encoder's next
                // spontaneous one is — there isn't one on a fixed schedule
                // (see picam-orchestrator's Vp8Encoder), so without this a
                // new viewer joining an already-established relay could
                // otherwise wait indefinitely for a decodable frame.
                relay->track->requestKeyframe();

                std::cerr << "\n[Relay] Browser viewer connected " << piCopy.name
                          << "/" << streamParam << "\n";

                std::string answerJson = "{\"sdp\":\"" + http::jsonEscapeString(std::string(*answer)) + "\"}";
                sendSimple(cfd, 200, "application/json", answerJson);
            } catch (const std::exception& e) {
                std::cerr << "\n[Relay] /webrtc/offer failed for " << pi->name << "/" << streamParam
                          << ": " << e.what() << "\n";
                sendSimple(cfd, 400, "application/json",
                          "{\"error\":\"" + http::jsonEscapeString(e.what()) + "\"}");
            }
            ::close(cfd);
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

    g_ice_port_min = static_cast<uint16_t>(cfg.get_int("webrtc.ice_port_min", 50000));
    g_ice_port_max = static_cast<uint16_t>(cfg.get_int("webrtc.ice_port_max", 50200));

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
              << "[Config] webrtc : ice ports " << g_ice_port_min << "-" << g_ice_port_max << "\n"
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

    // Close every relay's upstream PeerConnection explicitly rather than
    // relying on process exit to tear them down — closes cleanly notify
    // picam-orchestrator instead of just vanishing from its perspective.
    {
        std::lock_guard<std::mutex> lk(g_relaysMu);
        for (auto& [key, relay] : g_relays) relay->pc->close();
        g_relays.clear();
    }

    std::cerr << "\n[Main] Shutting down.\n";
    return 0;
}
