# picam-frontend

A web UI for viewing and controlling multiple Raspberry Pi cameras. The browser talks exclusively to picam-frontend, which proxies all traffic (MJPEG streams, status JSON, control commands) to the backend Pis running [picam-orchestrator](https://github.com).

## Features

- Live MJPEG stream grid — see all cameras at a glance
- Drill-down view with full-resolution stream and telemetry
- Switch between main/lores streams and dual-lens cameras (Camera 0/1)
- Toggle camera ID and timestamp overlays (OSD)
- Toggle per-resolution annotation
- Real-time telemetry (lux level, active camera index, frame timestamp)
- Graceful handling of unreachable Pis — no page crash, just an offline indicator

## Architecture

```
Browser ──── HTTP ────► picam-frontend (this) ──── HTTP ────► picam-orchestrator (each Pi)
```

picam-frontend is a hand-rolled C++17 HTTP server. It serves a single-page HTML+JS UI and proxies browser requests to the configured Pi backends. There are no external C++ dependencies beyond the standard library and pthreads.

## Build

**Requirements:** CMake 3.16+, a C++17 compiler, pthreads.

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

The binary is written to `build/picam_frontend`.

## Configuration

Edit `config.ini` before running:

```ini
[pis]
# Format: name = host[:port][, Display Label]
# Default port: 81 (picam-orchestrator default)
front = 10.10.0.50,Front Yard
back  = 10.10.0.51,Back Yard
side  = 10.10.0.52:8080,Side Gate

[output]
http_port = 80
web_dir   = ./web
```

- `name` — short identifier used in URLs and internally
- `host` — IP, hostname, or mDNS name (e.g. `picam-front.local`)
- `port` — optional; defaults to 81
- `Display Label` — optional; defaults to name

## Run

```bash
./build/picam_frontend --config config.ini
```

Then open `http://localhost` (or whatever `http_port` is set to) in a browser.

## API

The frontend exposes these endpoints, which the single-page app calls internally:

| Endpoint | Description |
|---|---|
| `GET /` | Serves `index.html` |
| `GET /pis.json` | JSON array of configured Pi objects |
| `GET /stream?pi=X&stream=Y` | Proxied MJPEG stream from Pi X |
| `GET /status.json?pi=X` | Proxied telemetry JSON from Pi X |
| `GET /camera?pi=X&id=N` | Switch camera lens on Pi X |
| `GET /osd?pi=X&camera_id=true/false&time=true/false` | Toggle OSD overlays |
| `GET /annotate?pi=X&main=true/false&lores=true/false` | Toggle annotation |

## Deployment (Debian package)

Build and install as a Debian package:

```bash
dpkg-buildpackage -us -uc -b
sudo dpkg -i ../picam-frontend_*.deb
```

Installed paths:

| Path | Contents |
|---|---|
| `/usr/bin/picam_frontend` | Binary |
| `/etc/picam-frontend/config.ini` | Configuration |
| `/usr/share/picam-frontend/web/` | Web assets |
| `/lib/systemd/system/picam-frontend.service` | systemd unit |

Manage with systemd:

```bash
sudo systemctl enable --now picam-frontend
sudo systemctl status picam-frontend
```

Edit `/etc/picam-frontend/config.ini` and restart the service to pick up changes.
