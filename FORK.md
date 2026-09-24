# Araponto fork of rustdesk-server

This is a modified version of [rustdesk/rustdesk-server](https://github.com/rustdesk/rustdesk-server),
licensed under AGPL-3.0 like the original. It runs the ID/rendezvous server (hbbs) and relay (hbbr)
behind `desk.araponto.com.br`. Every binary we deploy is built by `.github/workflows/araponto-build.yml`
from an `ara-*` tag in this repository, and hbbs logs the source URL on start.

Base: upstream `master` (1.1.17, unreleased), branch `araponto/main`.

## What changes

All additions are off by default. With every switch off, hbbs behaves like upstream.

| Switch (env) | Effect |
|---|---|
| `PUNCH_UDP=Y` | Answers `TestNatRequest` over UDP (rate limited per IP and globally, no `ConfigUpdate` attached) and forwards `udp_port`, so clients can UDP hole punch. A UDP `PunchHoleSent` is accepted only when it matches a pending punch (controller TCP address, peer id, peer IP, 20 s), and the answer (`is_udp`) goes over the controller's TCP connection, never over UDP. `LocalAddr` over UDP stays disabled, as upstream. |
| `PUNCH_IPV6=Y` | Forwards the client-declared `socket_addr_v6` when it is a global unicast address (2000::/3) with a port. Never derived from the observed address. |
| `WS_REGISTER=Y` | Accepts `RegisterPk` over WebSocket and keeps the peer reachable on that connection (server heartbeat every 15 s, offline as soon as it closes). For networks that block UDP but allow 443. |
| `API_BIND=127.0.0.1:21114` | Serves `/api/heartbeat`. A client that sends heartbeats but never registers over UDP is told `allow-websocket=Y`. `/api/sysinfo` and `/api/audit/*` are accepted and discarded. |

Always on:
- `X-Real-IP` / `X-Forwarded-For` on the WebSocket ports are trusted only from loopback (the reverse proxy),
  and the proxy-side port is kept, in hbbs and hbbr.
- `PUNCH_REQS` is bounded in size (10,000 entries).
- Local console (`127.0.0.1:21115`): `araponto-stats` (`as`) prints the counters, `as -` resets them.
- Per-controller-and-peer limit on UDP/IPv6 punch requests (8 per 30 s); above it, plain TCP punching.

Code: `src/araponto.rs` plus small hooks marked `araponto:` in `src/rendezvous_server.rs`,
`src/relay_server.rs` and `src/common.rs`.
