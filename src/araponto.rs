//! Araponto fork extensions to hbbs.
//!
//! Source of this fork: https://github.com/Araponto/rustdesk-server (AGPL-3.0, see FORK.md).
//! Everything here is OFF by default; with every switch off hbbs behaves exactly like upstream.
//!
//!   PUNCH_UDP=Y    answer TestNatRequest over UDP and relay `udp_port`, so clients can UDP hole punch.
//!                  A UDP PunchHoleSent is only accepted when it matches a pending punch, and the
//!                  answer always goes over the controller's TCP connection, never over UDP.
//!   PUNCH_IPV6=Y   relay validated `socket_addr_v6` (global unicast only) for IPv6 punching.
//!   WS_REGISTER=Y  accept RegisterPk over WebSocket and keep the peer reachable on that connection,
//!                  for networks that block UDP but allow 443.
//!   API_BIND=127.0.0.1:21114
//!                  serve the client heartbeat API. A client whose UDP registration never arrives is
//!                  told `allow-websocket=Y`, so it comes back over WebSocket on its next start.

use crate::common::get_arg;
use crate::peer::PeerMap;
use hbb_common::{
    bytes::Bytes,
    log,
    tokio::{self, sync::mpsc},
    try_into_v4, AddrMangle,
};
use once_cell::sync::Lazy;
use std::{
    collections::HashMap,
    hash::Hash,
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Mutex,
    },
    time::{Duration, Instant},
};

pub static PUNCH_UDP: AtomicBool = AtomicBool::new(false);
pub static PUNCH_IPV6: AtomicBool = AtomicBool::new(false);
pub static WS_REGISTER: AtomicBool = AtomicBool::new(false);

#[inline]
pub fn punch_udp() -> bool {
    PUNCH_UDP.load(Ordering::Relaxed)
}

#[inline]
pub fn punch_ipv6() -> bool {
    PUNCH_IPV6.load(Ordering::Relaxed)
}

#[inline]
pub fn ws_register_enabled() -> bool {
    WS_REGISTER.load(Ordering::Relaxed)
}

/// Reads the switches and starts the background tasks. Must run inside the tokio runtime.
pub fn init(pm: PeerMap) {
    Lazy::force(&STARTED);
    let on = |name: &str| get_arg(name).to_uppercase() == "Y";
    PUNCH_UDP.store(on("PUNCH_UDP"), Ordering::SeqCst);
    PUNCH_IPV6.store(on("PUNCH_IPV6"), Ordering::SeqCst);
    WS_REGISTER.store(on("WS_REGISTER"), Ordering::SeqCst);
    let api_bind = get_arg("API_BIND");
    log::info!(
        "araponto: PUNCH_UDP={} PUNCH_IPV6={} WS_REGISTER={} API_BIND={:?} (source: {})",
        punch_udp(),
        punch_ipv6(),
        ws_register_enabled(),
        api_bind,
        SOURCE_URL
    );
    tokio::spawn(stats_logger());
    if !api_bind.is_empty() {
        match api_bind.parse::<SocketAddr>() {
            Ok(addr) => {
                tokio::spawn(api::serve(pm, addr));
            }
            Err(err) => log::error!("araponto: invalid API_BIND {api_bind:?}: {err}"),
        }
    }
}

pub const SOURCE_URL: &str = "https://github.com/Araponto/rustdesk-server";

// ---------------------------------------------------------------------------------------------
// Counters

#[derive(Clone, Copy, Debug)]
pub enum Stat {
    TestNatUdp,
    TestNatUdpLimited,
    PunchFwdUdp,
    PunchFwdV6,
    PunchExtraLimited,
    PendingFull,
    HoleSentUdpAccepted,
    HoleSentUdpNoPending,
    HoleSentUdpMismatch,
    HoleSentUdpDuplicate,
    WsRegister,
    WsRegisterRejected,
    WsOffline,
    WsRouted,
    WsRouteLost,
    ApiHeartbeat,
    ApiFallbackPushed,
}

const STAT_NAMES: [&str; N_STATS] = [
    "test_nat_udp",
    "test_nat_udp_limited",
    "punch_fwd_udp",
    "punch_fwd_v6",
    "punch_extra_limited",
    "pending_full",
    "hole_sent_udp_accepted",
    "hole_sent_udp_no_pending",
    "hole_sent_udp_mismatch",
    "hole_sent_udp_duplicate",
    "ws_register",
    "ws_register_rejected",
    "ws_offline",
    "ws_routed",
    "ws_route_lost",
    "api_heartbeat",
    "api_fallback_pushed",
];

const N_STATS: usize = 17;
#[allow(clippy::declare_interior_mutable_const)]
const ZERO: AtomicU64 = AtomicU64::new(0);
static COUNTERS: [AtomicU64; N_STATS] = [ZERO; N_STATS];

#[inline]
pub fn inc(s: Stat) {
    COUNTERS[s as usize].fetch_add(1, Ordering::Relaxed);
}

fn snapshot() -> [u64; N_STATS] {
    let mut out = [0u64; N_STATS];
    for (i, c) in COUNTERS.iter().enumerate() {
        out[i] = c.load(Ordering::Relaxed);
    }
    out
}

/// Text for the local console command `araponto-stats` (`as`); `as -` resets the counters.
pub fn stats_text(reset: bool) -> String {
    let values = snapshot();
    let mut res = String::new();
    for (name, v) in STAT_NAMES.iter().zip(values.iter()) {
        res.push_str(&format!("{name}: {v}\n"));
    }
    res.push_str(&format!(
        "pending_udp_punch: {}\nws_peers: {}\nheartbeat_ids: {}\n",
        PENDING.lock().map(|m| m.len()).unwrap_or(0),
        WS_PEERS.lock().map(|m| m.len()).unwrap_or(0),
        HB_FIRST_SEEN.lock().map(|g| g.0.len()).unwrap_or(0),
    ));
    if reset {
        for c in COUNTERS.iter() {
            c.store(0, Ordering::Relaxed);
        }
    }
    res
}

/// One info line per minute with the non-zero deltas, so the log proves (or disproves) the gain.
async fn stats_logger() {
    let mut last = snapshot();
    let mut timer = tokio::time::interval(Duration::from_secs(60));
    loop {
        timer.tick().await;
        let now = snapshot();
        let parts: Vec<String> = STAT_NAMES
            .iter()
            .zip(now.iter().zip(last.iter()))
            .filter_map(|(name, (n, l))| (n > l).then(|| format!("{name}=+{}", n - l)))
            .collect();
        if !parts.is_empty() {
            log::info!("araponto stats (60s): {}", parts.join(" "));
        }
        last = now;
    }
}

// ---------------------------------------------------------------------------------------------
// Rate limiting (fixed window, bounded memory)

pub struct RateLimiter<K> {
    map: Mutex<(HashMap<K, (u32, Instant)>, Instant)>,
    max: u32,
    window: Duration,
    cap: usize,
}

impl<K: Eq + Hash + Clone> RateLimiter<K> {
    pub fn new(max: u32, window: Duration, cap: usize) -> Self {
        Self {
            map: Mutex::new((HashMap::new(), Instant::now())),
            max,
            window,
            cap,
        }
    }

    /// True if one more event for `key` fits in the current window.
    /// A full table is swept at most once per window, so a flood of new keys cannot turn every
    /// call into a full scan; while it stays full, new keys are refused (fail closed).
    pub fn allow(&self, key: &K) -> bool {
        let Ok(mut guard) = self.map.lock() else {
            return false;
        };
        let (m, last_sweep) = &mut *guard;
        let now = Instant::now();
        if m.len() >= self.cap && now.duration_since(*last_sweep) >= self.window {
            let window = self.window;
            m.retain(|_, v| now.duration_since(v.1) < window);
            *last_sweep = now;
        }
        match m.get_mut(key) {
            Some(v) if now.duration_since(v.1) < self.window => {
                if v.0 >= self.max {
                    return false;
                }
                v.0 += 1;
                true
            }
            Some(v) => {
                *v = (1, now);
                true
            }
            None => {
                if m.len() >= self.cap {
                    return false;
                }
                m.insert(key.clone(), (1, now));
                true
            }
        }
    }
}

// 1.4.9 retransmits TestNatRequest a few times within ~200 ms; 20/s per IP leaves room for a
// handful of clients behind one NAT. The global cap bounds what a spoofed flood can reflect.
static TEST_NAT_PER_IP: Lazy<RateLimiter<IpAddr>> =
    Lazy::new(|| RateLimiter::new(20, Duration::from_secs(1), 50_000));
static TEST_NAT_GLOBAL: Lazy<RateLimiter<()>> =
    Lazy::new(|| RateLimiter::new(2_000, Duration::from_secs(1), 1));
// Normal use: 2 parallel attempts x up to 3 retries = 6 requests per connection.
static PUNCH_EXTRA: Lazy<RateLimiter<(IpAddr, String)>> =
    Lazy::new(|| RateLimiter::new(8, Duration::from_secs(30), 50_000));

/// Whether a UDP TestNatRequest from `addr` may be answered.
/// The global cap is checked first, so a spoofed flood stops before touching the per-IP table.
pub fn test_nat_allow(addr: SocketAddr) -> bool {
    let ip = try_into_v4(addr).ip();
    if TEST_NAT_GLOBAL.allow(&()) && TEST_NAT_PER_IP.allow(&ip) {
        inc(Stat::TestNatUdp);
        true
    } else {
        inc(Stat::TestNatUdpLimited);
        false
    }
}

// ---------------------------------------------------------------------------------------------
// IPv6 address validation

/// Accepts only an 18-byte AddrMangle v6 address with a port, in 2000::/3 (global unicast), the
/// same filter the client applies. Never invent a v6 address from what the server observed:
/// our listener is dual-stack and sees IPv4 clients as [::ffff:a.b.c.d].
pub fn valid_v6(b: &[u8]) -> Bytes {
    if b.len() != 18 {
        return Bytes::new();
    }
    match AddrMangle::decode(b) {
        SocketAddr::V6(a) if a.port() > 0 && (a.ip().segments()[0] & 0xe000) == 0x2000 => {
            Bytes::copy_from_slice(b)
        }
        _ => Bytes::new(),
    }
}

/// The `udp_port` and `socket_addr_v6` hbbs may forward for this punch request.
/// Relay-only paths (WebSocket on either side, forced relay) never get them, and a controller
/// that asks too often for the same peer degrades to plain TCP punching.
pub fn punch_extras(
    req_udp_port: i32,
    req_v6: &[u8],
    a: SocketAddr,
    id: &str,
    relay_only: bool,
) -> (i32, Bytes) {
    if relay_only {
        return (0, Bytes::new());
    }
    let udp_port = if punch_udp() && (1..=65535).contains(&req_udp_port) {
        req_udp_port
    } else {
        0
    };
    let v6 = if punch_ipv6() {
        valid_v6(req_v6)
    } else {
        Bytes::new()
    };
    if (udp_port > 0 || !v6.is_empty())
        && !PUNCH_EXTRA.allow(&(try_into_v4(a).ip(), id.to_owned()))
    {
        inc(Stat::PunchExtraLimited);
        return (0, Bytes::new());
    }
    (udp_port, v6)
}

// ---------------------------------------------------------------------------------------------
// Pending UDP punches: the only UDP PunchHoleSent hbbs will act on

struct PendingUdpPunch {
    to_id: String,
    b_ip: IpAddr,
    tm: Instant,
    consumed: bool,
}

// The controller waits 3 + 6 + 9 s at most for the answer.
const PENDING_TTL: Duration = Duration::from_secs(20);
const PENDING_CAP: usize = 10_000;

static PENDING: Lazy<Mutex<HashMap<SocketAddr, PendingUdpPunch>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Records that controller `a` (its TCP address) asked for a UDP punch to peer `to_id`, whose
/// registered IP is `b_ip`. False when the table is full: the caller then drops `udp_port`.
pub fn pending_insert(a: SocketAddr, to_id: &str, b_ip: IpAddr) -> bool {
    let Ok(mut m) = PENDING.lock() else {
        return false;
    };
    let now = Instant::now();
    m.retain(|_, v| now.duration_since(v.tm) < PENDING_TTL);
    if m.len() >= PENDING_CAP {
        inc(Stat::PendingFull);
        return false;
    }
    m.insert(
        try_into_v4(a),
        PendingUdpPunch {
            to_id: to_id.to_owned(),
            b_ip: try_into_v4(SocketAddr::new(b_ip, 0)).ip(),
            tm: now,
            consumed: false,
        },
    );
    true
}

/// Consumes the pending punch that a UDP PunchHoleSent from `b_src` answers.
/// A packet that does not match (wrong peer id or source IP) leaves the entry in place, so a
/// forged packet cannot cancel the real punch. B sends three copies; the entry stays as consumed
/// until it expires, so the other two count as duplicates, not as unknown.
pub fn pending_take(a: SocketAddr, id: &str, b_src: SocketAddr) -> Result<(), Stat> {
    let key = try_into_v4(a);
    let Ok(mut m) = PENDING.lock() else {
        return Err(Stat::HoleSentUdpNoPending);
    };
    let Some(p) = m.get_mut(&key) else {
        return Err(Stat::HoleSentUdpNoPending);
    };
    if p.tm.elapsed() >= PENDING_TTL {
        m.remove(&key);
        return Err(Stat::HoleSentUdpNoPending);
    }
    if p.to_id != id || p.b_ip != try_into_v4(b_src).ip() {
        return Err(Stat::HoleSentUdpMismatch);
    }
    if p.consumed {
        return Err(Stat::HoleSentUdpDuplicate);
    }
    p.consumed = true;
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Peers registered over WebSocket, reachable by id on their own connection

struct WsPeer {
    conn_id: u64,
    tx: mpsc::UnboundedSender<Bytes>,
}

static WS_PEERS: Lazy<Mutex<HashMap<String, WsPeer>>> = Lazy::new(|| Mutex::new(HashMap::new()));
static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

/// Registers the connection as the route to `id`, replacing an older one. Returns its conn id.
pub fn ws_peer_add(id: &str, tx: mpsc::UnboundedSender<Bytes>) -> u64 {
    let conn_id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);
    if let Ok(mut m) = WS_PEERS.lock() {
        m.insert(id.to_owned(), WsPeer { conn_id, tx });
    }
    conn_id
}

/// Removes the route if it still belongs to `conn_id`. True when this call removed it.
pub fn ws_peer_remove(id: &str, conn_id: u64) -> bool {
    let Ok(mut m) = WS_PEERS.lock() else {
        return false;
    };
    if m.get(id).map(|p| p.conn_id) == Some(conn_id) {
        m.remove(id);
        return true;
    }
    false
}

pub fn ws_peer_exists(id: &str) -> bool {
    WS_PEERS.lock().map(|m| m.contains_key(id)).unwrap_or(false)
}

/// None: `id` is not a WebSocket peer (use UDP as usual). Some(true): queued on its connection.
/// Some(false): the connection is gone; the caller should answer OFFLINE.
pub fn ws_peer_send(id: &str, bytes: Bytes) -> Option<bool> {
    let Ok(mut m) = WS_PEERS.lock() else {
        return None;
    };
    let sent = m.get(id)?.tx.send(bytes).is_ok();
    if sent {
        inc(Stat::WsRouted);
    } else {
        m.remove(id);
        inc(Stat::WsRouteLost);
    }
    Some(sent)
}

// ---------------------------------------------------------------------------------------------
// Heartbeat API: tells a client with no UDP registration to use WebSocket

// A client with working UDP reaches hbbs within ~15 s of starting (RegisterPeer every ~12 s).
// Decide on the third heartbeat (~33 s after the client starts).
const FALLBACK_AFTER: Duration = Duration::from_secs(30);
// After hbbs starts, give every client a full registration cycle before judging it.
const STARTUP_GRACE: Duration = Duration::from_secs(60);
const UDP_SEEN_TTL: Duration = Duration::from_secs(120);
const UDP_SEEN_CAP: usize = 200_000;

static STARTED: Lazy<Instant> = Lazy::new(Instant::now);

// Last time any UDP registration packet (RegisterPeer / RegisterPk) arrived for an id. A client
// whose UDP is blocked never shows up here, whatever state hbbs was restarted in.
static UDP_SEEN: Lazy<Mutex<(HashMap<String, Instant>, Instant)>> =
    Lazy::new(|| Mutex::new((HashMap::new(), Instant::now())));

/// Called from the UDP loop for every RegisterPeer / RegisterPk with a non-empty id.
pub fn udp_seen(id: &str) {
    if id.is_empty() || id.len() > 64 {
        return;
    }
    let Ok(mut guard) = UDP_SEEN.lock() else {
        return;
    };
    let (m, last_sweep) = &mut *guard;
    let now = Instant::now();
    if let Some(t) = m.get_mut(id) {
        *t = now;
        return;
    }
    if m.len() >= UDP_SEEN_CAP && now.duration_since(*last_sweep) >= UDP_SEEN_TTL {
        m.retain(|_, t| now.duration_since(*t) < UDP_SEEN_TTL);
        *last_sweep = now;
    }
    if m.len() < UDP_SEEN_CAP {
        m.insert(id.to_owned(), now);
    }
}

fn udp_seen_recently(id: &str) -> bool {
    UDP_SEEN
        .lock()
        .ok()
        .and_then(|g| g.0.get(id).map(|t| t.elapsed() < UDP_SEEN_TTL))
        .unwrap_or(false)
}
const HB_TTL: Duration = Duration::from_secs(600);
const HB_CAP: usize = 100_000;

static HB_FIRST_SEEN: Lazy<Mutex<(HashMap<String, Instant>, Instant)>> =
    Lazy::new(|| Mutex::new((HashMap::new(), Instant::now())));

fn hb_forget(id: &str) {
    if let Ok(mut g) = HB_FIRST_SEEN.lock() {
        g.0.remove(id);
    }
}

/// How long `id` has been sending heartbeats without a UDP registration.
fn hb_age(id: &str) -> Duration {
    let Ok(mut guard) = HB_FIRST_SEEN.lock() else {
        return Duration::ZERO;
    };
    let (m, last_sweep) = &mut *guard;
    let now = Instant::now();
    if m.len() >= HB_CAP && !m.contains_key(id) {
        if now.duration_since(*last_sweep) >= Duration::from_secs(60) {
            m.retain(|_, t| now.duration_since(*t) < HB_TTL);
            *last_sweep = now;
        }
        if m.len() >= HB_CAP {
            return Duration::ZERO;
        }
    }
    let first = *m.entry(id.to_owned()).or_insert(now);
    if now.duration_since(first) >= HB_TTL {
        m.insert(id.to_owned(), now);
        return Duration::ZERO;
    }
    now.duration_since(first)
}

mod api {
    use super::*;
    use axum::{
        body::Bytes as Body,
        extract::{ContentLengthLimit, Extension},
        routing::post,
        Json, Router,
    };
    use serde_json::{json, Value};

    const REG_FRESH: Duration = Duration::from_millis(30_000);

    pub(super) async fn serve(pm: PeerMap, addr: SocketAddr) {
        let app = Router::new()
            .route("/api/heartbeat", post(heartbeat))
            .route("/api/sysinfo", post(sysinfo))
            .route("/api/audit/:typ", post(audit))
            .layer(Extension(pm));
        log::info!("araponto: heartbeat API listening on http://{addr}");
        if let Err(err) = axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
        {
            log::error!("araponto: heartbeat API stopped: {err}");
        }
    }

    async fn heartbeat(
        Extension(pm): Extension<PeerMap>,
        ContentLengthLimit(body): ContentLengthLimit<Body, 65_536>,
    ) -> Json<Value> {
        inc(Stat::ApiHeartbeat);
        let v: Value = serde_json::from_slice(&body).unwrap_or_default();
        let id = v["id"].as_str().unwrap_or_default();
        if id.is_empty() || id.len() > 64 {
            return Json(json!({}));
        }
        if should_use_ws(&pm, id).await {
            inc(Stat::ApiFallbackPushed);
            log::info!("araponto: {id} sends heartbeats but never registered over UDP, telling it to use WebSocket");
            return Json(json!({ "strategy": { "config_options": { "allow-websocket": "Y" } } }));
        }
        Json(json!({}))
    }

    async fn should_use_ws(pm: &PeerMap, id: &str) -> bool {
        // Telling a client to use WebSocket when hbbs cannot register it there would take it offline.
        if !ws_register_enabled() || STARTED.elapsed() < STARTUP_GRACE {
            return false;
        }
        if ws_peer_exists(id) || udp_seen_recently(id) {
            hb_forget(id);
            return false;
        }
        let registered = match pm.get_in_memory(id).await {
            Some(p) => p.read().await.last_reg_time.elapsed() < REG_FRESH,
            None => false,
        };
        if registered {
            hb_forget(id);
            return false;
        }
        hb_age(id) >= FALLBACK_AFTER
    }

    // Stops the client from re-uploading its system info every 2 minutes. We store nothing.
    async fn sysinfo(ContentLengthLimit(_body): ContentLengthLimit<Body, 65_536>) -> &'static str {
        "SYSINFO_UPDATED"
    }

    // Connection/file audit posts: accepted and discarded, so they do not show up as errors.
    async fn audit(ContentLengthLimit(_body): ContentLengthLimit<Body, 65_536>) -> &'static str {
        ""
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hbb_common::rendezvous_proto::PunchHoleRequest;
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV6};

    fn v6(ip: &str, port: u16) -> Vec<u8> {
        AddrMangle::encode(SocketAddr::V6(SocketAddrV6::new(
            ip.parse::<Ipv6Addr>().unwrap(),
            port,
            0,
            0,
        )))
    }

    #[test]
    fn valid_v6_accepts_only_global_unicast_with_port() {
        assert!(!valid_v6(&v6("2804:14c::1", 5000)).is_empty());
        assert!(valid_v6(&v6("2804:14c::1", 0)).is_empty());
        assert!(valid_v6(&v6("fe80::1", 5000)).is_empty());
        assert!(valid_v6(&v6("fd00::1", 5000)).is_empty());
        assert!(valid_v6(&v6("::1", 5000)).is_empty());
        // v4-mapped is encoded as IPv4 by AddrMangle and must never pass as v6
        assert!(valid_v6(&v6("::ffff:201.0.179.232", 5000)).is_empty());
        assert!(valid_v6(&[0u8; 17]).is_empty());
        assert!(valid_v6(&[]).is_empty());
    }

    #[test]
    fn rate_limiter_window_and_cap() {
        let rl = RateLimiter::new(2, Duration::from_secs(60), 2);
        assert!(rl.allow(&1));
        assert!(rl.allow(&1));
        assert!(!rl.allow(&1));
        assert!(rl.allow(&2));
        // table full of live keys: new key refused
        assert!(!rl.allow(&3));
    }

    #[test]
    fn pending_matches_id_and_ip_and_is_consumed() {
        let a: SocketAddr = "[::ffff:10.0.0.1]:40000".parse().unwrap();
        let b_ip = IpAddr::V4(Ipv4Addr::new(200, 1, 2, 3));
        assert!(pending_insert(a, "123456789", b_ip));
        let b_src: SocketAddr = "[::ffff:200.1.2.3]:5555".parse().unwrap();
        let other: SocketAddr = "9.9.9.9:5555".parse().unwrap();
        // wrong id / wrong ip do not consume
        assert!(matches!(
            pending_take("10.0.0.1:40000".parse().unwrap(), "999999999", b_src),
            Err(Stat::HoleSentUdpMismatch)
        ));
        assert!(matches!(
            pending_take("10.0.0.1:40000".parse().unwrap(), "123456789", other),
            Err(Stat::HoleSentUdpMismatch)
        ));
        // v4 and v4-mapped forms of A are the same key
        assert!(pending_take("10.0.0.1:40000".parse().unwrap(), "123456789", b_src).is_ok());
        // consumed: the copies B sends afterwards are duplicates
        assert!(matches!(
            pending_take(a, "123456789", b_src),
            Err(Stat::HoleSentUdpDuplicate)
        ));
    }

    #[test]
    fn punch_extras_off_by_default_and_relay_only() {
        let mut ph = PunchHoleRequest::new();
        ph.udp_port = 40000;
        ph.socket_addr_v6 = v6("2804:14c::1", 5000).into();
        let a: SocketAddr = "10.0.0.2:1".parse().unwrap();
        // switches are off in tests unless set
        let (u, s) = punch_extras(ph.udp_port, &ph.socket_addr_v6, a, "111111111", false);
        assert_eq!(u, 0);
        assert!(s.is_empty());
        PUNCH_UDP.store(true, Ordering::SeqCst);
        PUNCH_IPV6.store(true, Ordering::SeqCst);
        let (u, s) = punch_extras(ph.udp_port, &ph.socket_addr_v6, a, "111111111", true);
        assert_eq!(u, 0);
        assert!(s.is_empty());
        let (u, s) = punch_extras(ph.udp_port, &ph.socket_addr_v6, a, "111111111", false);
        assert_eq!(u, 40000);
        assert!(!s.is_empty());
        // over the per-pair limit: degrade to TCP punching
        for _ in 0..10 {
            punch_extras(ph.udp_port, &ph.socket_addr_v6, a, "222222222", false);
        }
        let (u, s) = punch_extras(ph.udp_port, &ph.socket_addr_v6, a, "222222222", false);
        assert_eq!(u, 0);
        assert!(s.is_empty());
        PUNCH_UDP.store(false, Ordering::SeqCst);
        PUNCH_IPV6.store(false, Ordering::SeqCst);
    }
}
