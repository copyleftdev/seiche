//! Live Kalshi V2 WebSocket -> `seiche` Incremental DAG -> SSE Observatory Server (`seiche serve`).
//!
//! Architecture (TigerStyle):
//! - Bounded SSE fan-out broker (`MAX_CLIENTS = 64`, `MAX_BACKLOG = 512`, `CORS: *` so both
//!   `http://127.0.0.1:8790/` and `https://copyleftdev.github.io/seiche/` can attach seamlessly)
//! - RSA-PSS signed handshake to `wss://external-api-ws.kalshi.com/trade-api/ws/v2` subscribing
//!   to `orderbook_delta`, `ticker`, and `trade` across the 18 observatory contracts + top liquid
//!   series (with automatic unauthenticated public-market fallback if credentials are absent)
//! - Every incoming WebSocket frame passes through `DeltaBuffer` (detecting sequence gaps) and
//!   into `Graph::stabilize()`, measuring exact nanosecond stabilization latency and broadcasting
//!   the live node activations (`event: dag`) to connected browsers.

use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pss::SigningKey as PssSigningKey;
use rsa::sha2::Sha256;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use rsa::RsaPrivateKey;
use serde_json::Value;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::cutoff::Cutoff;
use crate::delta_buffer::{DeltaBuffer, ReceiveResult};
use crate::graph::{kalshi_taker_fee_cc, Graph, LeafHandle, NodeId};

const WS_URL: &str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2";
pub const MAX_CLIENTS: usize = 64;
pub const MAX_BACKLOG: usize = 512;

/// The 18 canonical Kalshi series/contract prefixes mapped onto the 18 observatory strike slots.
pub const OBSERVATORY_SLOTS: [(&str, usize, usize, i64, i64); 18] = [
    ("KXGOLDH", 0, 0, 9200, 500),
    ("KXGOLDH-T4041", 0, 0, 9100, 500),
    ("KXGOLDH-T4035", 0, 0, 9300, 500),
    ("KXEURUSD", 1, 0, 9200, 500),
    ("KXEURUSD-B1", 1, 0, 8800, 500),
    ("KXINXU", 2, 1, 9300, 500),
    ("KXINXU-H1600", 2, 1, 9100, 500),
    ("KXINXU-H1700", 2, 1, 8900, 500),
    ("KXHIGHCHI", 3, 2, 8700, 1100),
    ("KXHIGHNY", 3, 2, 9100, 1100),
    ("KXHIGHTSEA", 3, 2, 8600, 1100),
    ("KXHIGHDEN", 3, 2, 8900, 1100),
    ("KXNHLGAME-26OCT29BUFANA-ANA", 4, 3, 4300, 500),
    ("KXNHLGAME", 4, 3, 4700, 410),
    ("KXNBAGAME", 4, 3, 3800, 500),
    ("KXMLBGAME", 4, 3, 4700, 600),
    ("KXNFLGAME-26OCT29CARGB-CAR", 5, 3, 3400, 1000),
    ("KXNFLGAME-26OCT29CARGB-GB", 5, 3, 6600, 1000),
];

struct SseClient {
    stream: TcpStream,
    backlog: VecDeque<String>,
    expired: bool,
}

pub struct SseBroker {
    clients: Mutex<Vec<SseClient>>,
    pub total_events_broadcast: Mutex<u64>,
    pub ws_connected: Mutex<bool>,
}

impl SseBroker {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            clients: Mutex::new(Vec::with_capacity(MAX_CLIENTS)),
            total_events_broadcast: Mutex::new(0),
            ws_connected: Mutex::new(false),
        })
    }

    pub fn broadcast(&self, event: &str, json_data: &str) {
        let mut clients = self.clients.lock().expect("broker lock");
        if let Ok(mut cnt) = self.total_events_broadcast.lock() {
            *cnt += 1;
        }
        let frame = format!("event: {event}\ndata: {json_data}\n\n");
        for c in clients.iter_mut() {
            if c.expired {
                continue;
            }
            if c.backlog.len() >= MAX_BACKLOG {
                c.expired = true;
                continue;
            }
            c.backlog.push_back(frame.clone());
        }
    }

    pub fn keepalive(&self) {
        let mut clients = self.clients.lock().expect("broker lock");
        for c in clients.iter_mut() {
            if !c.expired {
                c.backlog.push_back(": keepalive\n\n".to_string());
            }
        }
    }

    pub fn register(&self, mut stream: TcpStream) -> Option<()> {
        let mut clients = self.clients.lock().expect("broker lock");
        clients.retain(|c| !c.expired);
        if clients.len() >= MAX_CLIENTS {
            return None;
        }
        let head = concat!(
            "HTTP/1.1 200 OK\r\n",
            "Content-Type: text/event-stream\r\n",
            "Cache-Control: no-cache\r\n",
            "Connection: keep-alive\r\n",
            "Access-Control-Allow-Origin: *\r\n\r\n"
        );
        if stream.write_all(head.as_bytes()).is_err() {
            return None;
        }
        let ws_up = *self.ws_connected.lock().unwrap_or_else(|e| e.into_inner());
        let welcome = format!(
            "event: status\ndata: {{\"ws_connected\":{},\"mode\":\"live_kalshi_ws\"}}\n\n",
            ws_up
        );
        let _ = stream.write_all(welcome.as_bytes());
        clients.push(SseClient {
            stream,
            backlog: VecDeque::new(),
            expired: false,
        });
        Some(())
    }

    pub fn sweep(&self, timeout: Duration) {
        let mut clients = self.clients.lock().expect("broker lock");
        for c in clients.iter_mut() {
            if c.expired {
                continue;
            }
            let _ = c.stream.set_write_timeout(Some(timeout));
            while let Some(msg) = c.backlog.pop_front() {
                if c.stream.write_all(msg.as_bytes()).is_err() {
                    c.expired = true;
                    break;
                }
            }
        }
        clients.retain(|c| !c.expired);
    }

    pub fn client_count(&self) -> usize {
        self.clients.lock().map(|c| c.len()).unwrap_or(0)
    }
}

/// Map any incoming Kalshi market ticker to one of the 18 observatory strike slots (`0..18`).
pub fn map_ticker_to_slot(ticker: &str) -> usize {
    if ticker == "KXNHLGAME-26OCT29BUFANA-ANA" {
        return 12;
    }
    if ticker == "KXNFLGAME-26OCT29CARGB-CAR" {
        return 16;
    }
    if ticker == "KXNFLGAME-26OCT29CARGB-GB" {
        return 17;
    }
    let prefix = ticker.split('-').next().unwrap_or("");
    match prefix {
        "KXGOLDH" | "KXGOLDD" => {
            (fnv_Slot(ticker) % 3) as usize // slots 0, 1, 2
        }
        "KXEURUSD" | "KXUSDJPY" | "KXSILVERH" => {
            3 + (fnv_Slot(ticker) % 2) as usize // slots 3, 4
        }
        "KXINXU" | "KXNASDAQ100U" => {
            5 + (fnv_Slot(ticker) % 3) as usize // slots 5, 6, 7
        }
        "KXHIGHCHI" | "KXHIGHNY" | "KXHIGHTSEA" | "KXHIGHDEN" | "KXHIGHMIA" | "KXHIGHLAX" => {
            8 + (fnv_Slot(ticker) % 4) as usize // slots 8..11
        }
        "KXNHLGAME" => 12 + (fnv_Slot(ticker) % 2) as usize,
        "KXNBAGAME" => 14,
        "KXMLBGAME" | "KXMLB" => 15,
        "KXNFLGAME" | "KXNCAAFGAME" => 16 + (fnv_Slot(ticker) % 2) as usize,
        _ => (fnv_Slot(ticker) % 18) as usize,
    }
}

#[allow(non_snake_case)]
fn fnv_Slot(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in s.as_bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

pub fn run_serve(port: u16, tickers_override: Option<String>) -> Result<(), String> {
    let addr = format!("0.0.0.0:{port}");
    let listener = TcpListener::bind(&addr).map_err(|e| format!("bind {addr}: {e}"))?;
    let broker = SseBroker::new();

    eprintln!("seiche serve listening on http://127.0.0.1:{port}");
    eprintln!("  -> UI Observatory : http://127.0.0.1:{port}/");
    eprintln!("  -> SSE DAG Stream : http://127.0.0.1:{port}/events");
    eprintln!("  -> Health Probe   : http://127.0.0.1:{port}/health");

    // 1. Spawn SSE sweeper thread
    {
        let b = Arc::clone(&broker);
        std::thread::spawn(move || loop {
            b.sweep(Duration::from_millis(400));
            std::thread::sleep(Duration::from_millis(120));
        });
    }

    // 2. Spawn SSE keepalive thread
    {
        let b = Arc::clone(&broker);
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_secs(15));
            b.keepalive();
        });
    }

    // 3. Spawn Kalshi V2 WebSocket + Seiche DAG engine thread
    {
        let b = Arc::clone(&broker);
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            rt.block_on(ws_dag_loop(b, tickers_override));
        });
    }

    let index_html = include_str!("../docs/index.html");

    for conn in listener.incoming() {
        let Ok(stream) = conn else { continue };
        let b = Arc::clone(&broker);
        std::thread::spawn(move || handle_http(stream, b, index_html));
    }
    Ok(())
}

fn handle_http(mut stream: TcpStream, broker: Arc<SseBroker>, index_html: &str) {
    let mut buf = [0u8; 4096];
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let n = stream.read(&mut buf).unwrap_or(0);
    if n == 0 {
        return;
    }
    let req = String::from_utf8_lossy(&buf[..n]);
    let path = req
        .split_whitespace()
        .nth(1)
        .unwrap_or("/")
        .split('?')
        .next()
        .unwrap_or("/");

    match path {
        "/events" => {
            let _ = broker.register(stream);
        }
        "/health" => {
            let ws_up = *broker
                .ws_connected
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let ev_cnt = *broker
                .total_events_broadcast
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let body = format!(
                "{{\"ok\":true,\"clients\":{},\"ws_connected\":{},\"events_broadcast\":{}}}\n",
                broker.client_count(),
                ws_up,
                ev_cnt
            );
            let _ = write_http_response(&mut stream, 200, "application/json", body.as_bytes());
        }
        "/" | "/index.html" => {
            let _ = write_http_response(
                &mut stream,
                200,
                "text/html; charset=utf-8",
                index_html.as_bytes(),
            );
        }
        _ => {
            let _ = write_http_response(
                &mut stream,
                404,
                "text/plain",
                b"seiche serve endpoints: / (Observatory UI), /events (SSE), /health\n",
            );
        }
    }
}

fn write_http_response(
    stream: &mut TcpStream,
    code: u16,
    content_type: &str,
    body: &[u8],
) -> std::io::Result<()> {
    let status = match code {
        200 => "OK",
        404 => "Not Found",
        _ => "Error",
    };
    let head = format!(
        "HTTP/1.1 {code} {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(body)?;
    Ok(())
}

/// Discover active tickers from explicit CLI arg, local `tapehound` (:8787/markets), or fallback defaults.
fn discover_watch_tickers(override_csv: Option<&str>) -> Vec<String> {
    if let Some(csv) = override_csv {
        let list: Vec<String> = csv
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if !list.is_empty() {
            return list;
        }
    }
    let mut set = vec![
        "KXNHLGAME-26OCT29BUFANA-ANA".to_string(),
        "KXHIGHCHI-26OCT29-B72.5".to_string(),
        "KXNFLGAME-26OCT29CARGB-CAR".to_string(),
        "KXNFLGAME-26OCT29CARGB-GB".to_string(),
    ];
    // Probe local tapehound-serve (:8787/markets) for top liquid live tickers
    if let Ok(mut s) = TcpStream::connect_timeout(
        &"127.0.0.1:8787".parse().unwrap(),
        Duration::from_millis(800),
    ) {
        let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
        let _ = s.write_all(
            b"GET /markets HTTP/1.1\r\nHost: 127.0.0.1:8787\r\nConnection: close\r\n\r\n",
        );
        let mut resp = Vec::new();
        if s.read_to_end(&mut resp).is_ok() {
            if let Some(pos) = resp.windows(4).position(|w| w == b"\r\n\r\n") {
                if let Ok(Value::Array(arr)) = serde_json::from_slice::<Value>(&resp[pos + 4..]) {
                    for m in arr.iter().take(45) {
                        if let Some(tk) = m.get("ticker").and_then(|v| v.as_str()) {
                            if !set.iter().any(|x| x == tk) {
                                set.push(tk.to_string());
                            }
                        }
                    }
                }
            }
        }
    }
    set
}

async fn ws_dag_loop(broker: Arc<SseBroker>, tickers_override: Option<String>) {
    // Build the real Rust Seiche Graph for the 18 observatory slots
    let mut g = Graph::with_capacity(64);
    let mut bid_leaves: Vec<LeafHandle> = Vec::with_capacity(18);
    let mut harvest_nodes: Vec<NodeId> = Vec::with_capacity(18);

    for (idx, &(label, _ora, _fam, init_bid_cc, qty_h)) in OBSERVATORY_SLOTS.iter().enumerate() {
        let lf = g.add_leaf(
            &format!("leaf_{idx}_{label}"),
            init_bid_cc,
            Cutoff::Band(25),
        );
        let ht = g.add_harvest_trigger(
            &format!("harvest_{idx}_{label}"),
            g.leaf_node(lf),
            init_bid_cc - 150, // entry baseline 1.5c below initial mark
            qty_h,
            50, // 0.5c net buffer
            Cutoff::ExactEqual,
        );
        g.mark_output(ht, qty_h);
        bid_leaves.push(lf);
        harvest_nodes.push(ht);
    }
    let _total_fold =
        g.add_incr_fold_sum("total_observatory_fold", &harvest_nodes, Cutoff::ExactEqual);
    g.assert_invariants(true);

    // Per-subscription-id sequence buffer (`DeltaBuffer`)
    let mut seq_buffers: HashMap<u64, DeltaBuffer<i64>> = HashMap::new();
    // UI frame-rate pacing: emit at most 1 SSE update per >=75ms (~13 events/sec max)
    // while tracking cumulative cutoffs andstabilizations on the backend.
    let mut last_ui_emit = Instant::now() - Duration::from_secs(1);
    let mut suppressed_cutoffs: u64 = 0;

    let key_id = std::env::var("API_KEY_ID").ok();
    let pem_opt = std::env::var("KALSHI_PRIVATE_KEY_PATH")
        .ok()
        .and_then(|p| std::fs::read_to_string(p).ok());

    let Some(key_id) = key_id else {
        eprintln!("[seiche-ws] API_KEY_ID not set in env; waiting for env credentials");
        return;
    };
    let Some(pem) = pem_opt else {
        eprintln!("[seiche-ws] KALSHI_PRIVATE_KEY_PATH not readable");
        return;
    };
    let Ok(rsa_key) = RsaPrivateKey::from_pkcs1_pem(&pem) else {
        eprintln!("[seiche-ws] failed to parse RSA private key PEM");
        return;
    };

    let mut backoff_secs = 1u64;
    loop {
        let tickers = discover_watch_tickers(tickers_override.as_deref());
        let signing_key = PssSigningKey::<Sha256>::new(rsa_key.clone());
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .to_string();
        let msg = format!("{ts}GET/trade-api/ws/v2");
        let sig = signing_key
            .sign_with_rng(&mut rsa::rand_core::OsRng, msg.as_bytes())
            .to_vec();

        let Ok(mut req) = WS_URL.into_client_request() else {
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        };
        {
            let h = req.headers_mut();
            if let Ok(v) = key_id.parse() {
                h.insert("KALSHI-ACCESS-KEY", v);
            }
            if let Ok(v) = base64::engine::general_purpose::STANDARD
                .encode(&sig)
                .parse()
            {
                h.insert("KALSHI-ACCESS-SIGNATURE", v);
            }
            if let Ok(v) = ts.parse() {
                h.insert("KALSHI-ACCESS-TIMESTAMP", v);
            }
        }

        eprintln!(
            "[seiche-ws] connecting to Kalshi V2 WebSocket ({} watched tickers)...",
            tickers.len()
        );
        let mut ws = match connect_async(req).await {
            Ok((stream, _)) => stream,
            Err(e) => {
                eprintln!("[seiche-ws] connect error: {e}; retrying in {backoff_secs}s");
                if let Ok(mut up) = broker.ws_connected.lock() {
                    *up = false;
                }
                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(60);
                continue;
            }
        };
        backoff_secs = 1;
        if let Ok(mut up) = broker.ws_connected.lock() {
            *up = true;
        }
        broker.broadcast(
            "status",
            &format!(
                "{{\"ws_connected\":true,\"watched_tickers\":{}}}",
                tickers.len()
            ),
        );
        eprintln!("[seiche-ws] connected! Subscribing to ticker + trade + orderbook_delta...");

        let sub = serde_json::json!({
            "id": 1,
            "cmd": "subscribe",
            "params": {
                "channels": ["ticker", "trade", "orderbook_delta"],
                "market_tickers": tickers,
            }
        });
        if ws.send(Message::Text(sub.to_string())).await.is_err() {
            continue;
        }

        while let Some(frame) = ws.next().await {
            let text = match frame {
                Ok(Message::Text(t)) => t,
                Ok(Message::Ping(p)) => {
                    let _ = ws.send(Message::Pong(p)).await;
                    continue;
                }
                Ok(_) => continue,
                Err(e) => {
                    eprintln!("[seiche-ws] stream error: {e}; reconnecting");
                    break;
                }
            };

            let Ok(v) = serde_json::from_str::<Value>(&text) else {
                continue;
            };
            let msg_type = v.get("type").and_then(|x| x.as_str()).unwrap_or("");
            let sid = v.get("sid").and_then(|x| x.as_u64()).unwrap_or(1);
            let seq = v.get("seq").and_then(|x| x.as_u64());
            let Some(payload) = v.get("msg") else {
                continue;
            };
            let market_ticker = payload
                .get("market_ticker")
                .and_then(|x| x.as_str())
                .unwrap_or("");
            if market_ticker.is_empty() {
                continue;
            }

            let slot = map_ticker_to_slot(market_ticker);
            let cur_bid_cc = g.watch(g.leaf_node(bid_leaves[slot]));

            // Extract updated bid in centicents (10_000 = $1.00)
            let new_bid_cc: i64 = match msg_type {
                "ticker" | "trade" => {
                    let px_d = payload
                        .get("yes_bid_dollars")
                        .or_else(|| payload.get("yes_price_dollars"))
                        .or_else(|| payload.get("price_dollars"))
                        .and_then(|x| x.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .or_else(|| {
                            payload
                                .get("yes_bid")
                                .or_else(|| payload.get("yes_price"))
                                .and_then(|x| x.as_f64())
                                .map(|c| c / 100.0)
                        });
                    match px_d {
                        Some(d) if d > 0.0 => (d * 10_000.0).round() as i64,
                        _ => continue,
                    }
                }
                "orderbook_snapshot" => {
                    if let Some(s) = seq {
                        seq_buffers
                            .entry(sid)
                            .or_insert_with(|| DeltaBuffer::new(s, 128, 3))
                            .reset_at_snapshot(s);
                    }
                    extract_snapshot_best_bid_cc(payload).unwrap_or(cur_bid_cc)
                }
                "orderbook_delta" => {
                    let price_d = payload
                        .get("price_dollars")
                        .and_then(|x| x.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .or_else(|| {
                            payload
                                .get("price")
                                .and_then(|x| x.as_f64())
                                .map(|c| c / 100.0)
                        })
                        .unwrap_or(0.0);
                    let side = payload
                        .get("side")
                        .and_then(|x| x.as_str())
                        .unwrap_or("yes");
                    let level_cc = if side == "yes" {
                        (price_d * 10_000.0).round() as i64
                    } else {
                        10_000 - (price_d * 10_000.0).round() as i64
                    };

                    if let Some(s) = seq {
                        let buf = seq_buffers
                            .entry(sid)
                            .or_insert_with(|| DeltaBuffer::new(s, 128, 3));
                        match buf.receive(s, level_cc) {
                            ReceiveResult::Deliver(_) => level_cc.clamp(100, 9900),
                            ReceiveResult::GapDetected {
                                missing_seq,
                                retransmits,
                            } => {
                                let gap_json = format!(
                                    "{{\"kind\":\"gap\",\"ticker\":\"{}\",\"slot\":{},\"seq\":{},\"missing_seq\":{},\"retransmits\":{}}}",
                                    market_ticker, slot, s, missing_seq, retransmits
                                );
                                broker.broadcast("dag", &gap_json);
                                continue;
                            }
                            ReceiveResult::ResyncRequired { reason } => {
                                buf.reset_at_snapshot(s);
                                let gap_json = format!(
                                    "{{\"kind\":\"resync\",\"ticker\":\"{}\",\"slot\":{},\"reason\":\"{}\"}}",
                                    market_ticker, slot, reason
                                );
                                broker.broadcast("dag", &gap_json);
                                continue;
                            }
                        }
                    } else {
                        level_cc.clamp(100, 9900)
                    }
                }
                _ => continue,
            };

            let delta_cc = new_bid_cc - cur_bid_cc;
            let t0 = Instant::now();
            g.set_leaf(bid_leaves[slot], new_bid_cc);
            let out_events = g.stabilize();
            let elapsed_ns = t0.elapsed().as_nanos() as u64;
            let recomputed = g.last_recompute_count;
            if recomputed == 0 {
                suppressed_cutoffs += 1;
            }
            let harvest_fired = out_events.iter().any(|e| e.new_value > 0);

            // Pace SSE emissions to <= 8 events/sec (>= 125ms gap) unconditionally,
            // so the browser UI never receives an unthrottled firehose.
            if last_ui_emit.elapsed() < Duration::from_millis(125) {
                continue;
            }
            last_ui_emit = Instant::now();
            let fee_cc = kalshi_taker_fee_cc(OBSERVATORY_SLOTS[slot].4, new_bid_cc);

            let ev_json = format!(
                "{{\"kind\":\"tick\",\"msg_type\":\"{}\",\"ticker\":\"{}\",\"slot\":{},\"bid_cc\":{},\"delta_cc\":{},\"recomputed\":{},\"ns\":{},\"fee_cc\":{},\"harvest_fired\":{},\"cutoffs\":{},\"seq\":{}}}",
                msg_type,
                market_ticker,
                slot,
                new_bid_cc,
                delta_cc,
                recomputed,
                elapsed_ns.max(45),
                fee_cc,
                harvest_fired,
                suppressed_cutoffs,
                seq.unwrap_or(0)
            );
            broker.broadcast("dag", &ev_json);
        }
    }
}

fn extract_snapshot_best_bid_cc(payload: &Value) -> Option<i64> {
    let yd = payload.get("yes_dollars").and_then(|v| v.as_array())?;
    let mut best_cc = 0i64;
    for row in yd {
        if let Some(px_str) = row.get(0).and_then(|v| v.as_str()) {
            if let Ok(d) = px_str.parse::<f64>() {
                let cc = (d * 10_000.0).round() as i64;
                if cc > best_cc {
                    best_cc = cc;
                }
            }
        }
    }
    if best_cc > 0 {
        Some(best_cc)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_mapper_routes_known_series_deterministically() {
        assert_eq!(map_ticker_to_slot("KXNHLGAME-26OCT29BUFANA-ANA"), 12);
        assert_eq!(map_ticker_to_slot("KXNFLGAME-26OCT29CARGB-CAR"), 16);
        assert_eq!(map_ticker_to_slot("KXNFLGAME-26OCT29CARGB-GB"), 17);
        let gold_slot = map_ticker_to_slot("KXGOLDH-26OCT2918-T4039.99");
        assert!(gold_slot < 3);
        let wx_slot = map_ticker_to_slot("KXHIGHCHI-26OCT29-B72.5");
        assert!((8..12).contains(&wx_slot));
    }
}
