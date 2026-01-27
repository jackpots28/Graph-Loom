//! Actix-web server for Graph-Loom API (feature-gated)

use std::collections::HashMap;
use std::sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}};
use std::time::Duration;

use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

use super::{get_request_sender, ApiRequest};
use crate::gql::query_interface::{QueryOutcome, QueryResultRow};
use crate::persistence::settings::AppSettings;

// Store server state for stop/restart
struct ServerState {
    handle: Option<actix_web::dev::ServerHandle>,
    runtime: Option<Runtime>,
}

static SERVER_STATE: once_cell::sync::Lazy<Arc<Mutex<ServerState>>> = once_cell::sync::Lazy::new(|| {
    Arc::new(Mutex::new(ServerState { handle: None, runtime: None }))
});

static REQ_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
struct Cfg {
    api_key: Option<String>,
    log_dir: std::path::PathBuf,
}

fn ensure_dir(p: &std::path::Path) {
    if let Some(parent) = p.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
}

fn log_line(dir: &std::path::Path, line: &str) {
    use std::io::Write;
    let now = time::OffsetDateTime::now_utc();
    let date = time::macros::format_description!("[year][month][day]");
    let ts = time::macros::format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");
    let fname = match now.format(&date) { Ok(s) => format!("api_{}.log", s), Err(_) => "api.log".to_string() };
    let path = dir.join(fname);
    ensure_dir(&path);
    let ts_s = now.format(&ts).unwrap_or_else(|_| String::new());
    let msg = format!("{} | {}\n", ts_s, line);
    if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(&path) {
        let _ = f.write_all(msg.as_bytes());
    }
}

fn next_request_id() -> String {
    let n = REQ_COUNTER.fetch_add(1, Ordering::Relaxed);
    let now = time::OffsetDateTime::now_utc().unix_timestamp_nanos();
    format!("{}-{}", now, n)
}

#[derive(Deserialize)]
struct QueryBody {
    query: String,
    #[serde(default)]
    params: Option<HashMap<String, String>>,
    #[serde(default)]
    log: Option<bool>,
}

#[derive(Serialize)]
struct OutcomeRowDto {
    kind: &'static str,
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")] label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] to: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] metadata: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")] info: Option<String>,
}

#[derive(Serialize)]
struct OutcomeDto {
    rows: Vec<OutcomeRowDto>,
    affected_nodes: usize,
    affected_relationships: usize,
    mutated: bool,
}

fn map_outcome(o: QueryOutcome) -> OutcomeDto {
    let mut rows = Vec::with_capacity(o.rows.len());
    for r in o.rows {
        match r {
            QueryResultRow::Node { id, label, metadata } => rows.push(OutcomeRowDto {
                kind: "node",
                id: id.to_string(),
                label: Some(label),
                from: None,
                to: None,
                metadata: Some(metadata),
                info: None,
            }),
            QueryResultRow::Relationship { id, from, to, label, metadata } => rows.push(OutcomeRowDto {
                kind: "relationship",
                id: id.to_string(),
                label: Some(label),
                from: Some(from.to_string()),
                to: Some(to.to_string()),
                metadata: Some(metadata),
                info: None,
            }),
            QueryResultRow::Info(s) => rows.push(OutcomeRowDto {
                kind: "info",
                id: String::new(),
                label: None,
                from: None,
                to: None,
                metadata: None,
                info: Some(s),
            }),
        }
    }
    OutcomeDto {
        rows,
        affected_nodes: o.affected_nodes,
        affected_relationships: o.affected_relationships,
        mutated: o.mutated,
    }
}

fn unauthorized() -> HttpResponse { HttpResponse::Unauthorized().body("unauthorized") }

fn check_api_key(req: &HttpRequest, cfg: &Cfg) -> bool {
    match &cfg.api_key {
        None => true,
        Some(required) => match req.headers().get("X-API-Key") {
            Some(h) => h.to_str().map(|v| v == required).unwrap_or(false),
            None => false,
        },
    }
}

async fn handle_query(cfg: web::Data<Cfg>, req: HttpRequest, body: web::Json<QueryBody>) -> impl Responder {
    if !check_api_key(&req, &cfg) { return unauthorized(); }
    let sender = match get_request_sender() { Some(s) => s.clone(), None => return HttpResponse::ServiceUnavailable().body("broker not ready") };
    let (tx, rx) = std::sync::mpsc::channel();
    let rid = next_request_id();
    let api_req = ApiRequest {
        request_id: rid.clone(),
        query: body.query.clone(),
        params: body.params.clone(),
        log: body.log.unwrap_or(true),
        respond_to: tx,
    };
    let peer = req.peer_addr().map(|a| a.to_string()).unwrap_or_else(|| "unknown".into());
    log_line(&cfg.log_dir, &format!("RID={} HTTP /api/query from {} qlen={} params={} log={}", rid, peer, api_req.query.len(), api_req.params.as_ref().map(|m| m.len()).unwrap_or(0), api_req.log));
    let t0 = std::time::Instant::now();
    if sender.send(api_req).is_err() {
        log_line(&cfg.log_dir, &format!("RID={} enqueue failed", rid));
        return HttpResponse::ServiceUnavailable().body("failed to enqueue");
    }
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(Ok(out)) => {
            let dt = t0.elapsed();
            log_line(&cfg.log_dir, &format!("RID={} HTTP OK nodes={} rels={} mutated={} dt_ms={}", rid, out.affected_nodes, out.affected_relationships, out.mutated, dt.as_millis()));
            HttpResponse::Ok().json(map_outcome(out))
        }
        Ok(Err(e)) => {
            let dt = t0.elapsed();
            log_line(&cfg.log_dir, &format!("RID={} HTTP ERR {} dt_ms={}", rid, e, dt.as_millis()));
            HttpResponse::BadRequest().body(e)
        }
        Err(_) => {
            let dt = t0.elapsed();
            log_line(&cfg.log_dir, &format!("RID={} HTTP TIMEOUT dt_ms={}", rid, dt.as_millis()));
            HttpResponse::GatewayTimeout().body("query timeout")
        }
    }
}

// Simple WebSocket REPL: line-per-query
use actix_web_actors::ws;

struct ReplWs { cfg: Cfg }

impl ReplWs { fn new(cfg: Cfg) -> Self { Self { cfg } } }

impl actix::Actor for ReplWs {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.text("Graph-Loom REPL ready. Send queries as text.\n");
        log_line(&self.cfg.log_dir, "WS connected");
    }
}

impl actix::StreamHandler<Result<ws::Message, ws::ProtocolError>> for ReplWs {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Text(text)) => {
                let sender = match get_request_sender() { Some(s) => s.clone(), None => { ctx.text("broker not ready"); return; } };
                let q = text.trim().to_string();
                if q.is_empty() { return; }
                let rid = next_request_id();
                log_line(&self.cfg.log_dir, &format!("RID={} WS query qlen={}", rid, q.len()));
                let (tx, rx) = std::sync::mpsc::channel();
                let req = ApiRequest { request_id: rid.clone(), query: q, params: None, log: true, respond_to: tx };
                let t0 = std::time::Instant::now();
                if sender.send(req).is_err() { ctx.text("enqueue failed"); return; }
                match rx.recv_timeout(Duration::from_secs(60)) {
                    Ok(Ok(out)) => {
                        let dto = map_outcome(out);
                        let s = serde_json::to_string_pretty(&dto).unwrap_or_else(|_| "{}".into());
                        ctx.text(s);
                        let dt = t0.elapsed();
                        log_line(&self.cfg.log_dir, &format!("RID={} WS OK dt_ms={}", rid, dt.as_millis()));
                    }
                    Ok(Err(e)) => { let dt = t0.elapsed(); log_line(&self.cfg.log_dir, &format!("RID={} WS ERR {} dt_ms={}", rid, e, dt.as_millis())); ctx.text(format!("error: {}", e)) }
                    Err(_) => { let dt = t0.elapsed(); log_line(&self.cfg.log_dir, &format!("RID={} WS TIMEOUT dt_ms={}", rid, dt.as_millis())); ctx.text("timeout") }
                }
            }
            Ok(ws::Message::Ping(b)) => ctx.pong(&b),
            Ok(ws::Message::Close(_)) => { log_line(&self.cfg.log_dir, "WS closed"); ctx.close(None) },
            _ => {}
        }
    }
}

async fn ws_handler(cfg: web::Data<Cfg>, req: HttpRequest, stream: web::Payload) -> actix_web::Result<HttpResponse> {
    if !check_api_key(&req, &cfg) { return Ok(unauthorized()); }
    ws::start(ReplWs::new(cfg.get_ref().clone()), &req, stream)
}

pub fn start_server(cfg: &AppSettings) -> anyhow::Result<()> {
    let bind = cfg.api_endpoint();
    let api_key = cfg.api_key.clone();
    let log_dir = cfg.api_log_dir();
    stop_server();

    std::thread::spawn(move || {
        let rt = match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build() {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("[Graph-Loom] Failed to create tokio runtime for API: {}", e);
                    return;
                }
            };
        
        rt.block_on(async move {
            let cfg_data = Cfg { api_key, log_dir: log_dir.clone() };
            log_line(&cfg_data.log_dir, &format!("Server starting on {}", bind));
            let server = match HttpServer::new(move || {
                App::new()
                    .app_data(web::Data::new(cfg_data.clone()))
                    .route("/api/query", web::post().to(handle_query))
                    .route("/api/repl", web::get().to(ws_handler))
            })
            .bind(&bind) {
                Ok(s) => s.run(),
                Err(e) => {
                    eprintln!("[Graph-Loom] API server bind failed on {}: {}", bind, e);
                    return;
                }
            };
            {
                let mut st = SERVER_STATE.lock().unwrap();
                st.handle = Some(server.handle());
            }
            let _ = server.await;
        });
        {
            let mut st = SERVER_STATE.lock().unwrap();
            st.runtime = Some(rt);
        }
    });
    Ok(())
}

pub fn stop_server() {
    let (handle, rt) = {
        let mut st = SERVER_STATE.lock().unwrap();
        (st.handle.take(), st.runtime.take())
    };
    if let Some(h) = handle {
        let _ = h.stop(false);
    }
    if let Some(r) = rt {
        r.shutdown_timeout(Duration::from_millis(100));
    }
}

#[allow(dead_code)]
pub fn is_running() -> bool { SERVER_STATE.lock().unwrap().handle.is_some() }
