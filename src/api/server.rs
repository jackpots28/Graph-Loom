//! Actix-web server for Graph-Loom API (feature-gated)

use std::collections::HashMap;
use std::sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}};
use std::sync::atomic::AtomicU32;
use std::time::Duration;

use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

use super::{get_request_sender, ApiRequest};
use super::auth::{AuthConfig, AuthContext, AuthResult, validate_api_key};
use crate::gql::query_interface::{QueryOutcome, QueryResultRow};
use crate::persistence::settings::AppSettings;
use crate::persistence::persist::{active_sqlite_path, active_state_path};

// Store server state for stop/restart
struct ServerState {
    handle: Option<actix_web::dev::ServerHandle>,
    runtime: Option<Runtime>,
}

static SERVER_STATE: once_cell::sync::Lazy<Arc<Mutex<ServerState>>> = once_cell::sync::Lazy::new(|| {
    Arc::new(Mutex::new(ServerState { handle: None, runtime: None }))
});

static REQ_COUNTER: AtomicU64 = AtomicU64::new(1);

// Prometheus metrics
static METRICS_QUERIES_SUCCESS: AtomicU64 = AtomicU64::new(0);
static METRICS_QUERIES_ERROR: AtomicU64 = AtomicU64::new(0);
static METRICS_QUERIES_TIMEOUT: AtomicU64 = AtomicU64::new(0);
static METRICS_QUERY_DURATION_MS_SUM: AtomicU64 = AtomicU64::new(0);
static METRICS_WS_CONNECTIONS: AtomicU32 = AtomicU32::new(0);

#[derive(Clone)]
struct Cfg {
    api_key: Option<String>,
    log_dir: std::path::PathBuf,
    auth_config: AuthConfig,
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

#[derive(Serialize)]
struct PathsDto {
    state_db: String,
    state_ron: String,
    settings_dir: String,
    log_dir: String,
}

async fn handle_paths(cfg: web::Data<Cfg>, req: HttpRequest) -> impl Responder {
    if let Err(resp) = authenticate(&req, &cfg) {
        return resp;
    }
    let dto = PathsDto {
        state_db: active_sqlite_path().display().to_string(),
        state_ron: active_state_path().display().to_string(),
        settings_dir: AppSettings::settings_dir().display().to_string(),
        log_dir: cfg.log_dir.display().to_string(),
    };
    HttpResponse::Ok().json(dto)
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
fn forbidden() -> HttpResponse { HttpResponse::Forbidden().body("forbidden") }

/// Prometheus metrics endpoint (no auth required for scraping)
async fn handle_metrics() -> impl Responder {
    use crate::persistence::persist::get_sqlite_storage;
    
    let queries_success = METRICS_QUERIES_SUCCESS.load(Ordering::Relaxed);
    let queries_error = METRICS_QUERIES_ERROR.load(Ordering::Relaxed);
    let queries_timeout = METRICS_QUERIES_TIMEOUT.load(Ordering::Relaxed);
    let query_duration_sum = METRICS_QUERY_DURATION_MS_SUM.load(Ordering::Relaxed) as f64 / 1000.0;
    let ws_connections = METRICS_WS_CONNECTIONS.load(Ordering::Relaxed);
    let api_up = if SERVER_STATE.lock().unwrap().handle.is_some() { 1 } else { 0 };
    
    // Get graph stats from SQLite
    let (nodes_total, rels_total, tfidf_embeddings, word2vec_embeddings, onnx_embeddings) = 
        if let Some(storage) = get_sqlite_storage() {
            match storage.get_stats() {
                Ok(stats) => (stats.node_count, stats.relationship_count, 
                              stats.tfidf_embeddings, stats.word2vec_embeddings, stats.onnx_embeddings),
                Err(_) => (0, 0, 0, 0, 0),
            }
        } else {
            (0, 0, 0, 0, 0)
        };
    
    let version = env!("CARGO_PKG_VERSION");
    
    let body = format!(
        "# HELP graph_loom_queries_total Total number of queries processed\n\
         # TYPE graph_loom_queries_total counter\n\
         graph_loom_queries_total{{status=\"success\"}} {}\n\
         graph_loom_queries_total{{status=\"error\"}} {}\n\
         graph_loom_queries_total{{status=\"timeout\"}} {}\n\
         # HELP graph_loom_query_duration_seconds_sum Total query processing time in seconds\n\
         # TYPE graph_loom_query_duration_seconds_sum counter\n\
         graph_loom_query_duration_seconds_sum {}\n\
         # HELP graph_loom_api_up Whether the API server is running\n\
         # TYPE graph_loom_api_up gauge\n\
         graph_loom_api_up {}\n\
         # HELP graph_loom_websocket_connections Current WebSocket connections\n\
         # TYPE graph_loom_websocket_connections gauge\n\
         graph_loom_websocket_connections {}\n\
         # HELP graph_loom_nodes_total Total number of nodes in the graph\n\
         # TYPE graph_loom_nodes_total gauge\n\
         graph_loom_nodes_total {}\n\
         # HELP graph_loom_relationships_total Total number of relationships in the graph\n\
         # TYPE graph_loom_relationships_total gauge\n\
         graph_loom_relationships_total {}\n\
         # HELP graph_loom_embeddings_total Total embeddings by model\n\
         # TYPE graph_loom_embeddings_total gauge\n\
         graph_loom_embeddings_total{{model=\"tfidf\"}} {}\n\
         graph_loom_embeddings_total{{model=\"word2vec\"}} {}\n\
         graph_loom_embeddings_total{{model=\"onnx\"}} {}\n\
         # HELP graph_loom_info Graph-Loom version info\n\
         # TYPE graph_loom_info gauge\n\
         graph_loom_info{{version=\"{}\"}} 1\n",
        queries_success, queries_error, queries_timeout,
        query_duration_sum, api_up, ws_connections,
        nodes_total, rels_total,
        tfidf_embeddings, word2vec_embeddings, onnx_embeddings,
        version
    );
    
    HttpResponse::Ok()
        .content_type("text/plain; version=0.0.4; charset=utf-8")
        .body(body)
}

/// Health check endpoint
async fn handle_health() -> impl Responder {
    use crate::persistence::persist::get_sqlite_storage;
    
    let api_up = SERVER_STATE.lock().unwrap().handle.is_some();
    let db_ok = get_sqlite_storage().is_some();
    let version = env!("CARGO_PKG_VERSION");
    
    let status = if api_up && db_ok { "healthy" } else { "degraded" };
    
    let body = serde_json::json!({
        "status": status,
        "components": {
            "api": api_up,
            "database": db_ok
        },
        "version": version
    });
    
    HttpResponse::Ok().json(body)
}

/// Authenticate the request and return an AuthContext if successful
fn authenticate(req: &HttpRequest, cfg: &Cfg) -> Result<AuthContext, HttpResponse> {
    let provided_key = req.headers().get("X-API-Key")
        .and_then(|h| h.to_str().ok());
    
    match validate_api_key(provided_key, cfg.api_key.as_deref(), &cfg.auth_config) {
        AuthResult::Success(ctx) => Ok(ctx),
        AuthResult::Disabled => Ok(AuthContext::anonymous()),
        AuthResult::InvalidCredentials => Err(unauthorized()),
        AuthResult::MissingCredentials => Err(unauthorized()),
        AuthResult::Expired => Err(HttpResponse::Unauthorized().body("token expired")),
    }
}

async fn handle_query(cfg: web::Data<Cfg>, req: HttpRequest, body: web::Json<QueryBody>) -> impl Responder {
    let auth_ctx = match authenticate(&req, &cfg) {
        Ok(ctx) => ctx,
        Err(resp) => return resp,
    };
    
    // Check if user has permission to execute this query
    if !auth_ctx.can_execute_query(&body.query) {
        return forbidden();
    }
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
            METRICS_QUERIES_SUCCESS.fetch_add(1, Ordering::Relaxed);
            METRICS_QUERY_DURATION_MS_SUM.fetch_add(dt.as_millis() as u64, Ordering::Relaxed);
            log_line(&cfg.log_dir, &format!("RID={} HTTP OK nodes={} rels={} mutated={} dt_ms={}", rid, out.affected_nodes, out.affected_relationships, out.mutated, dt.as_millis()));
            HttpResponse::Ok().json(map_outcome(out))
        }
        Ok(Err(e)) => {
            let dt = t0.elapsed();
            METRICS_QUERIES_ERROR.fetch_add(1, Ordering::Relaxed);
            METRICS_QUERY_DURATION_MS_SUM.fetch_add(dt.as_millis() as u64, Ordering::Relaxed);
            log_line(&cfg.log_dir, &format!("RID={} HTTP ERR {} dt_ms={}", rid, e, dt.as_millis()));
            HttpResponse::BadRequest().body(e)
        }
        Err(_) => {
            let dt = t0.elapsed();
            METRICS_QUERIES_TIMEOUT.fetch_add(1, Ordering::Relaxed);
            METRICS_QUERY_DURATION_MS_SUM.fetch_add(dt.as_millis() as u64, Ordering::Relaxed);
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
        METRICS_WS_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
        ctx.text("Graph-Loom REPL ready. Send queries as text.\n");
        log_line(&self.cfg.log_dir, "WS connected");
    }
    
    fn stopped(&mut self, _ctx: &mut Self::Context) {
        METRICS_WS_CONNECTIONS.fetch_sub(1, Ordering::Relaxed);
    }
}

impl actix::StreamHandler<Result<ws::Message, ws::ProtocolError>> for ReplWs {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Text(text)) => {
                let sender = match get_request_sender() { Some(s) => s.clone(), None => { ctx.text("broker not ready"); return; } };
                let q = text.trim().to_string();
                if q.is_empty() { return; }
                
                // For WebSocket, we use anonymous context (auth was checked at connection time)
                let auth_ctx = AuthContext::anonymous();
                if !auth_ctx.can_execute_query(&q) {
                    ctx.text("error: permission denied");
                    return;
                }
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
                        METRICS_QUERIES_SUCCESS.fetch_add(1, Ordering::Relaxed);
                        METRICS_QUERY_DURATION_MS_SUM.fetch_add(dt.as_millis() as u64, Ordering::Relaxed);
                        log_line(&self.cfg.log_dir, &format!("RID={} WS OK dt_ms={}", rid, dt.as_millis()));
                    }
                    Ok(Err(e)) => {
                        let dt = t0.elapsed();
                        METRICS_QUERIES_ERROR.fetch_add(1, Ordering::Relaxed);
                        METRICS_QUERY_DURATION_MS_SUM.fetch_add(dt.as_millis() as u64, Ordering::Relaxed);
                        log_line(&self.cfg.log_dir, &format!("RID={} WS ERR {} dt_ms={}", rid, e, dt.as_millis()));
                        ctx.text(format!("error: {}", e));
                    }
                    Err(_) => {
                        let dt = t0.elapsed();
                        METRICS_QUERIES_TIMEOUT.fetch_add(1, Ordering::Relaxed);
                        METRICS_QUERY_DURATION_MS_SUM.fetch_add(dt.as_millis() as u64, Ordering::Relaxed);
                        log_line(&self.cfg.log_dir, &format!("RID={} WS TIMEOUT dt_ms={}", rid, dt.as_millis()));
                        ctx.text("timeout");
                    }
                }
            }
            Ok(ws::Message::Ping(b)) => ctx.pong(&b),
            Ok(ws::Message::Close(_)) => { log_line(&self.cfg.log_dir, "WS closed"); ctx.close(None) },
            _ => {}
        }
    }
}

async fn ws_handler(cfg: web::Data<Cfg>, req: HttpRequest, stream: web::Payload) -> actix_web::Result<HttpResponse> {
    // Authenticate at WebSocket connection time
    if let Err(resp) = authenticate(&req, &cfg) {
        return Ok(resp);
    }
    ws::start(ReplWs::new(cfg.get_ref().clone()), &req, stream)
}

pub fn start_server(cfg: &AppSettings) -> anyhow::Result<()> {
    let bind = cfg.api_endpoint();
    let api_key = cfg.api_key.clone();
    let log_dir = cfg.api_log_dir();
    let auth_config = cfg.auth_config.clone();
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
            let cfg_data = Cfg { api_key, log_dir: log_dir.clone(), auth_config };
            log_line(&cfg_data.log_dir, &format!("Server starting on {}", bind));
            let server = match HttpServer::new(move || {
                App::new()
                    .app_data(web::Data::new(cfg_data.clone()))
                    .route("/api/query", web::post().to(handle_query))
                    .route("/api/repl", web::get().to(ws_handler))
                    .route("/api/paths", web::get().to(handle_paths))
                    .route("/metrics", web::get().to(handle_metrics))
                    .route("/health", web::get().to(handle_health))
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
