use std::sync::{Arc, Mutex};
use tonic::{transport::Server, Request, Response, Status};

use crate::api::{get_request_sender, ApiRequest};
use crate::gql::query_interface::QueryResultRow;
use crate::persistence::settings::AppSettings;

pub mod proto {
    tonic::include_proto!("graph_loom");
}

use proto::graph_query_server::{GraphQuery, GraphQueryServer};
use proto::{QueryRequest, QueryResponse, QueryRow, Node, Relationship};

#[derive(Default)]
pub struct MyGraphQuery {
    api_key: Option<String>,
}

#[tonic::async_trait]
impl GraphQuery for MyGraphQuery {
    async fn execute(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        if let Some(required_key) = &self.api_key {
            let metadata = request.metadata();
            match metadata.get("x-api-key") {
                Some(key) if key == required_key => {}
                _ => return Err(Status::unauthenticated("invalid or missing api key")),
            }
        }

        let req = request.into_inner();
        let sender = match get_request_sender() {
            Some(s) => s.clone(),
            None => return Err(Status::unavailable("broker not ready")),
        };

        let (tx, rx) = std::sync::mpsc::channel();
        let api_req = ApiRequest {
            request_id: format!("grpc-{}", uuid::Uuid::now_v7()),
            query: req.query.clone(),
            params: Some(req.params),
            log: req.log,
            respond_to: tx,
        };

        if sender.send(api_req).is_err() {
            return Err(Status::internal("failed to enqueue request"));
        }

        match rx.recv_timeout(std::time::Duration::from_secs(30)) {
            Ok(Ok(out)) => {
                let mut rows = Vec::with_capacity(out.rows.len());
                for r in out.rows {
                    let row = match r {
                        QueryResultRow::Node { id, label, metadata } => QueryRow {
                            item: Some(proto::query_row::Item::Node(Node {
                                id: id.to_string(),
                                label,
                                metadata,
                            })),
                        },
                        QueryResultRow::Relationship { id, from, to, label, metadata } => QueryRow {
                            item: Some(proto::query_row::Item::Relationship(Relationship {
                                id: id.to_string(),
                                from_id: from.to_string(),
                                to_id: to.to_string(),
                                label,
                                metadata,
                            })),
                        },
                        QueryResultRow::Info(s) => QueryRow {
                            item: Some(proto::query_row::Item::Info(s)),
                        },
                    };
                    rows.push(row);
                }
                Ok(Response::new(QueryResponse {
                    rows,
                    affected_nodes: out.affected_nodes as u64,
                    affected_relationships: out.affected_relationships as u64,
                    mutated: out.mutated,
                    error: String::new(),
                }))
            }
            Ok(Err(e)) => Ok(Response::new(QueryResponse {
                rows: vec![],
                affected_nodes: 0,
                affected_relationships: 0,
                mutated: false,
                error: e,
            })),
            Err(_) => Err(Status::deadline_exceeded("query timeout")),
        }
    }
}

struct GrpcServerState {
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    runtime: Option<tokio::runtime::Runtime>,
}

static GRPC_SERVER_STATE: once_cell::sync::Lazy<Arc<Mutex<GrpcServerState>>> =
    once_cell::sync::Lazy::new(|| {
        Arc::new(Mutex::new(GrpcServerState { shutdown_tx: None, runtime: None }))
    });

pub fn start_grpc_server(cfg: &AppSettings) -> anyhow::Result<()> {
    if !cfg.grpc_enabled {
        return Ok(());
    }

    stop_grpc_server();

    let addr = format!("{}:{}", cfg.api_bind_addr, cfg.grpc_port).parse()?;
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let api_key = cfg.api_key.clone();

    {
        let mut state = GRPC_SERVER_STATE.lock().unwrap();
        state.shutdown_tx = Some(tx);
    }

    std::thread::spawn(move || {
        let rt = match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build() {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("[Graph-Loom] Failed to create tokio runtime for gRPC: {}", e);
                    return;
                }
            };

        rt.block_on(async {
            let service = MyGraphQuery { api_key };
            if let Err(e) = Server::builder()
                .add_service(GraphQueryServer::new(service))
                .serve_with_shutdown(addr, async {
                    let _ = rx.await;
                })
                .await {
                    eprintln!("[Graph-Loom] gRPC server failed: {}", e);
                }
        });
        {
            let mut state = GRPC_SERVER_STATE.lock().unwrap();
            state.runtime = Some(rt);
        }
    });

    Ok(())
}

pub fn stop_grpc_server() {
    let (shutdown_tx, rt) = {
        let mut state = GRPC_SERVER_STATE.lock().unwrap();
        (state.shutdown_tx.take(), state.runtime.take())
    };
    if let Some(tx) = shutdown_tx {
        let _ = tx.send(());
    }
    if let Some(r) = rt {
        r.shutdown_timeout(std::time::Duration::from_millis(100));
    }
}
