use once_cell::sync::OnceCell;
use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender};

use crate::gql::query_interface::QueryOutcome;

// Global sender that Actix handlers use to send requests into the GUI thread
static API_REQ_TX: OnceCell<Sender<ApiRequest>> = OnceCell::new();

#[derive(Debug, Clone)]
pub struct ApiRequest {
    pub api_key: Option<String>,
    pub request_id: String,
    pub query: String,
    pub params: Option<HashMap<String, String>>, // optional
    pub log: bool,
    pub respond_to: Sender<Result<QueryOutcome, String>>, // Ok = outcome, Err = error string
}

pub fn set_request_sender(tx: Sender<ApiRequest>) {
    let _ = API_REQ_TX.set(tx);
}

pub fn get_request_sender() -> Option<&'static Sender<ApiRequest>> {
    API_REQ_TX.get()
}

// Called by GUI when starting up to create the broker pair
pub fn init_broker() -> Receiver<ApiRequest> {
    let (tx, rx) = std::sync::mpsc::channel();
    set_request_sender(tx);
    rx
}

// Server lifecycle API (feature-gated). Non-API builds get no-op stubs.
#[cfg(feature = "api")]
pub mod server;
#[cfg(feature = "api")]
pub mod grpc;

#[cfg(not(feature = "api"))]
pub mod server {
    use crate::persistence::settings::AppSettings;

    pub fn start_server(_cfg: &AppSettings) -> anyhow::Result<()> { Ok(()) }
    pub fn stop_server() {}
    pub fn is_running() -> bool { false }
}

#[cfg(not(feature = "api"))]
pub mod grpc {
    use crate::persistence::settings::AppSettings;
    pub fn start_grpc_server(_cfg: &AppSettings) -> anyhow::Result<()> { Ok(()) }
    pub fn stop_grpc_server() {}
}
