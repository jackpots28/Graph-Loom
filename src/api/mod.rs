//! # API Request and Broker System
//!
//! This module facilitates communication between the GUI thread and background processes
//! using message passing. It provides a global sender for API requests and introduces a
//! broker mechanism for handling message transmission.
//!
//! ## Global Entities
//!
//! The module defines a global sender (`API_REQ_TX`) which is lazily instantiated using the
//! `OnceCell` utility to ensure thread-safe initialization. This sender is used to transmit
//! API requests (`ApiRequest`) to the GUI thread.
//!
//! ## Structures
//!
//! ### `ApiRequest`
//! Represents a request sent through the global sender, designed to encapsulate all the necessary
//! data and metadata required to handle an API query.
//!
//! - **Fields**:
//!   - `request_id`: A unique identifier for the request.
//!   - `query`: The API query string.
//!   - `params`: An optional `HashMap` containing parameters for the query.
//!   - `log`: A boolean indicating whether logging is enabled for the request.
//!   - `respond_to`: A sender which receives the result of the query, either as a `QueryOutcome`
//!     on success or an error string on failure.
//!
//! ## Functions
//!
//! ### `set_request_sender`
//! 
//! pub fn set_request_sender(tx: Sender<ApiRequest>)
//! 
//! Allows setting the global API request sender (`API_REQ_TX`). The function uses the `OnceCell`
//! API to ensure that the sender is only initialized once. Any subsequent attempts to overwrite
//! the existing sender will fail silently.
//!
//! - **Parameters**:
//!   - `tx`: A `Sender` of type `ApiRequest` used to transmit requests.
//!
//! ### `get_request_sender`
//! 
//! pub fn get_request_sender() -> Option<&'static Sender<ApiRequest>>
//! 
//! Retrieves the global API request sender.
//!
//! - **Returns**:
//!   - `Some(&Sender<ApiRequest>)`: If the sender is initialized.
//!   - `None`: If the sender has not been initialized.
//!
//! ### `init_broker`
//! 
//! pub fn init_broker() -> Receiver<ApiRequest>
//! 
//! Creates a broker channel, initializing the global sender and returning a receiver.
//! This function is typically invoked during GUI startup.
//!
//! - **Returns**:
//!   - `Receiver<ApiRequest>`: A receiver that listens for incoming API requests.
//!
//! ## Modules
//!
//! ### `auth`
//! A module for handling authentication and Role-Based Access Control (RBAC). This module is not
//! elaborated here but is expected to define functions and utilities for user authentication and
//! authorization.
//!
//! ### `server`
//! A module providing server lifecycle management APIs, such as starting, stopping, and checking
//! the server status. Behavior is feature-gated by the `api` feature flag:
//!
//! - **With `api` feature**:
//!   Provides full server lifecycle management.
//!
//! - **Without `api` feature**:
//!   Provides no-op stubs for the server's lifecycle functions.
//!
//! #### `start_server`
//! 
//! pub fn start_server(cfg: &AppSettings) -> anyhow::Result<()>
//! 
//! Starts the server.
//!
//! #### `stop_server`
//! 
//! pub fn stop_server()
//! 
//! Stops the server.
//!
//! #### `is_running`
//! 
//! pub fn is_running() -> bool
//! 
//! Checks if the server is running.
//!
//! ### `grpc`
//! A module to manage the gRPC server lifecycle, operating similar to the server module. Behavior
//! is also feature-gated based on the `api` flag:
//!
//! - **With `api` feature**:
//!   Full gRPC server management is provided.
//!
//! - **Without `api` feature**:
//!   Provides no-op stubs for gRPC server functions.
//!
//! #### `start_grpc_server`
//! 
//! pub fn start_grpc_server(cfg: &AppSettings) -> anyhow::Result<()>
//! 
//! Starts the gRPC server.
//!
//! #### `stop_grpc_server`
//! 
//! pub fn stop_grpc_server()
//! 
//! Stops the gRPC server.
//!
//! ## Usage
//!
//! 1. **Initialize the Broker**: Use `init_broker` during GUI startup to set up a communication
//!    channel between the GUI and background processes.
//! 2. **Set and Fetch Sender**: Use `set_request_sender` to assign the sender, and
//!    `get_request_sender` to retrieve it when needed.
//!
//! ## Feature Flags
//!
//! The behavior of server and gRPC modules can be controlled using the `api` feature flag:
//! - **Enabled**: Provides full functionality for managing server and gRPC server lifecycles.
//! - **Disabled**: Provides stubbed, no-op implementations to avoid unnecessary dependencies.
use once_cell::sync::OnceCell;
use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender};

use crate::gql::query_interface::QueryOutcome;

// Global sender that Actix handlers use to send requests into the GUI thread
static API_REQ_TX: OnceCell<Sender<ApiRequest>> = OnceCell::new();

#[derive(Debug, Clone)]
pub struct ApiRequest {
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

// Authentication and RBAC module
pub mod auth;

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
