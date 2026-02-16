//! This module serves as the entry point for several submodules that provide
//! functionality across different domains of the application. Below is an overview
//! of each submodule:
//!
//! - `graph_utils`: Contains utilities and functions for handling graph-related
//!   operations, such as graph traversal, manipulation, and representation.
//!
//! - `gui`: Provides the components and logic necessary for building and
//!   managing the graphical user interface of the application.
//!
//! - `persistence`: Handles data persistence functionality, such as saving and
//!   loading data to/from storage, and managing data serialization and deserialization.
//!
//! - `gql`: Implements GraphQL-related operations, including query execution,
//!   schema validation, and GraphQL API integration.
//!
//! - `api`: Includes functionality to expose and consume APIs, serving as an
//!   interface for external systems or clients interacting with the application.
//!
//! - `semantic`: Focuses on semantic processing, such as natural language
//!   understanding, data interpretation, or semantic validation.
//!
//! This module provides a centralized structure to facilitate organization
//! and code reusability across the application.
pub mod graph_utils;
pub mod gui;
pub mod persistence;
pub mod gql;
pub mod api;
pub mod semantic;
