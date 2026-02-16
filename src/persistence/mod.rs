//! The module `persist` provides functionality for persisting data and managing state over time.
//! It contains tools and utilities to save, load, and handle persistent storage efficiently.
//!
//! The module `settings` handles application-level configuration settings.
//! It provides features to manage and store configuration values such as user preferences,
//! environment settings, and other metadata required for the application.
//!
//! The module `sqlite_backend` contains implementations and utilities specifically for SQLite-based storage solutions.
//! It deals with database connections, queries, and schema management for applications that use SQLite as their backend database.
pub mod persist;
pub mod settings;
pub mod sqlite_backend;