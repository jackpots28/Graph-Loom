//! This module serves as the main entry point for the library, organizing essential components related 
//! to graphs, algorithms, and temporal data. It includes the following submodules:
//!
//! - `graph`: Provides functionality for creating and managing graphs, defining nodes, edges, and their relationships.
//! - `algorithms`: Contains a suite of algorithms that can be applied to graph structures, such as 
//!   shortest path computations, traversals, and optimizations.
//! - `temporal`: Focuses on handling temporal data or time-based graph computations, such as 
//!   time-annotated edges or temporal pathfinding.
pub mod graph;
pub mod algorithms;
pub mod temporal;
