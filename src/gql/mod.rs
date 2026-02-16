/*!
This module defines the structure and implementation of two key submodules
used in the system: `query_interface` and `cypher_spec`. These submodules
are designed to handle query logic and Cypher query specifications,
respectively.

# Modules

- `query_interface`: Contains functionality for interacting with and
  querying a database or data source. This module is responsible for
  facilitating the execution of queries and returning results to the caller.
  It may also include abstractions for different query execution engines.
- `cypher_spec`: Defines the Cypher query language specification and related
  utilities. This includes the parsing, construction, and validation of
  Cypher queries. It acts as the backbone for generating syntactically
  correct queries that are compatible with graph databases.

# Usage

The parent module acts as a container for these submodules, facilitating
their integration into the broader application.

# Notes

Make sure to explore the documentation of the individual submodules for
specific details about their functionality.
*/
pub mod query_interface;
pub mod cypher_spec;