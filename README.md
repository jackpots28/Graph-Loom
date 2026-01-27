# Graph-Loom

Graph-Loom is a lightweight, local-first graph notebook and visualizer built with Rust and egui. It features a physics-assisted layout, an OpenCypher-like query console, and a built-in API server.

![Tooling menus](assets/graph_loom_tooling_dropdowns_snippet.png)

## Key Features

- **Physics-Assisted Layout:** Auto-clustering and level-of-detail (LOD) rendering for smooth interaction even with hundreds of nodes.
- **Query Console:** Supports a pragmatic subset of OpenCypher for creating, matching, and deleting graph elements.
- **Embedded API Service:** Lightweight HTTP, WebSocket, and gRPC APIs for remote interaction and automation.
- **Local-First:** State is saved locally (assets/state.ron), with automatic backups and query logging.
- **Multi-Selection:** Rectangle select for bulk editing node labels and metadata.
- **Headless Mode:** Run as a pure graph database server without the GUI.

## Embedded API & gRPC

Graph-Loom can expose APIs for remote interaction. Enable these in **Settings → Preferences → API Settings**.

- **HTTP/WebSocket:** Default `127.0.0.1:8787`. Supports JSON queries and a WebSocket REPL.
- **gRPC:** Default port `50051`. High-performance interface for programmatic access.
- **Authentication:** Optional API key support for all interfaces.

### Python Client (gRPC)
A Python client example is available in `examples/python_client/`. See the [Python Client README](examples/python_client/README.md) for setup instructions.

### CLI Shell (glsh)
A command-line REPL for the WebSocket API.
```bash
cargo build --features cli --bin glsh
./target/debug/glsh --host 127.0.0.1 --port 8787
```

## Getting Started

### Prerequisites
- [Rust toolchain](https://www.rust-lang.org/tools/install) (stable)

### Build and Run
```bash
cargo run --release
```

### Headless Background Mode
```bash
./target/release/Graph-Loom --background --api-enable
```

## Using the App

- **Panning/Zooming:** Drag the background to pan; scroll to zoom (when cursor is over the canvas).
- **Node/Rel Creation:** Use the left sidebar tools or the Query Console.
- **Layout:** "Auto-cluster" in the sidebar organizes nodes by community detection.
- **Export:** Export matches or the entire graph as JSON/CSV from the sidebar or File menu.

## Query Language (OpenCypher subset)

Supports common patterns like:
```cypher
MATCH (p:Person {name: "Ada"})-[:WORKS_AT]->(c:Company)
RETURN p, c
```
Also supports `CREATE`, `MERGE`, `DELETE`, `SET`, and `REMOVE`. See the in-app help for details.

## License

[Apache 2.0](LICENSE)

