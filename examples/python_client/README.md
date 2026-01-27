# Python gRPC Client Example

This example demonstrates how to interface with the Graph-Loom gRPC API using Python.

## Prerequisites

- Python 3.7 or higher
- `pip` (Python package installer)

## Setup

1. **Create and activate a virtual environment (optional but recommended):**

   ```bash
   python3 -m venv client_env
   source client_env/bin/activate  # On Windows: client_env\Scripts\activate
   ```

2. **Install dependencies:**

   ```bash
   pip install grpcio grpcio-tools
   ```

3. **Generate Python gRPC code:**

   Run the following command from this directory. Replace `/Path/To/Graph-Loom` with the actual path to the project root on your machine.

   ```bash
   python3 -m grpc_tools.protoc -I ../../proto --python_out=. --grpc_python_out=. ../../proto/graph_loom.proto
   ```

   *Note: Using `../../proto` assumes you are running this from `examples/python_client/`.*

## Running the Client

1. **Ensure Graph-Loom is running** and the gRPC server is enabled (default port 50051). You can enable it in the application Preferences or via CLI:
   ```bash
   Graph-Loom --grpc-enable
   ```

2. **Run the example script:**

   ```bash
   python3 client.py
   ```

## Example Usage in `client.py`

The `client.py` script demonstrates:
- Connecting to the gRPC server.
- Executing a Cypher-like query.
- Handling the response (nodes, relationships, and metadata).
- Using API Key authentication (commented out by default).

To use authentication, uncomment the `metadata` lines in `client.py` and provide your API key.