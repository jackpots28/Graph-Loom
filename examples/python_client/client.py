import grpc
import graph_loom_pb2
import graph_loom_pb2_grpc


def run():
    # Configure connection (default gRPC port is 50051)
    channel = grpc.insecure_channel("localhost:50051")
    stub = graph_loom_pb2_grpc.GraphQueryStub(channel)

    # Prepare the query
    query = "CREATE (TEST:Note:URL {name: 'github'});"
    params = {}  # Optional: parameters for $param syntax

    request = graph_loom_pb2.QueryRequest(query=query, params=params, log=True)

    # Set up metadata for API Key authentication if enabled
    # The header key must be 'x-api-key'
    # metadata = [('x-api-key', 'your_secret_key_here')]

    try:
        # Execute the call
        response = stub.Execute(request)

        # Execute with api-key
        # response = stub.Execute(request, metadata=metadata)

        if response.error:
            print(f"Server Error: {response.error}")
            return

        print(f"Affected Nodes: {response.affected_nodes}")
        print(f"Affected Relationships: {response.affected_relationships}")
        print(f"Mutated: {response.mutated}")
        print("\nResults:")

        for row in response.rows:
            if row.HasField("node"):
                n = row.node
                print(f"[Node] ID: {n.id}, Label: {n.label}, Meta: {dict(n.metadata)}")
            elif row.HasField("relationship"):
                r = row.relationship
                print(
                    f"[Rel] ID: {r.id}, {r.from_id} -> {r.to_id}, Label: {r.label}, Meta: {dict(r.metadata)}"
                )
            elif row.HasField("info"):
                print(f"[Info] {row.info}")

    except grpc.RpcError as e:
        print(f"RPC failed: {e.code()} - {e.details()}")


if __name__ == "__main__":
    run()