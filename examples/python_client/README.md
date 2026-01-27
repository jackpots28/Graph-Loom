mkdir python_client && cd python_client
python3 -m venv client_env && source client_env/bin/activate
pip install grpcio grpcio-tools
python3 -m grpc_tools.protoc -I /Path/To/Graph-Loom/proto --python_out=. --grpc_python_out=. graph_loom.proto