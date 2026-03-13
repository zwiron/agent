# agent
Lightweight Go binary that runs inside the customer's network. Connects to Atlas via outbound gRPC, receives job/test commands, executes data transfers using the engine, and streams progress back. Manages its own RSA key pair for envelope encryption. Ships as a Docker image.
