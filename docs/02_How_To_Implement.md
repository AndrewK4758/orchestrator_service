# How to Implement

## 🚀 Running the Orchestrator

Because this service depends on the `ai_service` protobufs, it must be run within the context of the parent `doc_processor_service` directory.

### Docker (All Platforms - Recommended)
To run the Orchestrator, the AI Service, and NATS JetStream together seamlessly:
```bash
cd /home/ak/projects/doc_processor_service
docker-compose up -d --build
```
The Orchestrator will be available to clients on **port 11112**.

### Local Development (Linux / macOS)
Ensure the `ai_service` is already running on port 11111. Then run:
```bash
cd /home/ak/projects/doc_processor_service
cd orchestrator_service
export AI_SERVICE_ADDRESS=localhost:11111
export SEMANTIC_CACHE_SERVICE_ADDRESS=localhost:11115
export DEFAULT_CALLBACK_ADDRESS=localhost:5133
go run cmd/server/main.go
```

> **Note**: To cleanly build and run all ecosystem microservices simultaneously with correct relative paths and multiplexed logs, execute `./scripts/run_all.zsh` from the project root instead.

### Local Development (Windows PowerShell)
Ensure the `ai_service` is already running on port 11111. Then run:
```powershell
cd \home\ak\projects\doc_processor_service\orchestrator_service
$env:AI_SERVICE_ADDRESS="localhost:11111"
$env:SEMANTIC_CACHE_SERVICE_ADDRESS="localhost:11115"
$env:DEFAULT_CALLBACK_ADDRESS="localhost:5133"
go run cmd\server\main.go
```

## 🧪 Testing with the Dummy Client

This repository includes a test script that mimics the C# Dotnet Client application. It demonstrates the multi-node LangGraph execution workflow.

```bash
cd /home/ak/projects/doc_processor_service/orchestrator_service
go run scripts/test_client/main.go
```
