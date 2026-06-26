# AI Document Orchestrator Service

The Orchestrator Service acts as the "Manager" in a microservice architecture designed to process massive, completely unstructured scanned PDF documents using Multimodal LLMs (like Gemini 1.5 Pro or local Ollama Vision models).

This service is explicitly decoupled from the core `ai_service` to adhere to the Single Responsibility Principle.

## 🏗 Architecture & The State Machine

Processing a 100-page scanned PDF in a single LLM call is prone to timeouts and "attention fatigue" (the model forgetting things in the middle of the document). This service solves that using a **Hybrid Async/Sync State Machine**.

1. **Client Submission (Async):** A client submits a large PDF and immediately receives a `JobID` back to prevent HTTP/gRPC timeouts. The client then opens a **gRPC Server Stream** to listen for live progress.
2. **Chunking Engine:** The Orchestrator safely splits the massive PDF into batches of 20 pages using `pdfcpu`.
3. **Parallel Execution (Sync):** The Engine spawns parallel Goroutines. Each Goroutine makes a lightning-fast, blocking (synchronous) gRPC call to the underlying `ai_service` to extract bounding data and classifications.
4. **Idempotency & Resiliency:** All steps strictly check context deadlines. If a single chunk fails, the job fails gracefully, and can be retried safely.

---

## 🚀 Running the Orchestrator

Because this service depends on the `ai_service` protobufs, it must be run within the context of the parent `doc_processor_Service` directory.

### Option 1: Full Docker Cluster (Recommended)

To run the Orchestrator, the AI Service, and NATS JetStream together seamlessly:

```bash
cd /home/ak/projects/doc_processor_Service
docker-compose up -d --build
```

The Orchestrator will be available to clients on **port 50052**.

### Option 2: Running Locally (Development)

Ensure the `ai_service` is already running on port 50051. Then run:

```bash
cd /home/ak/projects/doc_processor_Service/orchestrator_service
export AI_SERVICE_ADDRESS=localhost:50051
go run cmd/server/main.go
```

---

## 🧪 Testing with the Dummy Client

This repository includes a test script that mimics the C# Dotnet Client application. It demonstrates the multi-turn **CLASSIFY -> PARSE** workflow on a single document.

```bash
cd /home/ak/projects/doc_processor_service/orchestrator_service
go run scripts/test_client/main.go /path/to/any/dummy.pdf
```

### Expected Output:
```text
==================================================
🚀 STARTING C#-STYLE WORKFLOW SIMULATION
==================================================

[STEP 1] 🧠 Sending 'dummy.pdf' for CLASSIFICATION...
   -> [Classification] Job job_17000000000 assigned. Waiting for completion...
      - STARTING: Initializing workflow

📋 CLASSIFICATION RESULT: 
[{"document_type": "Legal Disclaimer", ...}]

[STEP 2] ⚙️ C# Client Business Logic triggered...
   - Switch statement activated based on classification
   - Database queried for required schema fields
   - Selected Schema for Parsing: {...}

[STEP 3] 🔍 Sending 'dummy.pdf' for FULL PARSING...
   -> [Parsing] Job job_17000000001 assigned. Waiting for completion...
      - STARTING: Initializing workflow

✅ FINAL PARSED DATA (Ready for C# Persistence): 
[{"BorrowerName": "John Doe", "DocumentState": "NEW YORK"}]
```
