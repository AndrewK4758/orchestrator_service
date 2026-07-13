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

## 🔗 App-Agnostic Webhooks (Callback Client)

The Orchestrator acts strictly as a "brawn" execution engine and defers business logic back to the client apps. It is **100% application-agnostic** and can serve multiple different tenant apps simultaneously.

When submitting a document with `Action = "CLASSIFY"`, clients can optionally pass a `callback_address` (or rely on the `DEFAULT_CALLBACK_ADDRESS` environment variable). 
If provided, the Orchestrator will:
1. Dynamically connect back to the specified client's `SchemaRegistryService` via gRPC.
2. Fetch the specific parsing schema and rules for the classified document type.
3. Automatically execute the heavy `PARSE` stage using those dynamic rules without requiring the client to send the PDF again.

## 💾 Semantic Cache Integration
The Orchestrator natively connects to the `semantic_cache_service` via gRPC. This allows the external client to sequence generic Cache lookups and storage steps natively within its LangGraph workflows by evaluating the `PRIMITIVE_SEMANTIC_CACHE_CHECK` and `PRIMITIVE_SEMANTIC_CACHE_STORE` execution handlers.

This allows massive PDFs to be processed in a single multi-step workflow without bouncing the heavy payload back and forth across the network.

---

## ⚡ Queue Tuning & Concurrency (Hardware-Aware)

The Orchestrator uses native **NATS JetStream Backpressure** to strictly throttle throughput based on your hardware capabilities (especially when running local AI like Ollama on a single GPU).

You can tune the throughput via the `.env` file:
* **`MAX_ACK_PENDING`**: This strictly mathematically limits the maximum number of concurrent jobs the Orchestrator is allowed to pull from NATS.
  * **Default (5)**: Perfectly tuned for an Ollama node with exactly 12GB VRAM (`OLLAMA_NUM_PARALLEL=4`). This prevents the GPU from being starved or OOM crashing, allowing NATS to queue the remaining backlog gracefully.
  * **High-Throughput (1000+)**: If you switch to an enterprise cloud AI (like Gemini 1.5 via API), increase this limit to scale Go's highly-concurrent network I/O up to process thousands of simultaneous tasks.

The Orchestrator also utilizes dynamic `msg.InProgress()` heartbeats, allowing lightning-fast crash recovery (`AckWait` = 2 minutes) without prematurely terminating long-running GPU inference tasks!

---

## 🚀 Running the Orchestrator

Because this service depends on the `ai_service` protobufs, it must be run within the context of the parent `doc_processor_service` directory.

### Docker (All Platforms - Recommended)
To run the Orchestrator, the AI Service, and NATS JetStream together seamlessly:
```bash
cd /home/ak/projects/doc_processor_service
docker-compose up -d --build
```
The Orchestrator will be available to clients on **port 50052**.

### Local Development (Linux / macOS)
Ensure the `ai_service` is already running on port 50051. Then run:
```bash
cd /home/ak/projects/doc_processor_service/orchestrator_service
export AI_SERVICE_ADDRESS=localhost:50051
export SEMANTIC_CACHE_SERVICE_ADDRESS=localhost:50055
export DEFAULT_CALLBACK_ADDRESS=localhost:5133
go run cmd/server/main.go
```

### Local Development (Windows PowerShell)
Ensure the `ai_service` is already running on port 50051. Then run:
```powershell
cd \home\ak\projects\doc_processor_service\orchestrator_service
$env:AI_SERVICE_ADDRESS="localhost:50051"
$env:SEMANTIC_CACHE_SERVICE_ADDRESS="localhost:50055"
$env:DEFAULT_CALLBACK_ADDRESS="localhost:5133"
go run cmd\server\main.go
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
