# AI Document Orchestrator Service

The Orchestrator Service acts as the "Manager" in a microservice architecture designed to process massive, completely unstructured scanned PDF documents using Multimodal LLMs (like Gemini 1.5 Pro or local OpenAI-compatible Vision models like Ollama/vLLM).

This service is explicitly decoupled from the core `ai_service` to adhere to the Single Responsibility Principle.

## 🏗 The LangGraph State Architecture

The Orchestrator acts strictly as a "brawn" execution engine and defers all business logic back to the client apps. It is **100% application-agnostic** and operates on a pure LangGraph-style state machine model.

1. **Client Node Sequencing:** Client applications (e.g., Artemis) define their workflows in their own SQL databases. The Orchestrator does not know the graph up front.
2. **ExecuteWorkflowNode (gRPC):** The Client submits a single `ExecuteWorkflowNodeRequest` containing the current JSON `GlobalState` and the specific instructions for the current node (Step).
3. **Internal Execution (ReAct):** The Orchestrator's internal engine (Planner & Reviewer) evaluates the node instructions and executes internal primitives (e.g., `PRIMITIVE_AI_STEP`, `PRIMITIVE_HUMAN_STEP`, `PRIMITIVE_ZERO_SHOT_CLASSIFICATION`).
4. **Webhook Callback:** Once the node is completed, the Orchestrator fires a webhook back to the Client containing the dynamically mutated `GlobalState`. The Client evaluates the output against its dynamic rules and submits the next Node in the graph.

## 🛠 Using the ExecuteWorkflowNode Interface

The `ExecuteWorkflowNodeRequest` is the single entry point to the Orchestrator. The most important fields are:
- `JobId` (string): Unique identifier for the job (e.g., `AppId_UnixNano`).
- `StepId` (string): The ID of the specific node/step currently being executed.
- `GlobalState` (google.protobuf.Struct): The complete JSON state object built up by prior steps.
- `NodeConfig` (NodeConfiguration): The instructions that dictate what the Orchestrator should do on this specific step.

### Routing via OrchestratorPrimitive
The `NodeConfig.PrimitiveType` enum explicitly routes the Orchestrator's execution engine to specific downstream services:

1. **`PRIMITIVE_AI_STEP` (AI Data Extraction)**
   - **Triggers**: Connects to the `ai_service` (Processor) via gRPC.
   - **Usage**: Used for heavy LLM data extraction and validation. Provide `SystemInstructions` and an `ExpectedSchema` (JSON Schema) inside `NodeConfig`. The LLM's structured output will be dynamically merged back into `GlobalState`.

2. **`PRIMITIVE_ZERO_SHOT_CLASSIFICATION` (Semantic Cache Lookup)**
   - **Triggers**: Connects synchronously to the `semantic_cache_service`.
   - **Usage**: Use this to quickly query the vector database before falling back to heavy AI extraction. The Orchestrator hashes the input from `NodeConfig.CacheKeyInput` against the existing embeddings. On a cache hit, it instantly returns the cached JSON; on a miss, it can dynamically trigger an AI fallback or notify the client.

3. **`PRIMITIVE_SEMANTIC_CACHE_STORE` (Semantic Cache Write)**
   - **Triggers**: Fires an asynchronous, durable event to the `CACHE_REQUESTS` stream via NATS JetStream.
   - **Usage**: Used at the end of an AI extraction step to persist the successful JSON results into the vector database (Qdrant). Does not block client response times.

4. **`PRIMITIVE_HUMAN_STEP` (Human In The Loop)**
   - **Triggers**: Connects to the `human_validation_service`.
   - **Usage**: Used when the AI lacks confidence or encounters a fatal schema exception. The Orchestrator suspends the graph execution and queues a task for human operators. Once the human manually corrects the JSON state, the Orchestrator resumes the graph by sending the callback to the client.

## ✉️ The Standard Callback Envelope Pattern

To provide seamless, strongly-typed parsing inside the `.NET Client` (without resorting to fragile dynamic dictionary lookups), the Orchestrator strictly enforces the **Context Envelope Pattern** on the final mutated `GlobalState` before firing the webhook callback.

Whenever a workflow step succeeds, the Orchestrator will automatically create (or augment) a top-level `"Context"` key inside the `GlobalState` JSON and dynamically inject the `Metadata` map originally provided in the `ExecuteWorkflowNodeRequest`, alongside the `JobId` and `CompletedStepId`.

This guarantees that the C# client can utilize a generic utility wrapper:
```csharp
public class StepCallbackEnvelope<TState, TOutput> {
    public StepContext Context { get; set; }
    public TState Data { get; set; } // The heavily mutated business logic
}
```
This entirely removes hardcoded conditional logic and allows the `.NET` application to cleanly deserialize `google.protobuf.Struct` into `TState`!

---

## 💾 Semantic Cache Integration
The Orchestrator interacts with the `semantic_cache_service` using a **Hybrid Architecture** for maximum resilience and throughput. The Orchestrator acts strictly as a "dumb pipe"; the `.NET Client` has absolute authority over Cache Confidence levels via the `ConfidenceThreshold` passed in the execution request.

1. **Reads (gRPC)**: Cache lookups are evaluated synchronously via gRPC under the `PRIMITIVE_ZERO_SHOT_CLASSIFICATION` execution handler. The cache service uses a two-tier lookup (Metadata Hash -> Semantic Search). The client-provided `ConfidenceThreshold` dictates the success criteria. If a document chunk matches a prior index, the Orchestrator instantly terminates the AI step and merges the cached JSON payload directly into the client's global state.
2. **Writes (JetStream)**: Successful AI data extractions are sequenced dynamically via the `PRIMITIVE_SEMANTIC_CACHE_STORE` execution handler. The Orchestrator fires an asynchronous, durable event to the `CACHE_REQUESTS` stream via NATS JetStream and immediately continues processing without waiting for the vector DB to index the payload, completely removing latency bottlenecks and stampedes.
3. **Observability**: Cache operations feature a dedicated `CACHE` log level. The Orchestrator prints the exact client metadata used for partitioning, the resulting cosine similarity score, and the explicitly extracted cache payload directly to the console for complete visibility.

---

## ⚡ Queue Tuning & Concurrency (Hardware-Aware)

The Orchestrator uses native **NATS JetStream Backpressure** to strictly throttle throughput based on your hardware capabilities (especially when running local AI engines like Ollama/vLLM on a single GPU).

You can tune the throughput via the `.env` file:
* **`MAX_ACK_PENDING`**: This strictly mathematically limits the maximum number of concurrent jobs the Orchestrator is allowed to pull from NATS.
  * **Default (5)**: Perfectly tuned for an AI Engine (e.g., Ollama/vLLM) node with exactly 12GB VRAM. This prevents the GPU from being starved or OOM crashing, allowing NATS to queue the remaining backlog gracefully.
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

This repository includes a test script that mimics the C# Dotnet Client application. It demonstrates the multi-node LangGraph execution workflow.

```bash
cd /home/ak/projects/doc_processor_service/orchestrator_service
go run scripts/test_client/main.go
```

### Expected Output:
```text
==================================================
🚀 STARTING C#-STYLE WORKFLOW SIMULATION (LangGraph Mode)
==================================================

[NODE 1] 🧠 Submitting ExecuteWorkflowNodeRequest (Node: AI Data Extraction)...
   -> [Orchestrator] Executing primitive PRIMITIVE_AI_STEP...
   -> [Orchestrator] State mutated successfully. Firing webhook.

[WEBHOOK] ⚙️ C# Client received mutated GlobalState...
   - Evaluating dynamic rules on Node 1 output...
   - Determining next Node...

[NODE 2] 🔍 Submitting ExecuteWorkflowNodeRequest (Node: Human Review)...
   -> [Orchestrator] Executing primitive PRIMITIVE_HUMAN_STEP...
   -> [Orchestrator] State mutated successfully. Firing webhook.

✅ FINAL GLOBAL STATE (Ready for persistence): 
{
  "extracted_data": {
     "BorrowerName": "John Doe", 
     "DocumentState": "NEW YORK"
  },
  "review_status": "APPROVED"
}
```
