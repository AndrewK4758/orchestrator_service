# Core Components

The AI Document Orchestrator Service acts as the "Manager" in a microservice architecture designed to process massive, completely unstructured scanned PDF documents using Multimodal LLMs. It is explicitly decoupled from the core `ai_service`.

## 🏗 The LangGraph State Architecture

The Orchestrator acts strictly as a "brawn" execution engine and defers all business logic back to the client apps. It is **100% application-agnostic**, has ZERO hardcoded client logic, and operates purely on arbitrary JSON shapes within a LangGraph-style state machine model.

1. **Client Node Sequencing:** Client applications define workflows. Orchestrator acts on generic state.
2. **ExecuteWorkflowNode (gRPC):** Client submits an `ExecuteWorkflowNodeRequest` with the current JSON `GlobalState` and instructions.
3. **Internal Execution (ReAct):** The Orchestrator's internal engine evaluates instructions and executes primitives (`PRIMITIVE_AI_STEP`, `PRIMITIVE_HUMAN_STEP`, `PRIMITIVE_ZERO_SHOT_CLASSIFICATION`, `PRIMITIVE_SEMANTIC_CACHE_STORE`).
4. **Webhook Callback:** Once the node completes, the Orchestrator fires a webhook back to the Client containing the dynamically mutated `GlobalState`.

## Routing via OrchestratorPrimitive
The `NodeConfig.PrimitiveType` enum explicitly routes the Orchestrator's execution engine to specific downstream services:
- **`PRIMITIVE_AI_STEP`** (AI Data Extraction): Triggers `ai_service` via gRPC.
- **`PRIMITIVE_ZERO_SHOT_CLASSIFICATION`** (Semantic Cache Lookup): Connects synchronously to `semantic_cache_service`.
- **`PRIMITIVE_SEMANTIC_CACHE_STORE`** (Semantic Cache Write): Fires an asynchronous event via NATS JetStream to `CACHE_REQUESTS`.
- **`PRIMITIVE_HUMAN_STEP`** (Human In The Loop): Connects to `human_validation_service` using the `active_jobs` NATS JetStream KV store.

## ⚡ Queue Tuning & Concurrency (Hardware-Aware)

The Orchestrator uses NATS JetStream Backpressure to throttle throughput. Configure via `.env`:
* **`MAX_ACK_PENDING`**: Maximum concurrent jobs (default 5, perfectly tuned for 12GB VRAM AI Engine).
* **`WORKER_RESPONSE_TIMEOUT_SEC`**: Maximum wait for a worker response (default 300s).
