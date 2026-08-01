# AI Document Orchestrator Service

Welcome to the **AI Document Orchestrator Service**, the central "Manager" in the microservice architecture for processing unstructured scanned documents using Multimodal LLMs. It is a 100% application-agnostic, decoupled execution engine operating on a LangGraph-style state machine model.

## Documentation Index

1. **[Core Components](docs/01_Core_Components.md)**  
   Details on the LangGraph state architecture, routing via `OrchestratorPrimitive`, and hardware-aware queue tuning.
2. **[How to Implement](docs/02_How_To_Implement.md)**  
   Instructions for running the orchestrator via Docker or local development, and testing with a dummy client.
3. **[Service Communication](docs/03_Service_Communication.md)**  
   Details on the Context Envelope Pattern, semantic cache integration, and strict No Preemptive Serialization mandates.

---

## High-Level Architecture

```mermaid
flowchart TD
    subgraph Artemis Client Application
        Start([Client Workflow Trigger])
        DB[(Workflow Database)]
        Eval[Evaluate Routing Rules]
        Req[Dispatch Node Request]
        Hook[Webhook Listener]
    end

    subgraph Microservices [Core Microservices]
        API[Orchestrator: gRPC API]
        Engine[Orchestrator: Execution Engine]
        
        Cache[Semantic Cache Service]
        NATS[(NATS JetStream)]
        
        subgraph Workers [Generic Worker Pool]
            AI[AI Service]
            Doc[Document Service]
            Hum[Human Validation]
        end
    end

    %% Client Side
    Start --> DB
    DB --> Eval
    Eval -->|Initiate Node| Req
    Hook -->|Persist State| DB
    Hook -.->|Next Node Loop| Eval

    %% Orchestrator Inbound
    Req -->|gRPC: Execute Node| API
    API --> Engine

    %% Cache Validation
    Engine -->|gRPC: Cache Check| Cache
    Cache -->|Cache Result| Engine

    %% Event Dispatch
    Engine -->|"Publish (TargetWorkerSubject)"| NATS
    
    %% Worker Consumption
    NATS -->|"worker.ai.*"| AI
    NATS -->|"worker.doc.*"| Doc
    NATS -->|"worker.human.*"| Hum

    %% Worker Asynchronous Responses
    AI -->|Async Response| Engine
    Doc -->|Async Response| Engine
    Hum -->|Async Response| Engine

    %% Orchestrator Outbound
    Engine -->|Webhook Callback| Hook
```
