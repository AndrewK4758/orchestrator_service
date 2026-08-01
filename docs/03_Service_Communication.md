# Service Communication

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

## 💾 Semantic Cache Integration
The Orchestrator interacts with the `semantic_cache_service` using a **Hybrid Architecture**:
1. **Reads (gRPC)**: Synchronous evaluation. If matched, instantly terminates AI step.
2. **Writes (JetStream)**: Asynchronous AI extraction success persistence.
3. **Observability**: Dedicated `CACHE` log level for visibility.

## 📜 Data Contracts & No Preemptive Serialization

This service adheres to the strict **No Preemptive Serialization** mandate. 
- All JSON-based structural state must be parsed natively using `google.protobuf.Struct` (or `structpb.Struct` in Go, `Dictionary<string, object>` in C#).
- Preemptive stringification (e.g. converting nested objects to strings before the final persistence layer) is forbidden to prevent schema drift, whitespace corruption, and nested parsing bugs.
- Boundary interfaces rely on typed object mappings rather than dynamic regex or fuzzy key lookups.
