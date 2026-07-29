# Anti-Regression Feedback

The recent changes to `statemachine.go` contain severe regressions and violations of the project's strict architecture rules (`GEMINI.md`).

## 1. Validation AFTER Execution (Logical Flaw)
In `executeGenericStep`, the validation for `req.Identity` is performed **after** the message has already been published to the JetStream worker queue:
```go
	maxRetries := e.publishMaxRetries
	for i := range maxRetries {
		_, err = e.js.PublishMsg(ctx, nMsg, jetstream.WithMsgID(msgID))
        // ...
	}

	if req.Identity == nil || req.Identity.TenantId == "" || req.Identity.AppId == "" || req.Identity.JobId == "" {
		return nil, fmt.Errorf("infrastructure identity is strictly required")
	}
```
**Impact:** If a job lacks identity, it is successfully sent to the worker, but the Orchestrator throws an error immediately after. The worker will execute the job, reply to a corrupted JetStream subject (since `req.Identity.GetJobId()` will return `""`), and the state will be permanently orphaned.
**Fix:** Move the identity validation to the absolute top of the function before any subjects are constructed or messages published.

## 2. Fallbacks & Poison Pill Mishandling (Rule Violation)
In `ProcessJetStreamMessage`, there is a direct violation of the "Fail Fast & No Fallbacks" rule:
```go
	if err := proto.Unmarshal(msg.Data(), &resp); err != nil {
		rawJson = jsonclean.CleanJSON(string(msg.Data()))
	} else {
```
**Impact:** If a worker sends a malformed protobuf payload (a poison pill) or a random string, the Orchestrator suppresses the error and blindly falls back to treating the byte array as a raw JSON string. If this string fails JSON schema validation, it triggers the ReAct retry loop, endlessly re-submitting invalid data to the worker. 
**Fix:** Do not fallback to string casting. If `proto.Unmarshal` fails, instantly fail the job by creating an `ErrorContract` and routing it to the client via Webhook, then `msg.Ack()`.

## 3. Leakage of Domain Logic (Architectural Regression)
When JSON validation fails in `ProcessJetStreamMessage`, the ReAct loop retries inference by calling:
```go
	_ = e.pub.PublishAction(context.Background(), req)
```
**Impact:** `TracePublisher` is meant for telemetry and webhook publishing. Outsourcing JetStream worker queue routing (`PublishAction`) to a telemetry event publisher interface breaks the generic routing paradigm. The `Engine` itself should be responsible for re-queueing the message to the worker.
**Fix:** Remove `PublishAction` from the `TracePublisher` interface. Have `ProcessJetStreamMessage` invoke a local method on `Engine` to re-enqueue the message to the JetStream worker subject.

## 4. Environment Variables (Rule Violation)
The prompt required that environment variables "strictly panic on missing values instead of falling back to defaults". Currently, `NewEngine` returns `fmt.Errorf`.
**Fix:** Use `panic()` natively to ensure the service immediately crashes on startup if `NATS_URL`, `MAX_INFERENCE_RETRIES`, or `PUBLISH_MAX_RETRIES` are missing.

Please implement these corrections immediately.
