# Rules For This Project
## Always
- use the latest golang syntax and features. 
- make the best architectural decision for the task
- Ask me if you are unsure of the direction to explore and proceed
- Only make changes once 100% sure
- Use zsh as the first shell, if not available fallback to bash
- Keep GRPC schemas up to date across all services that use them
- Prefer GRPC over REST for all service to service communication
- Use the latest version of the language and packages
- All new features must not break existing functionality, and new changes must be implemented throughout all services that changes affect.
- Keep the README in sync with the code in all services with full docker, local, windows and linux instructions.
- Keep all test cases up to date with the latest changes.
- Keep all projects in sync with the latest changes.
- Keep all documentation up to date with the latest changes.

## Never 
- make changes you are not sure is the best practice
- guess what my meaning is
- go outside the bounds of the prompt
- change anything not pertinent to the task
- over comment code
- provide a change without a senior level engineer explanation

## Error Handling & Resiliency
- Handle errors using best practices for resiliency (retries, graceful degradation).
- Ensure high visibility (rich context logging, wrapped errors using `fmt.Errorf`).
- Design for idempotency (ability to continue where left off/recover state).
- All errors MUST be caught, handled, and clearly communicated back to the client.

## LangGraph State Architecture
- Model multi-agent workflows after LangGraph: The `.NET Client` must be the authority that defines and controls the structural `State` (as strongly-typed objects) passed between agents.
- The Orchestrator (Go) should act purely as a robust communication and structural execution layer, accepting a client-defined state schema, passing the entire current state context to the LLM nodes, and continuously merging the LLM's structured output back into the global State object.
- Never rely on implicit or unstructured string-passing between steps; always use formal, JSON-schema validated State objects where the output of the previous node explicitly mutates the state for the input of the next node.
- **Zero Hardcoded Client Logic in Backend/Orchestrator:** The Go Orchestrator must be completely generic and dumb. It must NEVER contain client-specific key checks, document classification branching, or hardcoded behavior tied to specific step names or state keys (such as checking for `ClassificationInfo` or specific `Classification` keys).
- **Client-Controlled Decision Making:** All workflow routing, decision branching, and schema interpretation reside strictly in the client (e.g., Artemis C# Client). The Orchestrator simply executes generic steps (e.g., `AI_EXTRACTION` or `HUMAN_VALIDATION`) and passes state updates back to the client.
- **Generic State & Metadata Synchronization:** Any compliance metadata update or state synchronization (like updating `DocumentType`, `TenantID`, `UserID`, `AppID`, `CorrelationID`) must be handled dynamically and generically from the client's state or request parameters (such as using generic, case-insensitive mapping helpers on the merged global state), never via hardcoded conditional logic inside the execution loops.

## AI Behavior & Prompting Rules
- This application is a strictly-controlled business tool where precise, repeatable, and deterministic results are the primary constraint. 
- ALWAYS configure prompts and AI instructions with 0 creativity. 
- DO NOT allow the AI to hallucinate, guess, or invent data outside of the explicit bounds of the provided schemas and guidelines.
- NEVER include app-specific rules or creative interpretations in base system prompts; simply instruct the AI to execute exactly what it is told to do based on the runtime constraints.
## Dynamic DB-Driven Workflow & ReAct Engine
- **Job Identifiers:** All jobs in the orchestrator are tracked natively using the format `[AppId]_[UnixNano]`. The client applications MUST supply their `AppId` in the `SubmitDocumentRequest`.
- **Client DB Definitions:** Client applications (e.g., Artemis) define workflows in SQL tables (`JobDefinition`, `StepDefinition`). The client initiates the first step to the Orchestrator, and upon receiving the webhook callback, reads the `ActiveJobExecution` table to sequence the next step to the Orchestrator.
- **Autonomous Primitive Actions:** The client does NOT pass hardcoded Action Types (like "AI_EXTRACTION") to the Orchestrator. The client simply passes `CustomInstructions` and an `ExpectedSchema`.
- **Backend Planner:** The Orchestrator leverages a fast internal AI Planner (ReAct engine) to evaluate the client's step and autonomously route it to the correct internal primitive handler (e.g., `PRIMITIVE_AI_STEP` or `PRIMITIVE_HUMAN_STEP`).
- **Agnostic Services:** The backend services contain absolutely zero app-specific strings, hardcoded logic, or fallback rules. Every fallback, routing decision, and retry is controlled by the external client.
