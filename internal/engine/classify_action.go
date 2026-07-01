package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"orchestrator_service/internal/ai_client"
	"orchestrator_service/internal/callback_client"
	"orchestrator_service/internal/events"
)

type ClassifyAction struct {
	aiClient  *ai_client.Client
	pub       *events.Publisher
	cbManager *callback_client.ClientManager
}

func NewClassifyAction(aiClient *ai_client.Client, pub *events.Publisher, cbManager *callback_client.ClientManager) *ClassifyAction {
	return &ClassifyAction{
		aiClient:  aiClient,
		pub:       pub,
		cbManager: cbManager,
	}
}

func (a *ClassifyAction) Name() string {
	return "Classify"
}

func (a *ClassifyAction) Execute(ctx context.Context, state *ProcessingState) error {
	log.Printf("[ClassifyAction] Starting classification for Job: %s", state.JobID)

	// Combine PDFs if there are multiple, or just classify the first one to determine type
	if len(state.RawPDFs) == 0 && len(state.RawTextContents) == 0 {
		return fmt.Errorf("no pdf or text contents provided for classification")
	}

	var pdfBytes []byte
	var textContent string

	if len(state.RawPDFs) > 0 {
		pdfBytes = state.RawPDFs[0] // Classify based on the first document for now
	} else if len(state.RawTextContents) > 0 {
		textContent = state.RawTextContents[0]
	}

	// 1. Call AI to Classify
	prompt := state.CustomInstructions
	if prompt == "" {
		return fmt.Errorf("no prompt provided for classification and no custom instructions available")
	}
	if len(state.AllowedClassifications) > 0 {
		prompt += fmt.Sprintf("\nYou MUST classify it as exactly one of these: %s. If it doesn't match, return 'UNKNOWN'.", strings.Join(state.AllowedClassifications, ", "))
	}

	resp, err := a.aiClient.ExtractChunk(ctx, pdfBytes, textContent, prompt, `{"type": "object", "properties": {"classification": {"type": "string"}}}`)
	if err != nil {
		return fmt.Errorf("AI classification failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("AI classification returned error: %s", resp.ErrorMessage)
	}

	cleanJson := strings.TrimSpace(resp.ExtractedJson)
	if strings.HasPrefix(cleanJson, "```json") {
		cleanJson = strings.TrimPrefix(cleanJson, "```json")
	} else if strings.HasPrefix(cleanJson, "```") {
		cleanJson = strings.TrimPrefix(cleanJson, "```")
	}
	if strings.HasSuffix(cleanJson, "```") {
		cleanJson = strings.TrimSuffix(cleanJson, "```")
	}
	cleanJson = strings.TrimSpace(cleanJson)

	log.Printf("[ClassifyAction] AI Raw Response: %s", cleanJson)

	type ClassifyResult struct {
		Classification string `json:"classification"`
	}
	var result ClassifyResult
	if err := json.Unmarshal([]byte(cleanJson), &result); err != nil {
		log.Printf("Failed to unmarshal AI response: %v", err)
	}

	classification := strings.ToUpper(strings.TrimSpace(result.Classification))
	if classification == "" {
		classification = "UNKNOWN"
	}

	log.Printf("[ClassifyAction] Classified as: %s", classification)

	if classification == "UNKNOWN" {
		// Log and continue, let the client or subsequent steps handle UNKNOWN
		log.Printf("Classification UNKNOWN for Job: %s", state.JobID)
		state.DocumentType = "UNKNOWN"
		// If it's CLASSIFY_ONLY, we just return nil so it can send the final JSON.
		// If it's a full workflow, it will probably fail in the next step (or we can just let it try).
		return nil
	}

	if state.Action == "CLASSIFY_ONLY" {
		log.Printf("[ClassifyAction] Action is CLASSIFY_ONLY, skipping schema lookup for %s", classification)
		state.DocumentType = classification
		return nil
	}

	// 2. Fetch Rules and Schema from generic Callback Server if enabled
	if state.CallbackAddress != "" || a.cbManager != nil {
		rulesText, schema, err := a.cbManager.GetRules(ctx, state.CallbackAddress, classification)
		if err != nil {
			log.Printf("Warning: failed to get rules from callback server for %s: %v", classification, err)
			if state.Schema != "" {
				log.Printf("Falling back to provided schema and instructions for %s", classification)
			} else {
				return fmt.Errorf("failed to get rules from callback server and no fallback schema provided: %w", err)
			}
		} else {
			log.Printf("[ClassifyAction] Successfully fetched rules and schema for %s via callback", classification)
			state.CustomInstructions = rulesText
			state.Schema = schema
		}
	} else if state.Schema == "" {
		return fmt.Errorf("no schema provided to parse classification %s and no callback server available", classification)
	}

	// 3. Update State
	state.DocumentType = classification

	// Change action to EXTRACT so it can proceed to ParallelExtract if desired by the workflow
	state.Action = "EXTRACT"

	return nil
}
