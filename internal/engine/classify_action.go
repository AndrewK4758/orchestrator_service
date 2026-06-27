package engine

import (
	"context"
	"fmt"
	"log"
	"strings"

	"orchestrator_service/internal/ai_client"
	"orchestrator_service/internal/events"
	"orchestrator_service/internal/registry_client"
)

type ClassifyAction struct {
	aiClient       *ai_client.Client
	registryClient *registry_client.Client
	pub            *events.Publisher
}

func NewClassifyAction(aiClient *ai_client.Client, regClient *registry_client.Client, pub *events.Publisher) *ClassifyAction {
	return &ClassifyAction{
		aiClient:       aiClient,
		registryClient: regClient,
		pub:            pub,
	}
}

func (a *ClassifyAction) Name() string {
	return "Classify"
}

func (a *ClassifyAction) Execute(ctx context.Context, state *ProcessingState) error {
	log.Printf("[ClassifyAction] Starting classification for Job: %s", state.JobID)

	// Combine PDFs if there are multiple, or just classify the first one to determine type
	if len(state.RawPDFs) == 0 {
		return fmt.Errorf("no pdf contents provided for classification")
	}

	pdfBytes := state.RawPDFs[0] // Classify based on the first document for now

	// 1. Call AI to Classify
	prompt := "You are a classification AI. Look at this document and classify it. Output ONLY ONE of the following words: CLOSING, TITLE_OPINION, UNKNOWN."

	resp, err := a.aiClient.ExtractChunk(ctx, pdfBytes, prompt, `{"type": "object", "properties": {"classification": {"type": "string"}}}`)
	if err != nil {
		return fmt.Errorf("AI classification failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("AI classification returned error: %s", resp.ErrorMessage)
	}

	log.Printf("[ClassifyAction] AI Raw Response: %s", resp.ExtractedJson)

	// Very simple parsing since the prompt says to return JSON
	// The AI will return {"classification": "CLOSING"}
	classification := "UNKNOWN"
	if strings.Contains(strings.ToUpper(resp.ExtractedJson), "CLOSING") {
		classification = "CLOSING"
	} else if strings.Contains(strings.ToUpper(resp.ExtractedJson), "TITLE_OPINION") {
		classification = "TITLE_OPINION"
	}

	log.Printf("[ClassifyAction] Classified as: %s", classification)

	if classification == "UNKNOWN" {
		// Log and possibly halt or fallback
		log.Printf("Classification UNKNOWN for Job: %s", state.JobID)
		return fmt.Errorf("could not classify document")
	}

	// 2. Fetch Rules and Schema from Artemis Registry
	rulesText, schema, err := a.registryClient.GetRules(ctx, classification)
	if err != nil {
		return fmt.Errorf("failed to get rules from Artemis for %s: %w", classification, err)
	}

	log.Printf("[ClassifyAction] Successfully fetched rules and schema for %s", classification)

	// 3. Update State
	state.DocumentType = classification
	state.CustomInstructions = rulesText
	state.Schema = schema
	
	// Change action to EXTRACT so it can proceed to ParallelExtract if desired by the workflow
	state.Action = "EXTRACT"

	return nil
}
