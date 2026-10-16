package validation

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/AndrewK4758/shared_utils/envutil"
	"github.com/nats-io/nats.go/jetstream"
)

// JetStreamPublisher defines the minimal publishing contract for drift telemetry.
type JetStreamPublisher interface {
	Publish(ctx context.Context, subj string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

// DriftTracker tracks rolling validation decisions per tenant and alerts on Tier 3 spikes.
type DriftTracker struct {
	mu                  sync.Mutex
	history             map[string][]int // tenantID -> slice of tier numbers (1, 2, 3)
	windowSize          int
	thresholdPercentage float64
	publisher           JetStreamPublisher
}

// NewDriftTracker constructs a new DriftTracker instance with environment configs.
func NewDriftTracker(pub JetStreamPublisher) *DriftTracker {
	winSize := 100
	if envWin := envutil.GetStringOrDefault("VALIDATION_DRIFT_WINDOW_SIZE", ""); envWin != "" {
		if w, err := strconv.Atoi(envWin); err == nil && w > 0 {
			winSize = w
		}
	}

	thresh := 20.0
	if envThresh := envutil.GetStringOrDefault("VALIDATION_DRIFT_TIER3_THRESHOLD_PERCENT", ""); envThresh != "" {
		if t, err := strconv.ParseFloat(envThresh, 64); err == nil && t > 0 {
			thresh = t
		}
	}

	return &DriftTracker{
		history:             make(map[string][]int),
		windowSize:          winSize,
		thresholdPercentage: thresh,
		publisher:           pub,
	}
}

// RecordAndCheck records a batch of tier results and emits a DECISION_DRIFT event if threshold is breached.
func (dt *DriftTracker) RecordAndCheck(ctx context.Context, tenantID, appID, jobID string, tiers []int) (*DecisionDriftEvent, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenantID cannot be empty")
	}

	dt.mu.Lock()
	defer dt.mu.Unlock()

	current := dt.history[tenantID]
	current = append(current, tiers...)

	if len(current) > dt.windowSize {
		current = current[len(current)-dt.windowSize:]
	}
	dt.history[tenantID] = current

	tier3Count := 0
	for _, t := range current {
		if t == 3 {
			tier3Count++
		}
	}

	tier3Pct := (float64(tier3Count) / float64(len(current))) * 100.0

	if tier3Pct >= dt.thresholdPercentage && len(current) >= min(10, dt.windowSize) {
		event := &DecisionDriftEvent{
			TenantID:            tenantID,
			AppID:               appID,
			JobID:               jobID,
			EventType:           "DECISION_DRIFT",
			WindowSize:          len(current),
			Tier3Count:          tier3Count,
			Tier3Percentage:     tier3Pct,
			ThresholdPercentage: dt.thresholdPercentage,
			Timestamp:           time.Now().UTC(),
		}

		if dt.publisher != nil {
			subject := fmt.Sprintf("trace.orchestrator.%s.%s", tenantID, appID)
			payload, _ := json.Marshal(event)
			_, _ = dt.publisher.Publish(ctx, subject, payload)
		}

		return event, nil
	}

	return nil, nil
}
