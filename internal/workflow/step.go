package workflow

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type StepStatus string

const (
	Pending StepStatus = "PENDING"
	Running StepStatus = "RUNNING"
	Success StepStatus = "SUCCESS"
	Failed  StepStatus = "FAILED"
)

type Step struct {
	ID            string
	Type          string
	Payload       json.RawMessage
	Output        json.RawMessage
	Error         *string
	Status        StepStatus
	Retries       int
	MaxRetries    int
	ExecutionHash string
	mu            sync.Mutex
	CreatedAt     time.Time
	UpdatedAt     time.Time
	CompleteAt    *time.Time
}

func (step *Step) TransitionStatus(newStatus StepStatus) error {
	step.mu.Lock()
	defer step.mu.Unlock()

	isValidTransition := false
	switch newStatus {
	case Pending:
		isValidTransition = (step.Status == Failed)
		if isValidTransition {
			step.CompleteAt = nil
		}
	case Running:
		isValidTransition = (step.Status == Pending)
	case Success:
		isValidTransition = (step.Status == Running)
	case Failed:
		isValidTransition = (step.Status == Running)
	}

	if !isValidTransition {
		return fmt.Errorf("Invalid transition from %s to %s for %s", step.Status, newStatus, step.ID)
	}
	step.Status = newStatus
	step.UpdatedAt = time.Now()
	if newStatus == Success || newStatus == Failed {
		now := time.Now()
		step.CompleteAt = &now
	}
	return nil
}
