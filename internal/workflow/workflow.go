package workflow

import (
	"fmt"
	"sync"
	"time"
)

type WorkflowStatus string

const (
	NotStarted      WorkflowStatus = "NOT_STARTED"
	WorkflowRunning WorkflowStatus = "RUNNING"
	Completed       WorkflowStatus = "COMPLETED"
	WorkflowFailed  WorkflowStatus = "FAILED"
)

type Workflow struct {
	ID         string
	Name       string
	Steps      map[string]*Step
	DAG        *DAG
	Status     WorkflowStatus
	mu         sync.Mutex
	CreatedAt  time.Time
	UpdatedAt  time.Time
	CompleteAt *time.Time
}

func NewWorkflow(id, name string) *Workflow {
	return &Workflow{
		ID:         id,
		Name:       name,
		Steps:      make(map[string]*Step),
		DAG:        NewDAG(),
		Status:     NotStarted,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
		CompleteAt: nil,
	}
}

func (wf *Workflow) AddStep(step *Step, dependsOn []string) error {
	wf.mu.Lock()
	defer wf.mu.Unlock()

	if _, exists := wf.Steps[step.ID]; exists {
		return fmt.Errorf("Step with ID %s already exists", step.ID)
	}

	for _, depID := range dependsOn {
		if _, exists := wf.Steps[depID]; !exists {
			return fmt.Errorf("Cannot add step '%s': parent dependency '%s' must be added to the workflow first", step.ID, depID)
		}
	}

	wf.Steps[step.ID] = step

	node := &Node{
		StepID:    step.ID,
		DependsOn: dependsOn,
	}
	return wf.DAG.AddNode(node)
}
