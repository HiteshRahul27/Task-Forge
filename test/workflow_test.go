package test

import (
	"context"
	"encoding/json"
	"testing"

	"durable-engine/internal/executor"
	"durable-engine/internal/runner"
	"durable-engine/internal/workflow"
)

func TestFullWorkflowEngine(t *testing.T) {
	ctx := context.Background()

	wf := workflow.NewWorkflow("wf-1", "test-workflow")

	stepA := &workflow.Step{
		ID:     "A",
		Type:   "dummy",
		Status: workflow.Pending,
	}

	stepB := &workflow.Step{
		ID:     "B",
		Type:   "dummy",
		Status: workflow.Pending,
	}

	stepC := &workflow.Step{
		ID:     "C",
		Type:   "dummy",
		Status: workflow.Pending,
	}

	if err := wf.AddStep(stepA, []string{}); err != nil {
		t.Fatalf("Failed to add step A: %v", err)
	}

	if err := wf.AddStep(stepB, []string{"A"}); err != nil {
		t.Fatalf("Failed to add step B: %v", err)
	}

	if err := wf.AddStep(stepC, []string{"B"}); err != nil {
		t.Fatalf("Failed to add step C: %v", err)
	}

	execInstance := executor.NewStepExecutor()

	dummyHandler := func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
		return []byte(`{"result": "success"}`), nil
	}

	execInstance.RegisterHandler("dummy", dummyHandler)

	err := runner.RunWorkflow(ctx, wf, execInstance, nil)
	if err != nil {
		t.Fatalf("Workflow execution completely broke: %v", err)
	}

	if wf.Status != workflow.Completed {
		t.Errorf("Expected workflow to be COMPLETED, but got: %s", wf.Status)
	}

	if stepA.Status != workflow.Success {
		t.Errorf("Expected step A to be SUCCESS, got: %s", stepA.Status)
	}

	if stepB.Status != workflow.Success {
		t.Errorf("Expected step B to be SUCCESS, got: %s", stepB.Status)
	}

	if stepC.Status != workflow.Success {
		t.Errorf("Expected step C to be SUCCESS, got: %s", stepC.Status)
	}

	if wf.CompleteAt == nil {
		t.Errorf("Expected workflow completion timestamp to be set, but got nil")
	}
}
