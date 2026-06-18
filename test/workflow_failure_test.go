package test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"durable-engine/internal/executor"
	"durable-engine/internal/runner"
	"durable-engine/internal/workflow"
)

func TestWorkflowFailure(t *testing.T) {
	wf := workflow.NewWorkflow("wf-2", "test-workflow-failure")

	stepA := &workflow.Step{
		ID:     "A",
		Type:   "failing",
		Status: workflow.Pending,
	}

	stepB := &workflow.Step{
		ID:     "B",
		Type:   "dummy",
		Status: workflow.Pending,
	}

	if err := wf.AddStep(stepA, []string{}); err != nil {
		t.Fatalf("Failed to add step A: %v", err)
	}

	if err := wf.AddStep(stepB, []string{"A"}); err != nil {
		t.Fatalf("Failed to add step B: %v", err)
	}

	execInstance := executor.NewStepExecutor()

	failingHandler := func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
		return nil, fmt.Errorf("simulated step failure -- boom")
	}

	execInstance.RegisterHandler("failing", failingHandler)

	dummyHandler := func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
		return []byte(`{"result": "success"}`), nil
	}

	execInstance.RegisterHandler("dummy", dummyHandler)

	err := runner.RunWorkflow(context.Background(), wf, execInstance)
	if err == nil {
		t.Fatalf("Expected to fail running workflow, but got no error")
	}

	if stepA.Status != workflow.Failed {
		t.Errorf("Expected step A to be failed,but got %s", stepA.Status)
	}

	if stepB.Status != workflow.Pending {
		t.Errorf("Expected step B to be pending,but got %s", stepB.Status)
	}

	if wf.Status != workflow.WorkflowFailed {
		t.Errorf("Expected workflow to be failed, but got %s", wf.Status)
	}
}
