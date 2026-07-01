package test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"task-forge/internal/executor"
	"task-forge/internal/runner"
	"task-forge/internal/workflow"
)

func TestWorkflowRetry(t *testing.T) {

	wf := workflow.NewWorkflow("wf-1", "test-workflow-retry")

	stepA := &workflow.Step{
		ID:         "A",
		Type:       "retryable",
		Status:     workflow.Pending,
		MaxRetries: 2,
	}

	if err := wf.AddStep(stepA, []string{}); err != nil {
		t.Fatalf("Failed to add step A: %v", err)
	}

	execInstance := executor.NewStepExecutor()

	attempt := 0

	retryableHandler := func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {

		attempt++

		if attempt <= 2 {
			return nil, fmt.Errorf("simulated step failure -- boom")
		}

		return []byte(`{"result":"success"}`), nil
	}

	execInstance.RegisterHandler("retryable", retryableHandler)

	err := runner.RunWorkflow(
		context.Background(),
		wf,
		execInstance,
		nil,
	)

	if err != nil {
		t.Fatalf(
			"Expected workflow to succeed after retries, but got error: %v",
			err,
		)
	}

	if stepA.Status != workflow.Success {
		t.Errorf(
			"Expected step A to be SUCCESS, got %s",
			stepA.Status,
		)
	}

	if wf.Status != workflow.Completed {
		t.Errorf(
			"Expected workflow to be COMPLETED, got %s",
			wf.Status,
		)
	}

	if stepA.Error != nil {
		t.Errorf(
			"Expected step error to be nil, got %s",
			*stepA.Error,
		)
	}

	t.Logf(
		"Final State -> status=%s retries=%d workflow=%s",
		stepA.Status,
		stepA.Retries,
		wf.Status,
	)
}
