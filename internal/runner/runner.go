package runner

import (
	"context"
	"fmt"
	"time"

	"durable-engine/internal/executor"
	"durable-engine/internal/scheduler"
	"durable-engine/internal/storage"
	"durable-engine/internal/workflow"
)

func RunWorkflow(
	ctx context.Context,
	wf *workflow.Workflow,
	exec *executor.StepExecutor,
	store *storage.WorkflowStorage,
) error {

	wf.Status = workflow.WorkflowRunning
	wf.UpdatedAt = time.Now()

	if err := store.SaveWorkflow(ctx, wf); err != nil {
		return fmt.Errorf("failed to save initial workflow state: %w", err)
	}

	for {
		readyStepIDs, hasFailures := scheduler.GetNextReadySteps(wf)

		if len(readyStepIDs) == 0 {
			allCompleted := true

			for _, step := range wf.Steps {
				if step.Status != workflow.Success {
					allCompleted = false
					break
				}
			}

			now := time.Now()
			wf.CompleteAt = &now
			wf.UpdatedAt = now

			if allCompleted {
				wf.Status = workflow.Completed
				_ = store.SaveWorkflow(ctx, wf)
				return nil
			}

			if hasFailures {
				wf.Status = workflow.WorkflowFailed
				_ = store.SaveWorkflow(ctx, wf)
				return fmt.Errorf("workflow failed")
			}

			wf.Status = workflow.WorkflowFailed
			_ = store.SaveWorkflow(ctx, wf)
			return fmt.Errorf("workflow is stuck")
		}

		for _, stepID := range readyStepIDs {
			step := wf.Steps[stepID]

			if err := step.TransitionStatus(workflow.Running); err != nil {
				return fmt.Errorf("failed to transition step to running: %w", err)
			}
			wf.UpdatedAt = time.Now()

			if err := store.SaveWorkflow(ctx, wf); err != nil {
				return fmt.Errorf("failed to save step running state: %w", err)
			}

			err := exec.ExecuteStep(ctx, step, step.Type)

			wf.UpdatedAt = time.Now()
			if saveErr := store.SaveWorkflow(ctx, wf); saveErr != nil {
				return fmt.Errorf("failed to save step post-execution state: %w", saveErr)
			}

			if err != nil {
				continue
			}
		}
	}
}
