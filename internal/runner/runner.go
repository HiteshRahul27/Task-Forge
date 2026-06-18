package runner

import (
	"context"
	"fmt"
	"time"

	"durable-engine/internal/executor"
	"durable-engine/internal/scheduler"
	"durable-engine/internal/workflow"
)

func RunWorkflow(
	ctx context.Context,
	wf *workflow.Workflow,
	exec *executor.StepExecutor,
) error {

	wf.Status = workflow.WorkflowRunning
	wf.UpdatedAt = time.Now()

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

			if allCompleted {
				now := time.Now()
				wf.Status = workflow.Completed
				wf.CompleteAt = &now
				wf.UpdatedAt = now
				return nil
			}

			if hasFailures {
				now := time.Now()
				wf.Status = workflow.WorkflowFailed
				wf.CompleteAt = &now
				wf.UpdatedAt = now
				return fmt.Errorf("workflow failed")
			}

			wf.UpdatedAt = time.Now()
			return fmt.Errorf("workflow is stuck")
		}

		for _, stepID := range readyStepIDs {
			step := wf.Steps[stepID]

			if err := exec.ExecuteStep(ctx, step, step.Type); err != nil {
				continue
			}
		}
	}
}
