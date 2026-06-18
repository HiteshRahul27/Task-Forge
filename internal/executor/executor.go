package executor

import (
	"context"
	"durable-engine/internal/workflow"
	"encoding/json"
	"fmt"
)

type TaskHandler func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error)

type StepExecutor struct {
	handlers map[string]TaskHandler
}

func NewStepExecutor() *StepExecutor {
	return &StepExecutor{
		handlers: make(map[string]TaskHandler),
	}
}

func (e *StepExecutor) RegisterHandler(name string, handler TaskHandler) {
	e.handlers[name] = handler
}

func (e *StepExecutor) ExecuteStep(ctx context.Context, step *workflow.Step, stepType string) error {

	if err := step.TransitionStatus(workflow.Running); err != nil {
		return fmt.Errorf("failed to start step %s: %w", step.ID, err)
	}

	handler, exists := e.handlers[stepType]
	if !exists {
		errText := fmt.Sprintf("no task handler registered for type %s", stepType)

		step.Error = &errText

		if err := step.TransitionStatus(workflow.Failed); err != nil {
			return fmt.Errorf("handler missing and failed transition failed: %w", err)
		}

		return fmt.Errorf("%s", errText)
	}

	output, err := handler(ctx, step.Payload)

	if err != nil {

		if step.Retries < step.MaxRetries {

			step.Retries++

			errStr := err.Error()
			step.Error = &errStr

			if transitionErr := step.TransitionStatus(workflow.Failed); transitionErr != nil {
				return fmt.Errorf(
					"execution failed (%s) and failed transition failed: %w",
					errStr,
					transitionErr,
				)
			}

			if transitionErr := step.TransitionStatus(workflow.Pending); transitionErr != nil {
				return fmt.Errorf(
					"execution failed (%s) and retry transition failed: %w",
					errStr,
					transitionErr,
				)
			}

			return err
		}

		errStr := err.Error()
		step.Error = &errStr

		if transitionErr := step.TransitionStatus(workflow.Failed); transitionErr != nil {
			return fmt.Errorf(
				"execution failed (%s) and failed transition failed: %w",
				errStr,
				transitionErr,
			)
		}

		return err
	}

	step.Output = output
	step.Error = nil

	if err := step.TransitionStatus(workflow.Success); err != nil {
		return fmt.Errorf(
			"execution succeeded but success transition failed: %w",
			err,
		)
	}

	return nil
}
