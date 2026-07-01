package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"task-forge/internal/workflow"
	"time"
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
	handler, exists := e.handlers[stepType]
	if !exists {
		errText := fmt.Sprintf("no task handler registered for type %s", stepType)
		step.Error = &errText
		_ = step.TransitionStatus(workflow.Failed)
		return fmt.Errorf("%s", errText)
	}

	output, err := handler(ctx, step.Payload)
	if err != nil {
		errStr := err.Error()
		step.Error = &errStr

		if step.Retries < step.MaxRetries {
			baseDelay := 1 * time.Second
			backoffFactor := math.Pow(2, float64(step.Retries))
			delayDuration := time.Duration(backoffFactor) * baseDelay

			maxDelay := 30 * time.Second
			if delayDuration > maxDelay {
				delayDuration = maxDelay
			}

			step.Retries++

			if transitionErr := step.TransitionStatus(workflow.Failed); transitionErr != nil {
				return fmt.Errorf(
					"execution failed (%s) and failed transition failed: %w",
					errStr,
					transitionErr,
				)
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delayDuration):
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
		return fmt.Errorf("execution succeeded but success transition failed: %w", err)
	}

	return nil
}
