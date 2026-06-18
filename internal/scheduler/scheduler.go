package scheduler

import (
	"durable-engine/internal/workflow"
)

func GetNextReadySteps(wf *workflow.Workflow) ([]string, bool) {
	statuses := make(map[string]workflow.StepStatus)
	for id, step := range wf.Steps {
		statuses[id] = step.Status
	}

	readyStepIDs, hasFailures := wf.DAG.GetReadySteps(statuses)

	return readyStepIDs, hasFailures
}
