package workflow

import "fmt"

type Node struct {
	StepID    string
	DependsOn []string
}

type DAG struct {
	Nodes map[string]*Node
}

func NewDAG() *DAG {
	return &DAG{
		Nodes: make(map[string]*Node),
	}
}

func (dag *DAG) AddNode(node *Node) error {
	if _, exists := dag.Nodes[node.StepID]; exists {
		return fmt.Errorf("Node with StepID %s already exists", node.StepID)
	}
	dag.Nodes[node.StepID] = node
	return nil
}

func (dag *DAG) GetReadySteps(stepStatuses map[string]StepStatus) ([]string, bool) {
	readySteps := []string{}
	hasFailures := false

	for stepID, node := range dag.Nodes {
		status := stepStatuses[stepID]

		if status == Failed {
			hasFailures = true
			continue
		}

		if status != Pending {
			continue
		}

		ready := true
		for _, dep := range node.DependsOn {
			if stepStatuses[dep] != Success {
				ready = false
				break
			}
		}

		if ready {
			readySteps = append(readySteps, stepID)
		}
	}

	return readySteps, hasFailures
}
