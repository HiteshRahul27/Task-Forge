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

func (dag *DAG) ValidateCycles() error {
	inDegree := make(map[string]int)
	adjList := make(map[string][]string)

	for id := range dag.Nodes {
		inDegree[id] = 0
	}

	for id, node := range dag.Nodes {
		for _, dep := range node.DependsOn {
			if _, exists := dag.Nodes[dep]; !exists {
				return fmt.Errorf("step %s depends on a missing step: %s", id, dep)
			}
			adjList[dep] = append(adjList[dep], id)
			inDegree[id]++
		}
	}

	var queue []string
	for id, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, id)
		}
	}

	processedCount := 0
	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		processedCount++

		for _, neighbor := range adjList[curr] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	if processedCount != len(dag.Nodes) {
		return fmt.Errorf("circular dependency detected: your workflow contains a loop")
	}

	return nil
}
