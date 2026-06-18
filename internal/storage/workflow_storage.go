package storage

import (
	"context"
	"database/sql"
	"durable-engine/internal/workflow"
	"encoding/json"
	"fmt"
	"time"
)

type WorkflowStorage struct {
	db *sql.DB
}

func NewWorkflowStorage(db *sql.DB) *WorkflowStorage {
	return &WorkflowStorage{db: db}
}

func toNullTime(t *time.Time) sql.NullTime {
	if t == nil {
		return sql.NullTime{Valid: false}
	}
	return sql.NullTime{Time: *t, Valid: true}
}

func (s *WorkflowStorage) SaveWorkflow(ctx context.Context, wf *workflow.Workflow) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	workflowQuery := `
		INSERT INTO workflow (id, name, status, created_at, updated_at, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (id) DO UPDATE SET status = $3, updated_at = $5, completed_at = $6;`

	_, err = tx.ExecContext(ctx, workflowQuery,
		wf.ID,
		wf.Name,
		wf.Status,
		wf.CreatedAt,
		wf.UpdatedAt,
		toNullTime(wf.CompleteAt),
	)
	if err != nil {
		return fmt.Errorf("failed to insert workflow: %w", err)
	}

	stepQuery := `
		INSERT INTO steps (
			id, workflow_id, type, status, output, error, payload, retries, max_retries, 
			execution_hash, created_at, updated_at, completed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (id) DO UPDATE SET type = $3, status = $4, output = $5, error = $6, retries = $8, updated_at = $12, completed_at = $13;`

	for _, step := range wf.Steps {
		_, err = tx.ExecContext(ctx, stepQuery,
			step.ID,
			wf.ID,
			step.Type,
			step.Status,
			step.Output,
			step.Error,
			step.Payload,
			step.Retries,
			step.MaxRetries,
			step.ExecutionHash,
			step.CreatedAt,
			step.UpdatedAt,
			toNullTime(step.CompleteAt),
		)
		if err != nil {
			return fmt.Errorf("failed to insert/update step %s: %w", step.ID, err)
		}
	}

	depQuery := `
		INSERT INTO step_dependencies (step_id, depends_on_step_id)
		VALUES ($1, $2)
		ON CONFLICT DO NOTHING;`

	for stepID, node := range wf.DAG.Nodes {
		for _, depID := range node.DependsOn {
			_, err = tx.ExecContext(ctx, depQuery, stepID, depID)
			if err != nil {
				return fmt.Errorf("failed to insert dependency from %s to %s: %w", stepID, depID, err)
			}
		}
	}

	return tx.Commit()
}

func (s *WorkflowStorage) LoadWorkflow(ctx context.Context, workflowID string) (*workflow.Workflow, error) {
	wf := &workflow.Workflow{}

	var wfCompletedAt sql.NullTime
	wfQuery := `SELECT id, name, status, created_at, updated_at, completed_at FROM workflow WHERE id = $1`

	err := s.db.QueryRowContext(ctx, wfQuery, workflowID).Scan(
		&wf.ID, &wf.Name, &wf.Status, &wf.CreatedAt, &wf.UpdatedAt, &wfCompletedAt,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("workflow not found: %s", workflowID)
	} else if err != nil {
		return nil, fmt.Errorf("failed to scan workflow row: %w", err)
	}

	if wfCompletedAt.Valid {
		wf.CompleteAt = &wfCompletedAt.Time
	}

	wf.Steps = make(map[string]*workflow.Step)
	wf.DAG = workflow.NewDAG()

	stepsQuery := `
		SELECT id, type, payload, output, error, status, retries, max_retries, execution_hash, created_at, updated_at, completed_at 
		FROM steps 
		WHERE workflow_id = $1`

	rows, err := s.db.QueryContext(ctx, stepsQuery, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to query workflow steps: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		step := &workflow.Step{}
		var completedAt sql.NullTime
		var rawOutput []byte
		var rawError sql.NullString

		err := rows.Scan(
			&step.ID,
			&step.Type,
			&step.Payload,
			&rawOutput,
			&rawError,
			&step.Status,
			&step.Retries,
			&step.MaxRetries,
			&step.ExecutionHash,
			&step.CreatedAt,
			&step.UpdatedAt,
			&completedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan step row: %w", err)
		}

		if rawOutput != nil {
			step.Output = json.RawMessage(rawOutput)
		}

		if rawError.Valid {
			errStr := rawError.String
			step.Error = &errStr
		}

		if completedAt.Valid {
			t := completedAt.Time
			step.CompleteAt = &t
		}

		wf.Steps[step.ID] = step

		node := &workflow.Node{
			StepID:    step.ID,
			DependsOn: make([]string, 0),
		}

		if err := wf.DAG.AddNode(node); err != nil {
			return nil, fmt.Errorf("failed to restore DAG node during load: %w", err)
		}
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("step rows integration error: %w", err)
	}

	depQuery := `
		SELECT step_dependencies.step_id, step_dependencies.depends_on_step_id 
		FROM step_dependencies
		INNER JOIN steps s ON step_dependencies.step_id = s.id
		WHERE s.workflow_id = $1`

	depRows, err := s.db.QueryContext(ctx, depQuery, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to query step dependencies: %w", err)
	}
	defer depRows.Close()

	for depRows.Next() {
		var stepID, dependsOnID string
		if err := depRows.Scan(&stepID, &dependsOnID); err != nil {
			return nil, fmt.Errorf("failed to scan dependency row: %w", err)
		}

		if node, exists := wf.DAG.Nodes[stepID]; exists {
			node.DependsOn = append(node.DependsOn, dependsOnID)
		}
	}

	return wf, nil
}
