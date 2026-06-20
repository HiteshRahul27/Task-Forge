package test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"durable-engine/internal/executor"
	"durable-engine/internal/runner"
	"durable-engine/internal/storage"
	"durable-engine/internal/workflow"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

func TestWorkflowRecovery(t *testing.T) {
	ctx := context.Background()

	if err := godotenv.Load("../.env"); err != nil {
		log.Println("No .env file found, falling back to system environment variables")
	}

	dsn := fmt.Sprintf("user=%s password='%s' host=%s port=%s dbname=%s sslmode=%s",
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"),
		os.Getenv("DB_PORT"),
		os.Getenv("DB_NAME"),
		os.Getenv("DB_SSLMODE"),
	)

	db, err := storage.NewPostgresDB(dsn)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	wf := workflow.NewWorkflow(
		uuid.New().String(),
		"test-workflow-recovery",
	)

	idA := uuid.New().String()
	idB := uuid.New().String()
	idC := uuid.New().String()

	dummyPayload := json.RawMessage(`{"input": "test data"}`)

	stepA := &workflow.Step{
		ID:         idA,
		Type:       "stepA",
		Status:     workflow.Success,
		Payload:    dummyPayload,
		MaxRetries: 3,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
		CompleteAt: func() *time.Time {
			t := time.Now()
			return &t
		}(),
	}

	stepB := &workflow.Step{
		ID:         idB,
		Type:       "stepB",
		Status:     workflow.Pending,
		Payload:    dummyPayload,
		Retries:    1,
		MaxRetries: 3,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	stepC := &workflow.Step{
		ID:         idC,
		Type:       "stepC",
		Status:     workflow.Pending,
		Payload:    dummyPayload,
		MaxRetries: 3,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	if err := wf.AddStep(stepA, []string{}); err != nil {
		t.Fatalf("Failed to add step A: %v", err)
	}

	if err := wf.AddStep(stepB, []string{idA}); err != nil {
		t.Fatalf("Failed to add step B: %v", err)
	}

	if err := wf.AddStep(stepC, []string{idB}); err != nil {
		t.Fatalf("Failed to add step C: %v", err)
	}

	store := storage.NewWorkflowStorage(db)

	if err := store.SaveWorkflow(ctx, wf); err != nil {
		t.Fatalf("Failed to save workflow: %v", err)
	}

	loadedWf, err := store.LoadWorkflow(ctx, wf.ID)
	if err != nil {
		t.Fatalf("Failed to load workflow: %v", err)
	}

	if loadedWf.ID != wf.ID || loadedWf.Name != wf.Name || loadedWf.Status != wf.Status {
		t.Errorf("Loaded workflow does not match original. Got ID: %s, Name: %s, Status: %s", loadedWf.ID, loadedWf.Name, loadedWf.Status)
	}

	for stepID, originalStep := range wf.Steps {
		loadedStep, exists := loadedWf.Steps[stepID]
		if !exists {
			t.Errorf("Step %s missing in loaded workflow", stepID)
			continue
		}

		if loadedStep.ID != originalStep.ID ||
			loadedStep.Type != originalStep.Type ||
			loadedStep.Status != originalStep.Status ||
			loadedStep.Retries != originalStep.Retries ||
			loadedStep.MaxRetries != originalStep.MaxRetries {
			t.Errorf("Loaded step %s does not match original. Got Type: %s, Status: %s, Retries: %d, MaxRetries: %d",
				stepID, loadedStep.Type, loadedStep.Status, loadedStep.Retries, loadedStep.MaxRetries)
		}
	}

	execInstance := executor.NewStepExecutor()

	aRuns := 0
	bRuns := 0
	cRuns := 0

	execInstance.RegisterHandler("stepA", func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
		aRuns++
		return []byte(`{"result":"success"}`), nil
	})

	execInstance.RegisterHandler("stepB", func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
		bRuns++
		return []byte(`{"result":"success"}`), nil
	})

	execInstance.RegisterHandler("stepC", func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
		cRuns++
		return []byte(`{"result":"success"}`), nil
	})

	err = runner.RunWorkflow(ctx, loadedWf, execInstance, nil)
	if err != nil {
		t.Fatalf("Workflow execution completely broke: %v", err)
	}

	if aRuns != 0 {
		t.Errorf("Expected step A to not execute, but it ran %d times", aRuns)
	}

	if bRuns != 1 {
		t.Errorf("Expected step B to execute once, but it ran %d times", bRuns)
	}

	if cRuns != 1 {
		t.Errorf("Expected step C to execute once, but it ran %d times", cRuns)
	}

	if loadedWf.Status != workflow.Completed {
		t.Errorf("Expected workflow to be COMPLETED, but got: %s", loadedWf.Status)
	}

	if loadedWf.Steps[idA].Status != workflow.Success {
		t.Errorf("Expected step A to be SUCCESS, got: %s", loadedWf.Steps[idA].Status)
	}

	if loadedWf.Steps[idB].Status != workflow.Success {
		t.Errorf("Expected step B to be SUCCESS, got: %s", loadedWf.Steps[idB].Status)
	}

	if loadedWf.Steps[idC].Status != workflow.Success {
		t.Errorf("Expected step C to be SUCCESS, got: %s", loadedWf.Steps[idC].Status)
	}
}
