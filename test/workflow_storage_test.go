package test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"durable-engine/internal/storage"
	"durable-engine/internal/workflow"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

func TestSaveLoadWorkflow(t *testing.T) {
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
		"test-workflow-save-load",
	)

	idA := uuid.New().String()
	idB := uuid.New().String()
	idC := uuid.New().String()

	dummyPayload := json.RawMessage(`{"input": "test data"}`)

	stepA := &workflow.Step{
		ID:         idA,
		Type:       "dummy",
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
		Type:       "dummy",
		Status:     workflow.Failed,
		Payload:    dummyPayload,
		Retries:    1,
		MaxRetries: 3,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	stepC := &workflow.Step{
		ID:         idC,
		Type:       "dummy",
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

	if loadedWf.ID != wf.ID || loadedWf.Name != wf.Name {
		t.Errorf("Loaded workflow does not match original. Got ID: %s, Name: %s", loadedWf.ID, loadedWf.Name)
	}

	for stepID, originalStep := range wf.Steps {
		loadedStep, exists := loadedWf.Steps[stepID]
		if !exists {
			t.Errorf("Step %s missing in loaded workflow", stepID)
			continue
		}

		if loadedStep.ID != originalStep.ID {
			t.Errorf(
				"Step %s ID mismatch: original=%s loaded=%s",
				stepID,
				originalStep.ID,
				loadedStep.ID,
			)
		}

		if loadedStep.Type != originalStep.Type {
			t.Errorf(
				"Step %s Type mismatch: original=%q loaded=%q",
				stepID,
				originalStep.Type,
				loadedStep.Type,
			)
		}

		if string(loadedStep.Payload) != string(originalStep.Payload) {
			t.Errorf(
				"Step %s Payload mismatch: original=%s loaded=%s",
				stepID,
				string(originalStep.Payload),
				string(loadedStep.Payload),
			)
		}

		if string(loadedStep.Output) != string(originalStep.Output) {
			t.Errorf(
				"Step %s Output mismatch: original=%s loaded=%s",
				stepID,
				string(originalStep.Output),
				string(loadedStep.Output),
			)
		}

		if loadedStep.Status != originalStep.Status {
			t.Errorf(
				"Step %s Status mismatch: original=%s loaded=%s",
				stepID,
				originalStep.Status,
				loadedStep.Status,
			)
		}

		if loadedStep.Retries != originalStep.Retries {
			t.Errorf(
				"Step %s Retries mismatch: original=%d loaded=%d",
				stepID,
				originalStep.Retries,
				loadedStep.Retries,
			)
		}

		if loadedStep.MaxRetries != originalStep.MaxRetries {
			t.Errorf(
				"Step %s MaxRetries mismatch: original=%d loaded=%d",
				stepID,
				originalStep.MaxRetries,
				loadedStep.MaxRetries,
			)
		}

		if (loadedStep.Error == nil) != (originalStep.Error == nil) {
			t.Errorf(
				"Step %s Error nil mismatch",
				stepID,
			)
		} else if loadedStep.Error != nil && originalStep.Error != nil {
			if *loadedStep.Error != *originalStep.Error {
				t.Errorf(
					"Step %s Error mismatch: original=%q loaded=%q",
					stepID,
					*originalStep.Error,
					*loadedStep.Error,
				)
			}
		}
	}
	loadedA, ok := loadedWf.DAG.Nodes[idA]
	if !ok {
		t.Errorf("Step %s missing in loaded DAG", idA)
	}

	if len(loadedA.DependsOn) != len(wf.DAG.Nodes[idA].DependsOn) {
		t.Errorf("Loaded DAG node %s dependencies count mismatch", idA)
	}

	if loadedWf.DAG.Nodes[idB].DependsOn[0] != idA {
		t.Errorf("Loaded DAG node B dependency mismatch, expected %s, got %s", idA, loadedWf.DAG.Nodes[idB].DependsOn[0])
	}

	if loadedWf.DAG.Nodes[idC].DependsOn[0] != idB {
		t.Errorf("Loaded DAG node C dependency mismatch, expected %s, got %s", idB, loadedWf.DAG.Nodes[idC].DependsOn[0])
	}

}
