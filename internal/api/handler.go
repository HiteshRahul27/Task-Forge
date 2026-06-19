package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"durable-engine/internal/storage"
	"durable-engine/internal/workflow"

	"github.com/google/uuid"
)

type IncomingRequest struct {
	Name  string        `json:"name"`
	Steps []StepPayload `json:"steps"`
}

type StepPayload struct {
	ID           string          `json:"id"`
	Type         string          `json:"type"`
	Payload      json.RawMessage `json:"payload"`
	Dependencies []string        `json:"dependencies"`
}

type WorkflowHandler struct {
	store *storage.WorkflowStorage
}

func NewWorkflowHandler(store *storage.WorkflowStorage) *WorkflowHandler {
	return &WorkflowHandler{
		store: store,
	}
}

func (h *WorkflowHandler) CreateWorkflow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "Failed to read request body")
		return
	}
	defer r.Body.Close()

	var req IncomingRequest

	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid JSON format: "+err.Error())
		return
	}

	if req.Name == "" {
		h.writeError(w, http.StatusBadRequest, "workflow name is required")
		return
	}

	wf := workflow.NewWorkflow(
		uuid.New().String(),
		req.Name,
	)

	idMapping := make(map[string]string)

	for _, s := range req.Steps {
		if s.ID == "" {
			h.writeError(w, http.StatusBadRequest, "each step must contain an id")
			return
		}

		if _, exists := idMapping[s.ID]; exists {
			h.writeError(
				w,
				http.StatusBadRequest,
				fmt.Sprintf("duplicate step id found: %s", s.ID),
			)
			return
		}

		idMapping[s.ID] = uuid.New().String()
	}

	for _, s := range req.Steps {
		if s.Type == "" {
			h.writeError(w, http.StatusBadRequest, "each step must contain a type")
			return
		}

		mappedDeps := make([]string, len(s.Dependencies))
		for i, depID := range s.Dependencies {
			uuidDep, exists := idMapping[depID]
			if !exists {
				h.writeError(w, http.StatusBadRequest, fmt.Sprintf("step %s depends on a missing step: %s", s.ID, depID))
				return
			}
			mappedDeps[i] = uuidDep
		}

		step := &workflow.Step{
			ID:         idMapping[s.ID],
			Type:       s.Type,
			Payload:    s.Payload,
			Status:     workflow.Pending,
			MaxRetries: 3,
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}

		if err := wf.AddStep(step, mappedDeps); err != nil {
			h.writeError(
				w,
				http.StatusBadRequest,
				fmt.Sprintf("failed to add step %s: %v", s.ID, err),
			)
			return
		}
	}

	if err := wf.Validate(); err != nil {
		h.writeError(
			w,
			http.StatusBadRequest,
			"Invalid workflow topology: "+err.Error(),
		)
		return
	}

	if err := h.store.SaveWorkflow(r.Context(), wf); err != nil {
		h.writeError(
			w,
			http.StatusInternalServerError,
			"failed to save workflow: "+err.Error(),
		)
		return
	}

	h.writeJSON(w, http.StatusCreated, map[string]interface{}{
		"workflow_id": wf.ID,
		"name":        wf.Name,
		"steps_count": len(wf.Steps),
	})
}

func (h *WorkflowHandler) GetWorkflow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	parts := strings.Split(r.URL.Path, "/")

	if len(parts) < 5 {
		h.writeError(
			w,
			http.StatusBadRequest,
			"missing workflow id",
		)
		return
	}

	workflowID := parts[4]

	wf, err := h.store.LoadWorkflow(
		r.Context(),
		workflowID,
	)
	if err != nil {
		h.writeError(
			w,
			http.StatusNotFound,
			err.Error(),
		)
		return
	}

	h.writeJSON(w, http.StatusOK, wf)
}

func (h *WorkflowHandler) writeError(
	w http.ResponseWriter,
	status int,
	message string,
) {
	h.writeJSON(w, status, map[string]string{
		"error": message,
	})
}

func (h *WorkflowHandler) writeJSON(
	w http.ResponseWriter,
	status int,
	data interface{},
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(data)
}
