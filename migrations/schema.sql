CREATE TABLE workflow(
    id UUID NOT NULL PRIMARY KEY,

    name TEXT NOT NULL,
    status TEXT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE steps(
    id UUID PRIMARY KEY,
    workflow_id UUID NOT NULL REFERENCES workflow(id) ON DELETE CASCADE,

    status TEXT NOT NULL,

    payload JSONB DEFAULT '{}',
    output JSONB DEFAULT '{}',

    error TEXT,
    retries INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 5,

    execution_hash TEXT,
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,

    worker_id VARCHAR(255),
    started_at TIMESTAMPTZ
);

CREATE TABLE step_dependencies(
    step_id UUID NOT NULL REFERENCES steps(id) ON DELETE CASCADE,
    depends_on_step_id UUID NOT NULL REFERENCES steps(id) ON DELETE CASCADE,
    PRIMARY KEY (step_id, depends_on_step_id)
);

CREATE INDEX idx_steps_workflow_id ON steps(workflow_id);

CREATE INDEX idx_steps_status_workflow_id ON steps(status, workflow_id);

CREATE INDEX idx_steps_execution_hash ON steps(execution_hash);