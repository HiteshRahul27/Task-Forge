package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type HTTPHandlerConfig struct {
	URL    string `json:"url"`
	Method string `json:"method"`
	Body   string `json:"body"`
}

func HTTPTaskHandler(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var config HTTPHandlerConfig
	if err := json.Unmarshal(payload, &config); err != nil {
		return nil, fmt.Errorf("invalid http handler config: %w", err)
	}
	if config.URL == "" {
		return nil, fmt.Errorf("url is required")
	}

	if config.Method == "" {
		config.Method = "GET"
	}

	config.Method = strings.ToUpper(config.Method)

	if config.Method != "GET" && config.Method != "POST" {
		return nil, fmt.Errorf("unsupported method: %s", config.Method)
	}
	req, err := http.NewRequestWithContext(ctx, config.Method, config.URL, strings.NewReader(config.Body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("http request returned status %d: %s", resp.StatusCode, string(body))
	}

	result := map[string]interface{}{
		"status_code": resp.StatusCode,
		"body":        string(body),
	}

	output, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %w", err)
	}

	return json.RawMessage(output), nil
}
