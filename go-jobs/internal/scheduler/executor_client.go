package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// ExecutorTrigger is the payload sent to an executor's /run endpoint.
type ExecutorTrigger struct {
	LogID           int64  `json:"log_id"`
	JobID           int64  `json:"job_id"`
	ExecutorHandler string `json:"executor_handler"`
	ExecuteType     string `json:"execute_type"`
	ExecuteParam    string `json:"execute_param"`
	ShardingIndex   int    `json:"sharding_index"`
	ShardingTotal   int    `json:"sharding_total"`
	Timeout         int    `json:"timeout"`
}

// KillRequest is the payload sent to an executor's /kill endpoint.
type KillRequest struct {
	LogID int64 `json:"log_id"`
	JobID int64 `json:"job_id"`
}

// ExecutorClient is a thin HTTP client for communicating with executor nodes.
type ExecutorClient struct {
	address    string
	httpClient *http.Client
}

// NewExecutorClient creates an ExecutorClient targeting the given address (host:port).
func NewExecutorClient(address string) *ExecutorClient {
	return &ExecutorClient{
		address: address,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// Run triggers a job execution on the remote executor.
func (c *ExecutorClient) Run(ctx context.Context, trigger *ExecutorTrigger) error {
	return c.post(ctx, "/executor/run", trigger)
}

// Kill requests the executor to terminate a running job.
func (c *ExecutorClient) Kill(ctx context.Context, req *KillRequest) error {
	return c.post(ctx, "/executor/kill", req)
}

// IdleBeat checks whether the executor is idle (not running anything for a given handler).
func (c *ExecutorClient) IdleBeat(ctx context.Context, jobID int64) error {
	return c.post(ctx, "/executor/idleBeat", map[string]int64{"job_id": jobID})
}

// Beat sends a heartbeat ping to verify the executor is alive.
func (c *ExecutorClient) Beat(ctx context.Context) error {
	return c.post(ctx, "/executor/beat", nil)
}

func (c *ExecutorClient) post(ctx context.Context, path string, body interface{}) error {
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return fmt.Errorf("executor client: encode body: %w", err)
		}
	}

	url := fmt.Sprintf("http://%s%s", c.address, path)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, &buf)
	if err != nil {
		return fmt.Errorf("executor client: new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Go-Jobs-Token", "internal") // simple internal auth header

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("executor client: do request to %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errBody struct {
			Message string `json:"message"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&errBody)
		return fmt.Errorf("executor client: %s returned %d: %s", url, resp.StatusCode, errBody.Message)
	}
	return nil
}
