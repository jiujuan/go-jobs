package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/jiujuan/go-jobs/pkg/logger"
)

const (
	defaultHeartbeatInterval = 20 * time.Second
	defaultRegisterRetry     = 3
)

// RegistrationRequest is sent to the admin API to register/heartbeat.
type RegistrationRequest struct {
	AppName string `json:"app_name"`
	Title   string `json:"title"`
	Address string `json:"address"` // this executor's listening address
	Version string `json:"version"`
}

// AutoRegistrar handles auto-registration and keep-alive heartbeats
// with the go-jobs scheduler admin.
type AutoRegistrar struct {
	adminURL  string
	req       RegistrationRequest
	interval  time.Duration
	stopCh    chan struct{}
	wg        sync.WaitGroup
	httpClient *http.Client
}

// RegistrarOption is a functional option for AutoRegistrar.
type RegistrarOption func(*AutoRegistrar)

// WithHeartbeatInterval overrides the heartbeat period.
func WithHeartbeatInterval(d time.Duration) RegistrarOption {
	return func(r *AutoRegistrar) { r.interval = d }
}

// NewAutoRegistrar creates a new AutoRegistrar.
func NewAutoRegistrar(adminURL string, req RegistrationRequest, opts ...RegistrarOption) *AutoRegistrar {
	ar := &AutoRegistrar{
		adminURL: adminURL,
		req:      req,
		interval: defaultHeartbeatInterval,
		stopCh:   make(chan struct{}),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
	for _, opt := range opts {
		opt(ar)
	}
	return ar
}

// Start registers the executor and begins heartbeating.
func (r *AutoRegistrar) Start() error {
	if err := r.register(); err != nil {
		return fmt.Errorf("registrar: initial register failed: %w", err)
	}
	r.wg.Add(1)
	go r.beatLoop()
	return nil
}

// Stop sends a deregister request and stops the heartbeat loop.
func (r *AutoRegistrar) Stop() {
	close(r.stopCh)
	r.wg.Wait()
	_ = r.deregister()
}

func (r *AutoRegistrar) beatLoop() {
	defer r.wg.Done()
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			if err := r.heartbeat(); err != nil {
				logger.Warn("registrar: heartbeat failed", zap.Error(err))
				// Attempt re-registration if heartbeat fails.
				_ = r.register()
			}
		}
	}
}

func (r *AutoRegistrar) register() error {
	return r.post("/api/executor/register", r.req)
}

func (r *AutoRegistrar) heartbeat() error {
	return r.post("/api/executor/heartbeat", r.req)
}

func (r *AutoRegistrar) deregister() error {
	return r.post("/api/executor/deregister", r.req)
}

func (r *AutoRegistrar) post(path string, body interface{}) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	url := r.adminURL + path
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("registrar: POST %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("registrar: POST %s returned %d", url, resp.StatusCode)
	}
	return nil
}
