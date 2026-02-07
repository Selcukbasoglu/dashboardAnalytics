package services

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"macroquant-intel/backend-go/internal/config"
	"macroquant-intel/backend-go/internal/models"
)

type PythonClient struct {
	baseURL      string
	hc           *http.Client
	intelTimeout time.Duration
	cb           *circuitBreaker
}

type UpstreamError struct {
	Status int
	Body   string
}

func (e *UpstreamError) Error() string {
	return fmt.Sprintf("python api: %d", e.Status)
}

type circuitBreaker struct {
	mu        sync.Mutex
	failures  int
	threshold int
	openedAt  time.Time
	cooldown  time.Duration
}

func newCircuitBreaker(threshold int, cooldown time.Duration) *circuitBreaker {
	return &circuitBreaker{threshold: threshold, cooldown: cooldown}
}

func (c *circuitBreaker) allow() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failures < c.threshold {
		return true
	}
	if time.Since(c.openedAt) > c.cooldown {
		c.failures = 0
		c.openedAt = time.Time{}
		return true
	}
	return false
}

func (c *circuitBreaker) success() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failures = 0
	c.openedAt = time.Time{}
}

func (c *circuitBreaker) fail() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failures++
	if c.failures >= c.threshold {
		c.openedAt = time.Now()
	}
}

func NewPythonClient(cfg config.Config) *PythonClient {
	return &PythonClient{
		baseURL: cfg.PyBaseURL,
		hc: &http.Client{
			Timeout: cfg.RequestTimeout,
		},
		intelTimeout: cfg.IntelTimeout,
		cb:           newCircuitBreaker(cfg.CircuitFailLimit, cfg.CircuitCooldown),
	}
}

func (c *PythonClient) Health(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/health", nil)
	if err != nil {
		return err
	}
	res, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		return fmt.Errorf("python health: %s", res.Status)
	}
	return nil
}

func (c *PythonClient) RunIntel(ctx context.Context, req models.IntelRequest) (models.IntelResponse, error) {
	var out models.IntelResponse
	if !c.cb.allow() {
		return out, errors.New("python circuit breaker open")
	}

	payload, err := json.Marshal(req)
	if err != nil {
		return out, err
	}

	url := c.baseURL + "/intel/run"
	intelClient := *c.hc
	if c.intelTimeout > 0 {
		intelClient.Timeout = c.intelTimeout
	}
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
		if err != nil {
			return out, err
		}
		hreq.Header.Set("Content-Type", "application/json")

		res, err := intelClient.Do(hreq)
		if err != nil {
			lastErr = err
			select {
			case <-ctx.Done():
				c.cb.fail()
				return out, ctx.Err()
			case <-time.After(time.Duration(attempt+1) * 300 * time.Millisecond):
				continue
			}
		}

		if res.StatusCode >= 300 {
			lastErr = fmt.Errorf("python intel: %s", res.Status)
			res.Body.Close()
			select {
			case <-ctx.Done():
				c.cb.fail()
				return out, ctx.Err()
			case <-time.After(time.Duration(attempt+1) * 300 * time.Millisecond):
				continue
			}
		}

		if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
			lastErr = err
			res.Body.Close()
			select {
			case <-ctx.Done():
				c.cb.fail()
				return out, ctx.Err()
			case <-time.After(time.Duration(attempt+1) * 300 * time.Millisecond):
				continue
			}
		}

		res.Body.Close()
		c.cb.success()
		return out, nil
	}

	c.cb.fail()
	return out, lastErr
}

func (c *PythonClient) FetchJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	res, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		return fmt.Errorf("python api: %s", res.Status)
	}
	return json.NewDecoder(res.Body).Decode(out)
}

func (c *PythonClient) FetchJSONWithTimeout(ctx context.Context, path string, out any, timeout time.Duration) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	hc := *c.hc
	hc.Timeout = timeout
	res, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		return fmt.Errorf("python api: %s", res.Status)
	}
	return json.NewDecoder(res.Body).Decode(out)
}

func (c *PythonClient) FetchJSONWithStatus(ctx context.Context, path string, out any) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return 0, err
	}
	res, err := c.hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	status := res.StatusCode
	if status >= 300 {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		return status, &UpstreamError{Status: status, Body: string(body)}
	}
	if err := json.NewDecoder(res.Body).Decode(out); err != nil {
		return status, err
	}
	return status, nil
}

func (c *PythonClient) FetchJSONWithStatusTimeout(ctx context.Context, path string, out any, timeout time.Duration) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return 0, err
	}
	hc := *c.hc
	hc.Timeout = timeout
	res, err := hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	status := res.StatusCode
	if status >= 300 {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		return status, &UpstreamError{Status: status, Body: string(body)}
	}
	if err := json.NewDecoder(res.Body).Decode(out); err != nil {
		return status, err
	}
	return status, nil
}

func (c *PythonClient) PostJSON(ctx context.Context, path string, body []byte, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		return fmt.Errorf("python api: %s", res.Status)
	}
	return json.NewDecoder(res.Body).Decode(out)
}

func (c *PythonClient) PostJSONWithStatus(ctx context.Context, path string, body []byte, out any) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	status := res.StatusCode
	if status >= 300 {
		bodyStr, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		return status, &UpstreamError{Status: status, Body: string(bodyStr)}
	}
	if err := json.NewDecoder(res.Body).Decode(out); err != nil {
		return status, err
	}
	return status, nil
}

func (c *PythonClient) DeleteJSONWithStatus(ctx context.Context, path string, out any) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL+path, nil)
	if err != nil {
		return 0, err
	}
	res, err := c.hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	status := res.StatusCode
	if status >= 300 {
		bodyStr, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		return status, &UpstreamError{Status: status, Body: string(bodyStr)}
	}
	if err := json.NewDecoder(res.Body).Decode(out); err != nil {
		return status, err
	}
	return status, nil
}
