package exporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Sink implementations honor context cancellation and may receive duplicate IDs.
type Sink interface {
	Send(context.Context, Record) error
}

type HTTPStatusError struct{ Status int }

func (e *HTTPStatusError) Error() string { return fmt.Sprintf("export HTTP status %d", e.Status) }

type HTTPSink struct {
	client                       *http.Client
	endpoint, username, password string
}

func NewHTTPSink(c Config) *HTTPSink {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = c.ExportTimeout
	transport.MaxIdleConnsPerHost = c.Workers
	return &HTTPSink{client: &http.Client{Transport: transport, Timeout: c.ExportTimeout,
		// Redirects must not forward ingest credentials or alter request semantics.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, endpoint: c.SinkURL, username: c.SinkUsername, password: c.SinkPassword}
}

func (s *HTTPSink) Send(ctx context.Context, r Record) error {
	body, e := json.Marshal(r)
	if e != nil {
		return fmt.Errorf("encode event: %w", e)
	}
	body = append(body, '\n')
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(body))
	if e != nil {
		return fmt.Errorf("create export request: %w", e)
	}
	req.Header.Set("Content-Type", "application/stream+json")
	if s.username != "" || s.password != "" {
		req.SetBasicAuth(s.username, s.password)
	}
	resp, e := s.client.Do(req)
	if e != nil { // URL query may contain secrets, so do not propagate url.Error text.
		return fmt.Errorf("export request failed: %w", sanitizeTransportError(e))
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &HTTPStatusError{Status: resp.StatusCode}
	}
	return nil
}

func (s *HTTPSink) Close() { s.client.CloseIdleConnections() }

// transportFailure exposes retry classification without returning request URLs.
type transportFailure struct{ cause error }

func (e transportFailure) Error() string   { return "transport error" }
func (e transportFailure) Unwrap() error   { return e.cause }
func sanitizeTransportError(e error) error { return transportFailure{cause: e} }

// Backoff returns a capped exponential delay; jitter is injected for deterministic tests.
func Backoff(initial, max time.Duration, attempt int, jitter float64) time.Duration {
	d := initial
	for i := 0; i < attempt && d < max; i++ {
		if d > max/2 {
			d = max
			break
		}
		d *= 2
	}
	if d > max {
		d = max
	}
	return time.Duration(float64(d) * (0.5 + 0.5*jitter))
}
