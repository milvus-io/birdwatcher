package ops

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// GcPauseParams pauses datacoord garbage collection for the given duration.
// Wraps GET /management/datacoord/garbage_collection/pause.
type GcPauseParams struct {
	Addr         string `yaml:"addr"`
	APIKey       string `yaml:"apiKey"`
	PauseSeconds int64  `yaml:"pauseSeconds"`
	CollectionID int64  `yaml:"collectionId"`
}

// GcResumeParams resumes datacoord garbage collection.
// Wraps GET /management/datacoord/garbage_collection/resume.
type GcResumeParams struct {
	Addr   string `yaml:"addr"`
	APIKey string `yaml:"apiKey"`
	Ticket string `yaml:"ticket"`
}

func (p *GcPauseParams) Execute(ctx context.Context, rc *RunContext) (any, error) {
	if p.Addr == "" {
		return nil, fmt.Errorf("gc_pause: `addr` is required")
	}
	if p.PauseSeconds <= 0 {
		return nil, fmt.Errorf("gc_pause: `pauseSeconds` must be > 0")
	}
	q := url.Values{}
	q.Set("pause_seconds", strconv.FormatInt(p.PauseSeconds, 10))
	if p.CollectionID > 0 {
		q.Set("collection_id", strconv.FormatInt(p.CollectionID, 10))
	}
	target := strings.TrimRight(p.Addr, "/") + "/management/datacoord/garbage_collection/pause?" + q.Encode()

	status, body, err := doManagementGet(ctx, target, p.APIKey)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("gc_pause failed, status=%d body=%s", status, body)
	}
	fmt.Fprintf(rc.Out(), "gc pause: %s\n", body)
	return map[string]any{"status": status, "body": body}, nil
}

func (p *GcResumeParams) Execute(ctx context.Context, rc *RunContext) (any, error) {
	if p.Addr == "" {
		return nil, fmt.Errorf("gc_resume: `addr` is required")
	}
	q := url.Values{}
	if p.Ticket != "" {
		q.Set("ticket", p.Ticket)
	}
	target := strings.TrimRight(p.Addr, "/") + "/management/datacoord/garbage_collection/resume"
	if encoded := q.Encode(); encoded != "" {
		target += "?" + encoded
	}

	status, body, err := doManagementGet(ctx, target, p.APIKey)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("gc_resume failed, status=%d body=%s", status, body)
	}
	fmt.Fprintf(rc.Out(), "gc resume: %s\n", body)
	return map[string]any{"status": status, "body": body}, nil
}

func doManagementGet(ctx context.Context, target, apiKey string) (int, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return 0, "", fmt.Errorf("build http request: %w", err)
	}
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body), nil
}

func init() {
	Register("gc_pause", func() Op { return &GcPauseParams{} })
	Register("gc_resume", func() Op { return &GcResumeParams{} })
}
