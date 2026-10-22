package pool

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// Prober runs active health checks: every Interval it calls Path on each backend, and a check passes on a 2xx
// response within Timeout.
type Prober struct {
	Pool     *Pool
	Path     string
	Interval time.Duration
	Timeout  time.Duration
	Client   *http.Client
}

// Run probes until ctx is cancelled.
func (pr *Prober) Run(ctx context.Context) {
	t := time.NewTicker(pr.Interval)
	defer t.Stop()
	for {
		pr.ProbeAll(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// ProbeAll checks every backend once, concurrently, and records the results.
func (pr *Prober) ProbeAll(ctx context.Context) {
	client := pr.Client
	if client == nil {
		client = http.DefaultClient
	}
	var wg sync.WaitGroup
	for _, b := range pr.Pool.Backends() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pctx, cancel := context.WithTimeout(ctx, pr.Timeout)
			defer cancel()
			req, err := http.NewRequestWithContext(pctx, http.MethodGet, b.URL.JoinPath(pr.Path).String(), nil)
			passed := false
			if err == nil {
				if resp, err := client.Do(req); err == nil {
					passed = resp.StatusCode >= 200 && resp.StatusCode < 300
					resp.Body.Close()
				}
			}
			if ctx.Err() == nil {
				pr.Pool.ProbeResult(b, passed)
			}
		}()
	}
	wg.Wait()
}
