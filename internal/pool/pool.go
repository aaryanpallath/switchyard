// Package pool tracks the backends a load balancer can send traffic to, and which of them are fit to receive it.
//
// A backend leaves rotation in two ways:
//   - active health checks: a background prober calls each backend's health endpoint and marks it down after
//     FailThreshold failures in a row (and up again after RiseThreshold successes);
//   - passive ejection: the proxy reports the outcome of real requests, and a backend that fails EjectAfter
//     requests in a row is ejected for EjectFor, without waiting for the next probe.
package pool

import (
	"errors"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

type Strategy string

const (
	RoundRobin       Strategy = "round_robin"
	LeastConnections Strategy = "least_connections"
)

type Config struct {
	Strategy      Strategy
	FailThreshold int           // failed probes in a row before a backend is marked down
	RiseThreshold int           // passed probes in a row before it is marked up again
	EjectAfter    int           // failed requests in a row before passive ejection; 0 disables it
	EjectFor      time.Duration // how long an ejected backend stays out
}

func DefaultConfig() Config {
	return Config{Strategy: RoundRobin, FailThreshold: 2, RiseThreshold: 2, EjectAfter: 3, EjectFor: 5 * time.Second}
}

type Backend struct {
	ID  string
	URL *url.URL

	active       atomic.Int64 // requests in flight
	mu           sync.Mutex
	up           bool
	probeFails   int
	probePasses  int
	requestFails int
	ejectedUntil time.Time
	served       int64
	failed       int64
}

// Status is a snapshot of a backend for the admin API.
type Status struct {
	ID           string    `json:"id"`
	URL          string    `json:"url"`
	Up           bool      `json:"up"`
	Ejected      bool      `json:"ejected"`
	EjectedUntil time.Time `json:"ejected_until,omitzero"`
	InFlight     int64     `json:"in_flight"`
	Served       int64     `json:"served"`
	Failed       int64     `json:"failed"`
}

type Pool struct {
	cfg      Config
	mu       sync.RWMutex
	backends []*Backend
	next     atomic.Uint64
	seq      int
	now      func() time.Time
}

var (
	ErrDuplicate = errors.New("a backend with this URL is already registered")
	ErrNotFound  = errors.New("no such backend")
	ErrNoBackend = errors.New("no healthy backend")
)

func New(cfg Config) *Pool { return &Pool{cfg: cfg, now: time.Now} }

// SetClock replaces the clock, for tests.
func (p *Pool) SetClock(now func() time.Time) { p.now = now }

func (p *Pool) Config() Config { return p.cfg }

// Add registers a backend. New backends start in rotation; the prober takes them out if they fail.
func (p *Pool) Add(raw string) (*Backend, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("invalid backend URL %q: want http(s)://host:port", raw)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, b := range p.backends {
		if b.URL.String() == u.String() {
			return nil, ErrDuplicate
		}
	}
	p.seq++
	b := &Backend{ID: fmt.Sprintf("b%d", p.seq), URL: u, up: true}
	p.backends = append(p.backends, b)
	return b, nil
}

func (p *Pool) Remove(id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, b := range p.backends {
		if b.ID == id {
			p.backends = append(p.backends[:i:i], p.backends[i+1:]...)
			return nil
		}
	}
	return ErrNotFound
}

func (p *Pool) Backends() []*Backend {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return append([]*Backend(nil), p.backends...)
}

func (b *Backend) available(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.up && !now.Before(b.ejectedUntil)
}

// Pick chooses a backend for a request, skipping any in exclude (backends this request already tried).
// The caller must call Done on the result when the request finishes.
func (p *Pool) Pick(exclude map[string]bool) (*Backend, error) {
	now := p.now()
	var ok []*Backend
	for _, b := range p.Backends() {
		if !exclude[b.ID] && b.available(now) {
			ok = append(ok, b)
		}
	}
	if len(ok) == 0 {
		return nil, ErrNoBackend
	}
	var b *Backend
	switch p.cfg.Strategy {
	case LeastConnections:
		// fewest requests in flight; the round-robin counter breaks ties so equal backends share load
		start := int(p.next.Add(1) % uint64(len(ok)))
		for i := range ok {
			c := ok[(start+i)%len(ok)]
			if b == nil || c.active.Load() < b.active.Load() {
				b = c
			}
		}
	default:
		b = ok[int(p.next.Add(1)%uint64(len(ok)))]
	}
	b.active.Add(1)
	return b, nil
}

// Done records a finished request: success resets the failure streak, and a streak of EjectAfter failures ejects
// the backend for EjectFor.
func (p *Pool) Done(b *Backend, success bool) {
	b.active.Add(-1)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.served++
	if success {
		b.requestFails = 0
		return
	}
	b.failed++
	b.requestFails++
	if p.cfg.EjectAfter > 0 && b.requestFails >= p.cfg.EjectAfter {
		b.ejectedUntil = p.now().Add(p.cfg.EjectFor)
		b.requestFails = 0
	}
}

// ProbeResult records one active health check.
func (p *Pool) ProbeResult(b *Backend, passed bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if passed {
		b.probeFails = 0
		b.probePasses++
		if !b.up && b.probePasses >= p.cfg.RiseThreshold {
			b.up = true
		}
		return
	}
	b.probePasses = 0
	b.probeFails++
	if b.up && b.probeFails >= p.cfg.FailThreshold {
		b.up = false
	}
}

func (p *Pool) Status() []Status {
	now := p.now()
	var out []Status
	for _, b := range p.Backends() {
		b.mu.Lock()
		s := Status{ID: b.ID, URL: b.URL.String(), Up: b.up, Ejected: now.Before(b.ejectedUntil),
			InFlight: b.active.Load(), Served: b.served, Failed: b.failed}
		if s.Ejected {
			s.EjectedUntil = b.ejectedUntil
		}
		b.mu.Unlock()
		out = append(out, s)
	}
	return out // in registration order
}
