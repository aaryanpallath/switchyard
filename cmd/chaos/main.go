// chaos: how much do health checks, passive ejection and retries protect clients when backends fail?
//
//	go run ./cmd/chaos            # prints a table, writes results/chaos.json
//
// Five in-process backends serve a request in ~2 ms. Two seconds into each 12-second run, three of them fail in
// different ways:
//
//	dead   the process exits: connections are refused
//	flaky  half of its responses are 503, but its health endpoint still says OK (a partial failure)
//	slow   every request takes 1.5 s, and so does its health check
//
// 32 clients send GET requests in a loop through the balancer, each with a 5-second timeout, and every
// configuration faces the same scenario on fresh backends. A request fails if it gets a non-2xx status or no
// response.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aaryanpallath/switchyard/internal/pool"
	"github.com/aaryanpallath/switchyard/internal/proxy"
)

type mode int32

const (
	healthy mode = iota
	flaky
	slow
	dead
)

type backend struct {
	mode atomic.Int32
	srv  *http.Server
	ln   net.Listener
	url  string
	rng  *rand.Rand
	mu   sync.Mutex
}

func startBackend(seed uint64) *backend {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	b := &backend{ln: ln, url: "http://" + ln.Addr().String(), rng: rand.New(rand.NewPCG(seed, 1))}
	b.srv = &http.Server{Handler: http.HandlerFunc(b.serve), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = b.srv.Serve(ln) }()
	return b
}

func (b *backend) serve(w http.ResponseWriter, r *http.Request) {
	m := mode(b.mode.Load())
	if m == slow {
		time.Sleep(1500 * time.Millisecond)
	}
	if r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusOK) // a flaky backend's health check still passes
		return
	}
	time.Sleep(2 * time.Millisecond)
	if m == flaky {
		b.mu.Lock()
		fail := b.rng.IntN(2) == 0
		b.mu.Unlock()
		if fail {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
	}
	_, _ = w.Write([]byte("ok"))
}

func (b *backend) kill() {
	b.mode.Store(int32(dead))
	_ = b.srv.Close()
}

type setup struct {
	Name        string
	Strategy    pool.Strategy
	Probe       bool
	EjectAfter  int
	MaxAttempts int
	TryTimeout  time.Duration
}

type result struct {
	Setup           string  `json:"setup"`
	Requests        int     `json:"requests_after_failures"`
	Failed          int     `json:"failed"`
	FailedPct       float64 `json:"failed_pct"`
	P50Ms           float64 `json:"p50_ms"`
	P99Ms           float64 `json:"p99_ms"`
	RequestsPerSec  float64 `json:"requests_per_sec"`
	FailedLastHalf  int     `json:"failed_in_last_5s"`
	RequestsLastHal int     `json:"requests_in_last_5s"`
}

func run(s setup, duration, failAt time.Duration, clients int) result {
	var backends []*backend
	cfg := pool.DefaultConfig()
	cfg.Strategy, cfg.EjectAfter = s.Strategy, s.EjectAfter
	p := pool.New(cfg)
	for i := range 5 {
		b := startBackend(uint64(i + 1))
		backends = append(backends, b)
		if _, err := p.Add(b.url); err != nil {
			panic(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if s.Probe {
		go (&pool.Prober{Pool: p, Path: "/healthz", Interval: 500 * time.Millisecond, Timeout: 300 * time.Millisecond}).Run(ctx)
	}
	lb := httptest.NewServer(proxy.New(p, proxy.Config{MaxAttempts: s.MaxAttempts, TryTimeout: s.TryTimeout, MaxBodyBytes: 1 << 20}))
	defer lb.Close()

	type sample struct {
		at  time.Duration
		lat time.Duration
		ok  bool
	}
	var mu sync.Mutex
	var samples []sample
	start := time.Now()
	go func() {
		time.Sleep(failAt)
		backends[0].kill()
		backends[1].mode.Store(int32(flaky))
		backends[2].mode.Store(int32(slow))
	}()
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: clients}}
	var wg sync.WaitGroup
	for range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var local []sample
			for time.Since(start) < duration {
				t0 := time.Now()
				resp, err := client.Get(lb.URL + "/work")
				ok := err == nil && resp.StatusCode == http.StatusOK
				if err == nil {
					resp.Body.Close()
				}
				local = append(local, sample{t0.Sub(start), time.Since(t0), ok})
			}
			mu.Lock()
			samples = append(samples, local...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	for _, b := range backends {
		_ = b.srv.Close()
	}

	r := result{Setup: s.Name}
	var lats []float64
	for _, x := range samples {
		if x.at < failAt {
			continue
		}
		r.Requests++
		lats = append(lats, float64(x.lat.Microseconds())/1000)
		if !x.ok {
			r.Failed++
		}
		if x.at >= duration-5*time.Second {
			r.RequestsLastHal++
			if !x.ok {
				r.FailedLastHalf++
			}
		}
	}
	slices.Sort(lats)
	pct := func(q float64) float64 { return lats[min(len(lats)-1, int(q*float64(len(lats))))] }
	r.FailedPct = 100 * float64(r.Failed) / float64(r.Requests)
	r.P50Ms, r.P99Ms = pct(0.5), pct(0.99)
	r.RequestsPerSec = float64(r.Requests) / (duration - failAt).Seconds()
	return r
}

func main() {
	duration := flag.Duration("duration", 12*time.Second, "length of each run")
	clients := flag.Int("clients", 32, "concurrent clients")
	out := flag.String("out", "results/chaos.json", "where to write results")
	flag.Parse()
	setups := []setup{
		{Name: "round robin, nothing else", Strategy: pool.RoundRobin, MaxAttempts: 1},
		{Name: "+ active health checks", Strategy: pool.RoundRobin, Probe: true, MaxAttempts: 1},
		{Name: "+ passive ejection", Strategy: pool.RoundRobin, Probe: true, EjectAfter: 3, MaxAttempts: 1},
		{Name: "+ retries (3 tries, 300 ms each)", Strategy: pool.RoundRobin, Probe: true, EjectAfter: 3, MaxAttempts: 3,
			TryTimeout: 300 * time.Millisecond},
		{Name: "all of it, least connections", Strategy: pool.LeastConnections, Probe: true, EjectAfter: 3,
			MaxAttempts: 3, TryTimeout: 300 * time.Millisecond},
	}
	var results []result
	fmt.Printf("%-34s %9s %8s %9s %9s %8s %16s\n", "setup", "requests", "failed", "p50 ms", "p99 ms", "req/s", "failed, last 5 s")
	for _, s := range setups {
		r := run(s, *duration, 2*time.Second, *clients)
		results = append(results, r)
		fmt.Printf("%-34s %9d %7.2f%% %9.1f %9.1f %8.0f %8d / %d\n", r.Setup, r.Requests, r.FailedPct, r.P50Ms, r.P99Ms,
			r.RequestsPerSec, r.FailedLastHalf, r.RequestsLastHal)
	}
	data, _ := json.MarshalIndent(map[string]any{
		"scenario": "5 backends; at 2 s one dies, one returns 503 half the time (health check still passes), one " +
			"takes 1.5 s per request; 32 clients, 5 s client timeout; measured from the failure on",
		"duration_s": duration.Seconds(), "clients": *clients, "results": results,
	}, "", "  ")
	_ = os.MkdirAll("results", 0o755)
	if err := os.WriteFile(*out, append(data, '\n'), 0o644); err != nil {
		panic(err)
	}
}
