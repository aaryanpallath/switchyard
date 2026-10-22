package proxy

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aaryanpallath/switchyard/internal/pool"
)

func serve(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(h)
	t.Cleanup(s.Close)
	return s
}

// a URL nothing listens on: connections are refused
func refused(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return "http://" + addr
}

func setup(t *testing.T, cfg Config, urls ...string) (*pool.Pool, *httptest.Server) {
	t.Helper()
	pc := pool.DefaultConfig()
	pc.EjectAfter = 0
	p := pool.New(pc)
	for _, u := range urls {
		if _, err := p.Add(u); err != nil {
			t.Fatal(err)
		}
	}
	lb := httptest.NewServer(New(p, cfg))
	t.Cleanup(lb.Close)
	return p, lb
}

func get(t *testing.T, method, url, body string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestForwardsPathQueryBodyAndHeaders(t *testing.T) {
	be := serve(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		w.Header().Set("X-Seen", r.URL.Path+"?"+r.URL.RawQuery+" "+string(b)+" "+r.Header.Get("X-Forwarded-For"))
		w.WriteHeader(http.StatusTeapot)
	})
	_, lb := setup(t, DefaultConfig(), be.URL)
	req, _ := http.NewRequest(http.MethodPut, lb.URL+"/a/b?x=1", strings.NewReader("hello"))
	req.Header.Set("Connection", "close")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusTeapot || resp.Header.Get("X-Seen") != "/a/b?x=1 hello 127.0.0.1" {
		t.Fatalf("got %d %q", resp.StatusCode, resp.Header.Get("X-Seen"))
	}
}

func TestClientCannotSpoofForwardingHeaders(t *testing.T) {
	var got http.Header
	be := serve(t, func(w http.ResponseWriter, r *http.Request) { got = r.Header.Clone() })
	_, lb := setup(t, DefaultConfig(), be.URL)
	req, _ := http.NewRequest(http.MethodGet, lb.URL, nil)
	req.Header.Set("X-Forwarded-For", "1.2.3.4")
	req.Header.Set("X-Real-IP", "1.2.3.4")
	req.Header.Set("Forwarded", "for=1.2.3.4")
	req.Header.Set("Connection", "X-Secret")
	req.Header.Set("X-Secret", "hop-by-hop, must not be forwarded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if got.Get("X-Forwarded-For") != "127.0.0.1" || got.Get("X-Real-Ip") != "" || got.Get("Forwarded") != "" {
		t.Fatalf("spoofed forwarding headers reached the backend: %v", got)
	}
	if got.Get("X-Secret") != "" || got.Get("X-Forwarded-Proto") != "http" {
		t.Fatalf("hop-by-hop or proto header wrong: %v", got)
	}
}

func TestRefusedConnectionIsRetriedElsewhere(t *testing.T) {
	// Regression: the error path used to call a nil cancel func and crash the handler. The handler is called
	// directly, because through a real server Go's HTTP client quietly resends a GET whose connection dies, which
	// hid the crash from an earlier version of this test.
	ok := serve(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	pc := pool.DefaultConfig()
	pc.EjectAfter = 0
	p := pool.New(pc)
	_, _ = p.Add(refused(t))
	_, _ = p.Add(ok.URL)
	h := New(p, DefaultConfig())
	for range 4 {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
		if w.Code != 200 || w.Body.String() != "ok" {
			t.Fatalf("got %d %q", w.Code, w.Body)
		}
	}
}

func TestPostIsNeverRetried(t *testing.T) {
	var calls atomic.Int32
	bad := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	good := serve(t, func(w http.ResponseWriter, _ *http.Request) { calls.Add(1) })
	p, lb := setup(t, DefaultConfig(), bad.URL, good.URL)
	_ = p
	fails := 0
	for range 4 {
		code, _ := get(t, http.MethodPost, lb.URL, "pay $5")
		if code == http.StatusInternalServerError {
			fails++
		}
	}
	if calls.Load() != 4 || fails != 2 {
		t.Fatalf("each POST must reach exactly one backend: %d calls, %d failures", calls.Load(), fails)
	}
}

func TestSlowBackendHitsTryTimeoutAndRetries(t *testing.T) {
	slow := serve(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(2 * time.Second):
		case <-r.Context().Done():
		}
	})
	fast := serve(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("fast")) })
	cfg := DefaultConfig()
	cfg.TryTimeout = 100 * time.Millisecond
	_, lb := setup(t, cfg, slow.URL, fast.URL)
	start := time.Now()
	for range 2 { // round robin sends one of the two to the slow backend first
		if code, body := get(t, http.MethodGet, lb.URL, ""); code != 200 || body != "fast" {
			t.Fatalf("got %d %q", code, body)
		}
	}
	if time.Since(start) > time.Second {
		t.Fatalf("took %v: the per-try timeout did not cut the slow backend off", time.Since(start))
	}
}

func TestAllBackendsFailing(t *testing.T) {
	bad := serve(t, func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "down", http.StatusServiceUnavailable) })
	dead := refused(t)
	_, lb := setup(t, DefaultConfig(), dead, bad.URL)
	code, body := get(t, http.MethodGet, lb.URL, "")
	if code != http.StatusServiceUnavailable && code != http.StatusBadGateway {
		t.Fatalf("got %d", code)
	}
	if strings.Contains(body, strings.TrimPrefix(dead, "http://")) {
		t.Fatalf("error response leaks a backend address: %q", body)
	}
	_, empty := setup(t, DefaultConfig())
	if code, body := get(t, http.MethodGet, empty.URL, ""); code != http.StatusBadGateway || !strings.Contains(body, "no healthy backend") {
		t.Fatalf("got %d %q", code, body)
	}
}

func TestBodyLimit(t *testing.T) {
	be := serve(t, func(http.ResponseWriter, *http.Request) {})
	cfg := DefaultConfig()
	cfg.MaxBodyBytes = 10
	_, lb := setup(t, cfg, be.URL)
	if code, _ := get(t, http.MethodPut, lb.URL, strings.Repeat("x", 11)); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("got %d", code)
	}
}

func TestProberMarksDownAndUp(t *testing.T) {
	var healthy atomic.Bool
	healthy.Store(true)
	be := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		if !healthy.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	p := pool.New(pool.DefaultConfig())
	_, _ = p.Add(be.URL)
	pr := &pool.Prober{Pool: p, Path: "/healthz", Interval: time.Hour, Timeout: time.Second}
	healthy.Store(false)
	pr.ProbeAll(context.Background())
	pr.ProbeAll(context.Background())
	if p.Status()[0].Up {
		t.Fatal("still up after two failed probes")
	}
	healthy.Store(true)
	pr.ProbeAll(context.Background())
	pr.ProbeAll(context.Background())
	if !p.Status()[0].Up {
		t.Fatal("still down after two passed probes")
	}
}
