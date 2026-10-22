// Package proxy forwards HTTP requests to the pool's backends, with per-try timeouts and retries.
//
// A request is retried on a different backend when the first one fails to connect, times out or answers 5xx, but
// only for idempotent methods (GET, HEAD, PUT, DELETE, OPTIONS): retrying a POST could perform it twice. Every
// outcome is reported to the pool, which drives passive ejection.
package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/aaryanpallath/switchyard/internal/pool"
)

type Config struct {
	MaxAttempts  int           // tries per request, each on a different backend; 1 disables retries
	TryTimeout   time.Duration // per try; 0 means only the client's own deadline applies
	MaxBodyBytes int64         // request bodies are buffered so a retry can resend them
}

func DefaultConfig() Config {
	return Config{MaxAttempts: 3, TryTimeout: 2 * time.Second, MaxBodyBytes: 1 << 20}
}

type Proxy struct {
	pool   *pool.Pool
	cfg    Config
	client *http.Client
}

func New(p *pool.Pool, cfg Config) *Proxy {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 256
	return &Proxy{pool: p, cfg: cfg, client: &http.Client{
		Transport: transport,
		// pass redirects through to the client instead of following them here
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

var hopByHop = []string{"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer",
	"Transfer-Encoding", "Upgrade"}

func idempotent(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete, http.MethodOptions:
		return true
	}
	return false
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, p.cfg.MaxBodyBytes))
	if err != nil {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	attempts := max(p.cfg.MaxAttempts, 1)
	if !idempotent(r.Method) {
		attempts = 1
	}
	tried := map[string]bool{}
	var lastErr error
	for range attempts {
		b, err := p.pool.Pick(tried)
		if err != nil {
			break
		}
		tried[b.ID] = true
		resp, cancel, err := p.try(r, b, body)
		if err == nil && resp.StatusCode < 500 {
			p.pool.Done(b, true)
			relay(w, resp)
			cancel()
			return
		}
		p.pool.Done(b, false)
		if err == nil { // a 5xx: keep it in case no other backend does better
			lastErr = errors.New(resp.Status)
			if len(tried) == attempts {
				relay(w, resp)
				cancel()
				return
			}
			resp.Body.Close()
		} else {
			lastErr = err
		}
		cancel()
		if r.Context().Err() != nil { // the client gave up
			return
		}
	}
	msg := "no healthy backend"
	if lastErr != nil {
		msg = "upstream failed: " + lastErr.Error()
	}
	http.Error(w, msg, http.StatusBadGateway)
}

func (p *Proxy) try(r *http.Request, b *pool.Backend, body []byte) (*http.Response, context.CancelFunc, error) {
	ctx, cancel := r.Context(), context.CancelFunc(func() {})
	if p.cfg.TryTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, p.cfg.TryTimeout)
	}
	target := *b.URL
	target.Path = singleJoin(b.URL.Path, r.URL.Path)
	target.RawQuery = r.URL.RawQuery
	out, err := http.NewRequestWithContext(ctx, r.Method, target.String(), bytes.NewReader(body))
	if err != nil {
		cancel()
		return nil, func() {}, err
	}
	out.Header = r.Header.Clone()
	for _, h := range hopByHop {
		out.Header.Del(h)
	}
	out.Header.Set("X-Forwarded-For", clientIP(r))
	out.Host = r.Host
	resp, err := p.client.Do(out)
	if err != nil {
		cancel()
		// a no-op, not nil: the caller always calls it (the first chaos run crashed here on a refused connection)
		return nil, func() {}, err
	}
	return resp, cancel, nil
}

func relay(w http.ResponseWriter, resp *http.Response) {
	defer resp.Body.Close()
	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	for _, h := range hopByHop {
		w.Header().Del(h)
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func singleJoin(a, b string) string {
	switch {
	case a == "" || a == "/":
		return b
	case a[len(a)-1] == '/' && len(b) > 0 && b[0] == '/':
		return a + b[1:]
	case a[len(a)-1] != '/' && (len(b) == 0 || b[0] != '/'):
		return a + "/" + b
	}
	return a + b
}

func clientIP(r *http.Request) string {
	host := r.RemoteAddr
	for i := len(host) - 1; i >= 0; i-- {
		if host[i] == ':' {
			return host[:i]
		}
	}
	return host
}
