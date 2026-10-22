package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aaryanpallath/switchyard/internal/pool"
)

func do(t *testing.T, h http.Handler, method, path, body string, headers ...string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Host = "127.0.0.1:8081"
	r.RemoteAddr = "127.0.0.1:50000"
	r.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(headers); i += 2 {
		switch headers[i] {
		case "Host":
			r.Host = headers[i+1]
		case "RemoteAddr":
			r.RemoteAddr = headers[i+1]
		default:
			r.Header.Set(headers[i], headers[i+1])
		}
	}
	h.ServeHTTP(w, r)
	return w
}

func TestBackendLifecycle(t *testing.T) {
	h := Handler(pool.New(pool.DefaultConfig()), "")
	if w := do(t, h, "POST", "/backends", `{"url": "http://10.0.0.5:9000"}`); w.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", w.Code, w.Body)
	}
	if w := do(t, h, "POST", "/backends", `{"url": "http://10.0.0.5:9000"}`); w.Code != http.StatusConflict {
		t.Fatalf("duplicate: %d", w.Code)
	}
	for _, bad := range []string{`{"url": "10.0.0.6"}`, `not json`, `{"url": "http://x:1", "admin": true}`} {
		if w := do(t, h, "POST", "/backends", bad); w.Code != http.StatusBadRequest && w.Code != http.StatusUnprocessableEntity {
			t.Fatalf("%s: %d", bad, w.Code)
		}
	}
	var list []pool.Status
	w := do(t, h, "GET", "/backends", "")
	if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil || len(list) != 1 || !list[0].Up {
		t.Fatalf("list: %s", w.Body)
	}
	if w := do(t, h, "DELETE", "/backends/"+list[0].ID, ""); w.Code != http.StatusNoContent {
		t.Fatalf("delete: %d", w.Code)
	}
	if w := do(t, h, "DELETE", "/backends/"+list[0].ID, ""); w.Code != http.StatusNotFound {
		t.Fatalf("delete again: %d", w.Code)
	}
	if w := do(t, h, "PATCH", "/backends", ""); w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("wrong method: %d", w.Code)
	}
}

func TestAdminRefusesBrowsersRebindingAndMissingTokens(t *testing.T) {
	open := Handler(pool.New(pool.DefaultConfig()), "")
	body := `{"url": "http://10.0.0.5:9000"}`
	if w := do(t, open, "POST", "/backends", body, "Origin", "https://evil.example"); w.Code != http.StatusForbidden {
		t.Fatalf("browser request: %d", w.Code)
	}
	if w := do(t, open, "POST", "/backends", body, "Host", "evil.example:8081"); w.Code != http.StatusForbidden {
		t.Fatalf("DNS-rebound host: %d", w.Code)
	}
	if w := do(t, open, "POST", "/backends", body, "Content-Type", "text/plain"); w.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("form-style post: %d", w.Code)
	}
	if w := do(t, open, "GET", "/backends", "", "RemoteAddr", "203.0.113.9:4444", "Host", "localhost:8081"); w.Code != http.StatusForbidden {
		t.Fatalf("remote client claiming Host: localhost: %d", w.Code)
	}
	if w := do(t, open, "GET", "/backends", "", "Host", "localhost:8081"); w.Code != http.StatusOK {
		t.Fatalf("localhost: %d", w.Code)
	}

	locked := Handler(pool.New(pool.DefaultConfig()), "s3cret")
	if w := do(t, locked, "GET", "/backends", "", "Host", "lb.internal:8081"); w.Code != http.StatusUnauthorized {
		t.Fatalf("no token: %d", w.Code)
	}
	if w := do(t, locked, "GET", "/backends", "", "Authorization", "Bearer wrong"); w.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token: %d", w.Code)
	}
	if w := do(t, locked, "POST", "/backends", body, "Host", "lb.internal:8081", "Authorization", "Bearer s3cret"); w.Code != http.StatusCreated {
		t.Fatalf("right token from another host: %d", w.Code)
	}
}

func TestLoopbackAddr(t *testing.T) {
	for addr, want := range map[string]bool{"127.0.0.1:8081": true, "localhost:8081": true, "[::1]:8081": true,
		":8081": false, "0.0.0.0:8081": false, "10.0.0.5:8081": false} {
		if LoopbackAddr(addr) != want {
			t.Errorf("LoopbackAddr(%q) = %v", addr, !want)
		}
	}
}
