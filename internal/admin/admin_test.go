package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aaryanpallath/switchyard/internal/pool"
)

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))
	return w
}

func TestBackendLifecycle(t *testing.T) {
	h := Handler(pool.New(pool.DefaultConfig()))
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
