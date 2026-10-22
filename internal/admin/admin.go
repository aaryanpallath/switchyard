// Package admin is the REST API operators use to manage the pool.
//
//	GET    /backends         every backend with its health, ejection state and request counts
//	POST   /backends         {"url": "http://10.0.0.5:8080"} registers a backend
//	DELETE /backends/{id}    removes one
//	GET    /healthz          the balancer's own health
//
// The API can redirect all traffic, so every request must carry the bearer token when one is configured. Without a
// token, only requests from a loopback address, addressed to a loopback host name, are accepted: the first proves
// the caller is local, the second stops DNS rebinding. Requests with an
// Origin header (sent by browsers) are refused either way, so a web page cannot drive the API through a visitor's
// browser, and POST requires a JSON content type.
package admin

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"mime"
	"net"
	"net/http"

	"github.com/aaryanpallath/switchyard/internal/pool"
)

// LoopbackAddr reports whether a listen address only accepts local connections.
func LoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	return err == nil && host != "" && loopbackHost(host)
}

func loopbackHost(hostport string) bool {
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		host = hostport
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func guard(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Header.Get("Origin") != "":
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "browser requests are not accepted"})
		case token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1:
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing or wrong bearer token"})
		case token == "" && (!loopbackHost(r.RemoteAddr) || !loopbackHost(r.Host)):
			// the peer address is what proves the caller is local (a remote client can send any Host header);
			// the Host check additionally stops a browser that was pointed here by DNS rebinding
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "without a token, only localhost may call the admin API"})
		default:
			next.ServeHTTP(w, r)
		}
	})
}

// Handler serves the admin API. An empty token restricts it to loopback host names.
func Handler(p *pool.Pool, token string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /backends", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, p.Status())
	})
	mux.HandleFunc("POST /backends", func(w http.ResponseWriter, r *http.Request) {
		if mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); mt != "application/json" {
			writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be application/json"})
			return
		}
		var in struct {
			URL string `json:"url"`
		}
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&in); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body must be {\"url\": \"http://host:port\"}"})
			return
		}
		b, err := p.Add(in.URL)
		switch {
		case errors.Is(err, pool.ErrDuplicate):
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		case err != nil:
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
		default:
			writeJSON(w, http.StatusCreated, map[string]string{"id": b.ID, "url": b.URL.String()})
		}
	})
	mux.HandleFunc("DELETE /backends/{id}", func(w http.ResponseWriter, r *http.Request) {
		if err := p.Remove(r.PathValue("id")); err != nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return guard(token, mux)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
