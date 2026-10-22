// Package admin is the REST API operators use to manage the pool.
//
//	GET    /backends         every backend with its health, ejection state and request counts
//	POST   /backends         {"url": "http://10.0.0.5:8080"} registers a backend
//	DELETE /backends/{id}    removes one
//	GET    /healthz          the balancer's own health
package admin

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/aaryanpallath/switchyard/internal/pool"
)

func Handler(p *pool.Pool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /backends", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, p.Status())
	})
	mux.HandleFunc("POST /backends", func(w http.ResponseWriter, r *http.Request) {
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
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
