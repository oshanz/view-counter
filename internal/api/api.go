// Package api is the HTTP layer.
package api

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/oz/view-counter/internal/store"
)

const (
	readTimeout  = 2 * time.Second
	writeTimeout = 5 * time.Second
	maxRangeDays = 366
	defaultTop   = 10
	maxTop       = 100
	maxFPLen     = 256
)

var keyRE = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

type API struct {
	store  *store.Store
	secret []byte
}

func New(s *store.Store, secret []byte) http.Handler {
	a := &API{store: s, secret: secret}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/hit", a.hit)
	mux.HandleFunc("GET /v1/views", a.views)
	mux.HandleFunc("GET /v1/top", a.top)
	return mux
}

func (a *API) hit(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Key         string `json:"key"`
		Fingerprint string `json:"fingerprint"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httpError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if !keyRE.MatchString(body.Key) {
		httpError(w, http.StatusBadRequest, "invalid key")
		return
	}
	if body.Fingerprint == "" || len(body.Fingerprint) > maxFPLen {
		httpError(w, http.StatusBadRequest, "invalid fingerprint")
		return
	}

	mac := hmac.New(sha256.New, a.secret)
	mac.Write([]byte(body.Fingerprint))
	fpHash := mac.Sum(nil)[:16]

	ctx, cancel := context.WithTimeout(r.Context(), writeTimeout)
	defer cancel()
	if _, err := a.store.Hit(ctx, body.Key, fpHash); err != nil {
		log.Printf("hit: %v", err)
		httpError(w, http.StatusServiceUnavailable, "try again")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) views(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	key := q.Get("key")
	if !keyRE.MatchString(key) {
		httpError(w, http.StatusBadRequest, "invalid key")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), readTimeout)
	defer cancel()

	if q.Get("from") == "" && q.Get("to") == "" {
		n, err := a.store.Total(ctx, key)
		if err != nil {
			serverError(w, err)
			return
		}
		writeJSON(w, map[string]any{"key": key, "views": n})
		return
	}

	from, err1 := time.Parse(time.DateOnly, q.Get("from"))
	to, err2 := time.Parse(time.DateOnly, q.Get("to"))
	if err1 != nil || err2 != nil {
		httpError(w, http.StatusBadRequest, "from and to must be YYYY-MM-DD")
		return
	}
	if to.Before(from) || to.Sub(from) > maxRangeDays*24*time.Hour {
		httpError(w, http.StatusBadRequest, "invalid range (max 366 days)")
		return
	}
	days, err := a.store.Daily(ctx, key, from, to)
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, map[string]any{"key": key, "from": q.Get("from"), "to": q.Get("to"), "days": days})
}

func (a *API) top(w http.ResponseWriter, r *http.Request) {
	n := defaultTop
	if s := r.URL.Query().Get("n"); s != "" {
		v, err := strconv.Atoi(s)
		if err != nil || v < 1 || v > maxTop {
			httpError(w, http.StatusBadRequest, "n must be 1-100")
			return
		}
		n = v
	}
	ctx, cancel := context.WithTimeout(r.Context(), readTimeout)
	defer cancel()
	top, err := a.store.Top(ctx, n)
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, map[string]any{"top": top})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func httpError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func serverError(w http.ResponseWriter, err error) {
	log.Printf("read: %v", err)
	httpError(w, http.StatusServiceUnavailable, "try again")
}
