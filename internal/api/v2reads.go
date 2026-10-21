package api

import (
	"context"
	"net/http"
	"strconv"
)

// viewsV2 serves GET /v2/views. A total comes from page_totals; a daily series
// reads page_views like /v1, so it goes through the same handler.
func (a *API) viewsV2(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Get("from") != "" || q.Get("to") != "" {
		a.views(w, r)
		return
	}
	key := q.Get("key")
	if !keyRE.MatchString(key) {
		httpError(w, http.StatusBadRequest, "invalid key")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), readTimeout)
	defer cancel()
	n, err := a.store.TotalFromTotals(ctx, key)
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, map[string]any{"key": key, "views": n})
}

// topV2 serves GET /v2/top from page_totals.
func (a *API) topV2(w http.ResponseWriter, r *http.Request) {
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
	top, err := a.store.TopFromTotals(ctx, n)
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, map[string]any{"top": top})
}
