package handlers

import (
	"fmt"
	"net/http"
	"net/url"

	"macroquant-intel/backend-go/internal/services"
)

func (a *API) Bars(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	assets := q.Get("assets")
	limit := parseIntParam(q.Get("limit"), 96, 8, 192)
	if assets == "" {
		writeJSON(w, http.StatusOK, map[string]any{"tsISO": nowISO(), "assets": map[string]any{}})
		return
	}
	key := fmt.Sprintf("bars:v1:%s:%d", assets, limit)
	if a.cache != nil {
		if b, ok := a.cache.Get(r.Context(), key); ok {
			var cached map[string]any
			if err := services.UnmarshalCache(b, &cached); err == nil {
				writeJSON(w, http.StatusOK, cached)
				return
			}
		}
	}

	ctx, cancel := timeboxed(r, a.cfg.RequestTimeout)
	defer cancel()
	path := fmt.Sprintf("/bars/latest?assets=%s&limit=%d", url.QueryEscape(assets), limit)
	var resp map[string]any
	if err := a.py.FetchJSON(ctx, path, &resp); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}
	if a.cache != nil {
		if b, err := services.MarshalCache(resp); err == nil {
			_ = a.cache.Set(r.Context(), key, b, a.cfg.CacheTTLMarket)
		}
	}
	writeJSON(w, http.StatusOK, resp)
}
