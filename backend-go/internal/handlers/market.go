package handlers

import (
	"net/http"

	"macroquant-intel/backend-go/internal/models"
	"macroquant-intel/backend-go/internal/services"
)

func (a *API) Market(w http.ResponseWriter, r *http.Request) {
	key := "market:v1"
	if a.cache != nil {
		if b, ok := a.cache.Get(r.Context(), key); ok {
			var cached models.MarketSnapshot
			if err := services.UnmarshalCache(b, &cached); err == nil {
				writeJSON(w, http.StatusOK, cached)
				return
			}
		}
	}

	ctx, cancel := timeboxed(r, a.cfg.RequestTimeout)
	defer cancel()

	resp, _, err := a.intel.GetSnapshot(ctx, "1h", "6h", nil)
	if resp.TsISO == "" || err != nil {
		writeJSON(w, http.StatusOK, models.MarketSnapshot{TsISO: nowISO()})
		return
	}
	if a.cache != nil {
		if b, err := services.MarshalCache(resp.Market); err == nil {
			_ = a.cache.Set(r.Context(), key, b, a.cfg.CacheTTLMarket)
		}
	}
	writeJSON(w, http.StatusOK, resp.Market)
}
