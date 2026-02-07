package handlers

import (
	"fmt"
	"net/http"
	"time"

	"macroquant-intel/backend-go/internal/services"
)

func (a *API) ForecastLatest(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	tf := q.Get("tf")
	target := q.Get("target")
	if tf == "" || target == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "tf and target required"})
		return
	}
	ctx, cancel := timeboxed(r, a.cfg.RequestTimeout)
	defer cancel()

	var out map[string]any
	path := fmt.Sprintf("/forecasts/latest?tf=%s&target=%s", tf, target)
	if err := a.py.FetchJSON(ctx, path, &out); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *API) ForecastMetrics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := timeboxed(r, a.cfg.RequestTimeout)
	defer cancel()

	var out map[string]any
	if err := a.py.FetchJSON(ctx, "/forecasts/metrics", &out); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *API) EventsLatest(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	hours := parseIntParam(q.Get("hours"), 24, 1, 168)
	path := fmt.Sprintf("/events/latest?hours=%d", hours)
	cacheKey := fmt.Sprintf("events:v1:hours=%d", hours)
	staleKey := cacheKey + ":stale"
	downKey := cacheKey + ":down"

	if a.cache != nil {
		if _, ok := a.cache.Get(r.Context(), downKey); ok {
			if b, ok := a.cache.Get(r.Context(), staleKey); ok {
				var cached map[string]any
				if err := services.UnmarshalCache(b, &cached); err == nil {
					cached["data_status"] = "stale"
					cached["stale_reason"] = "analytics_unreachable"
					writeJSON(w, http.StatusOK, cached)
					return
				}
			}
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "analytics service unreachable"})
			return
		}
		if b, ok := a.cache.Get(r.Context(), cacheKey); ok {
			var cached map[string]any
			if err := services.UnmarshalCache(b, &cached); err == nil {
				cached["data_status"] = "cached"
				writeJSON(w, http.StatusOK, cached)
				return
			}
		}
	}

	timeout := a.cfg.RequestTimeout
	if timeout > 4*time.Second {
		timeout = 4 * time.Second
	}
	ctx, cancel := timeboxed(r, timeout)
	defer cancel()

	var out map[string]any
	status, err := a.py.FetchJSONWithStatusTimeout(ctx, path, &out, timeout)
	if err == nil && status < http.StatusBadRequest {
		out["data_status"] = "fresh"
		out["fetched_at"] = nowISO()
		if a.cache != nil {
			if b, err := services.MarshalCache(out); err == nil {
				_ = a.cache.Set(r.Context(), cacheKey, b, a.cfg.CacheTTLNews)
				_ = a.cache.Set(r.Context(), staleKey, b, 30*time.Minute)
			}
		}
		writeJSON(w, http.StatusOK, out)
		return
	}

	if a.cache != nil {
		_ = a.cache.Set(r.Context(), downKey, []byte("1"), 30*time.Second)
		if b, ok := a.cache.Get(r.Context(), staleKey); ok {
			var cached map[string]any
			if err := services.UnmarshalCache(b, &cached); err == nil {
				cached["data_status"] = "stale"
				cached["stale_reason"] = "analytics_unreachable"
				writeJSON(w, http.StatusOK, cached)
				return
			}
		}
	}

	writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "analytics service unreachable"})
}
