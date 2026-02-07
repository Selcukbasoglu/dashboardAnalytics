package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"macroquant-intel/backend-go/internal/config"
	"macroquant-intel/backend-go/internal/services"
)

type API struct {
	cfg   config.Config
	cache services.Cache
	py    *services.PythonClient
	deriv *services.DerivativesClient
	quotes *services.QuotesClient
	intel *services.IntelService
}

func New(cfg config.Config, cache services.Cache, py *services.PythonClient) *API {
	return &API{
		cfg:   cfg,
		cache: cache,
		py:    py,
		deriv: services.NewDerivativesClient(cfg, cache),
		quotes: services.NewQuotesClient(cfg, cache),
		intel: services.NewIntelService(cfg, cache, py),
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func parseWatchlist(raw string, max int) []string {
	if raw == "" {
		return []string{}
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
		if len(out) >= max {
			break
		}
	}
	return out
}

func parseIntParam(v string, def int, min int, max int) int {
	if v == "" {
		return def
	}
	var out int
	_, err := fmt.Sscanf(v, "%d", &out)
	if err != nil {
		return def
	}
	if out < min {
		return min
	}
	if out > max {
		return max
	}
	return out
}

func nowISO() string {
	return time.Now().UTC().Format(time.RFC3339)
}
