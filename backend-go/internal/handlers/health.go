package handlers

import (
	"context"
	"net/http"
	"os"
	"time"

	"macroquant-intel/backend-go/internal/models"
)

func (a *API) Health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	deps := []string{}
	missing := []string{}
	depsStatus := map[string]models.DepStatus{}
	if err := a.py.Health(ctx); err != nil {
		missing = append(missing, "analytics_unreachable")
		depsStatus["analytics"] = models.DepStatus{Ok: false, Error: err.Error()}
	} else {
		deps = append(deps, "analytics")
		depsStatus["analytics"] = models.DepStatus{Ok: true}
	}

	resp := models.HealthResponse{
		Ok:          len(missing) == 0,
		TsISO:       nowISO(),
		Service:     "backend-go",
		Version:     os.Getenv("SERVICE_VERSION"),
		Deps:        deps,
		DepsStatus:  depsStatus,
		DataMissing: missing,
		Env: map[string]bool{
			"FINNHUB_API_KEY":       os.Getenv("FINNHUB_API_KEY") != "",
			"TWELVEDATA_API_KEY":    os.Getenv("TWELVEDATA_API_KEY") != "",
			"OPENAI_API_KEY":        os.Getenv("OPENAI_API_KEY") != "",
			"OPENAI_MODEL":          os.Getenv("OPENAI_MODEL") != "",
			"ENABLE_OPENAI_SUMMARY": os.Getenv("ENABLE_OPENAI_SUMMARY") != "",
			"PY_INTEL_BASE_URL":     os.Getenv("PY_INTEL_BASE_URL") != "",
			"DATABASE_URL":          os.Getenv("DATABASE_URL") != "",
			"NEXT_PUBLIC_API_BASE":  os.Getenv("NEXT_PUBLIC_API_BASE") != "",
		},
		Features: map[string]bool{
			"finnhub_fallback_enabled":   os.Getenv("FINNHUB_API_KEY") != "",
			"twelvedata_fallback_enabled": os.Getenv("TWELVEDATA_API_KEY") != "",
			"openai_summaries_enabled": summaryEnabled(),
		},
	}
	writeJSON(w, http.StatusOK, resp)
}

func summaryEnabled() bool {
	flag := os.Getenv("ENABLE_OPENAI_SUMMARY")
	if flag == "" {
		return false
	}
	enabled := flag == "1" || flag == "true" || flag == "yes" || flag == "on"
	if !enabled {
		return false
	}
	if os.Getenv("OPENAI_API_KEY") == "" {
		return false
	}
	return true
}
