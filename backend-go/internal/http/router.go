package http

import (
	"net/http"

	"macroquant-intel/backend-go/internal/config"
	"macroquant-intel/backend-go/internal/handlers"
	"macroquant-intel/backend-go/internal/services"
)

func NewRouter(cfg config.Config, cache services.Cache, py *services.PythonClient) http.Handler {
	api := handlers.New(cfg, cache, py)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/health", api.Health)
	mux.HandleFunc("/api/v1/intel", api.Intel)
	mux.HandleFunc("/api/v1/market", api.Market)
	mux.HandleFunc("/api/v1/bars", api.Bars)
	mux.HandleFunc("/api/v1/news", api.News)
	mux.HandleFunc("/api/v1/leaders", api.Leaders)
	mux.HandleFunc("/api/v1/portfolio", api.Portfolio)
	mux.HandleFunc("/api/v1/portfolio/daily-brief", api.PortfolioDailyBrief)
	mux.HandleFunc("/api/v1/portfolio/debate", api.PortfolioDebate)
	mux.HandleFunc("/api/v1/derivatives", api.Derivatives)
	mux.HandleFunc("/api/v1/forecasts/latest", api.ForecastLatest)
	mux.HandleFunc("/api/v1/forecasts/metrics", api.ForecastMetrics)
	mux.HandleFunc("/api/v1/events/latest", api.EventsLatest)
	mux.HandleFunc("/api/v1/stream", api.StreamIntel)

	h := http.Handler(mux)
	h = withRecovery(h)
	h = withLogging(h)
	h = withRateLimit(cfg.RateLimitPerMin)(h)
	h = withCORS(h)
	return h
}
