package handlers

import (
	"context"
	"net/http"
	"time"

	"macroquant-intel/backend-go/internal/models"
)

func (a *API) Intel(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	timeframe := q.Get("timeframe")
	if timeframe == "" {
		timeframe = "1h"
	}
	newsTimespan := q.Get("newsTimespan")
	if newsTimespan == "" {
		newsTimespan = "6h"
	}
	watch := parseWatchlist(q.Get("watch"), a.cfg.MaxWatchlist)

	ctx, cancel := timeboxed(r, a.cfg.RequestTimeout)
	defer cancel()

	resp, cached := a.getIntel(ctx, timeframe, newsTimespan, watch)
	if resp.TsISO == "" {
		resp = emptyIntel(timeframe, newsTimespan)
		resp.Debug.Notes = append(resp.Debug.Notes, "python_unreachable")
		resp.Debug.DataMissing = append(resp.Debug.DataMissing, "analytics")
		writeJSON(w, http.StatusOK, resp)
		return
	}

	if cached {
		resp.Debug.Notes = append(resp.Debug.Notes, "cache_hit")
	}

	writeJSON(w, http.StatusOK, resp)
}

func emptyIntel(timeframe string, newsTimespan string) models.IntelResponse {
	return models.IntelResponse{
		TsISO:        nowISO(),
		Timeframe:    timeframe,
		NewsTimespan: newsTimespan,
		Market:       models.MarketSnapshot{TsISO: nowISO()},
		Leaders: []models.LeadersGroup{
			{Key: "states", Title: "Devlet Baskanlari"},
			{Key: "energy", Title: "Enerji CEO"},
			{Key: "sp500", Title: "S&P 500 CEO"},
			{Key: "crypto_ceos", Title: "Kripto CEO"},
		},
		TopNews: []models.NewsItem{},
		Flow:    models.FlowPanel{FlowScore: 0, Evidence: []string{"veri eksik"}, WatchMetrics: []string{}},
		Derivatives: models.DerivativesPanel{Status: "data_missing"},
		Risk:        models.RiskPanel{Flags: []string{"DATA_MISSING"}},
		Debug:       models.DebugInfo{DataMissing: []string{"analytics"}, Notes: []string{}},
		BlockHashes: map[string]string{},
		ChangedBlocks: []string{},
		DailyMovers: models.DailyEquityMovers{
			Asof:          nowISO(),
			Universe:      "US",
			MethodVersion: "movers_v1",
			Items:         []models.DailyEquityMoverItem{},
			Debug:         models.DailyEquityMoversDebug{ReasonIfEmpty: "DATA_LOW_SIGNAL"},
		},
		Forecast: models.ForecastPanel{
			CryptoOutlook: models.CryptoOutlook{
				Asof:       nowISO(),
				BtcBias:    0,
				EthBias:    0,
				Confidence: 0,
				Drivers:    []string{"veri eksik"},
				WatchMetrics: []string{},
			},
		},
	}
}

func timeboxed(r *http.Request, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), d)
}
