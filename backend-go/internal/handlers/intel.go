package handlers

import (
	"context"
	"net/http"
	"strings"
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

	intelTimeout := a.cfg.IntelTimeout
	if intelTimeout <= 0 {
		intelTimeout = a.cfg.RequestTimeout
	}
	ctx, cancel := timeboxed(r, intelTimeout)
	defer cancel()

	resp, meta, err := a.intel.GetSnapshot(ctx, timeframe, newsTimespan, watch)
	if resp.TsISO == "" {
		resp = emptyIntel(timeframe, newsTimespan)
		resp.Debug.Notes = appendUniqueString(resp.Debug.Notes, "python_unreachable")
		resp.Debug.DataMissing = appendUniqueString(resp.Debug.DataMissing, "analytics")
		writeJSON(w, http.StatusOK, resp)
		return
	}

	if meta.Source == "cache" || meta.Source == "stale_cache" {
		resp.Debug.Notes = appendUniqueString(resp.Debug.Notes, "cache_hit")
	}
	if meta.Stale {
		resp.Debug.Notes = appendUniqueString(resp.Debug.Notes, "stale_cache")
	}
	if err != nil || meta.Err != "" {
		resp.Debug.Notes = appendUniqueString(resp.Debug.Notes, "refresh_error")
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
		TopNews:       []models.NewsItem{},
		Flow:          models.FlowPanel{FlowScore: 0, Evidence: []string{"veri eksik"}, WatchMetrics: []string{}},
		Derivatives:   models.DerivativesPanel{Status: "data_missing"},
		Risk:          models.RiskPanel{Flags: []string{"DATA_MISSING"}},
		Debug:         models.DebugInfo{DataMissing: []string{"analytics"}, Notes: []string{}},
		BlockHashes:   map[string]string{},
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
				Asof:         nowISO(),
				BtcBias:      0,
				EthBias:      0,
				Confidence:   0,
				Drivers:      []string{"veri eksik"},
				WatchMetrics: []string{},
			},
		},
	}
}

func appendUniqueString(items []string, value string) []string {
	v := strings.TrimSpace(value)
	if v == "" {
		return items
	}
	for _, it := range items {
		if strings.EqualFold(strings.TrimSpace(it), v) {
			return items
		}
	}
	return append(items, v)
}

func timeboxed(r *http.Request, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), d)
}
