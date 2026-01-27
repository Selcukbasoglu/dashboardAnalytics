package handlers

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

func (a *API) Derivatives(w http.ResponseWriter, r *http.Request) {
	exchange := strings.ToLower(r.URL.Query().Get("exchange"))
	if exchange == "" {
		exchange = "binance"
	}
	symbol := strings.ToUpper(r.URL.Query().Get("symbol"))
	if symbol == "" {
		symbol = "BTCUSDT"
	}

	ctx, cancel := contextTimeout(r.Context(), a.cfg.RequestTimeout)
	defer cancel()

	resp, health := a.deriv.Get(ctx, exchange, symbol)
	resp.Exchange = exchange
	resp.Symbol = symbol
	if resp.Ts == "" {
		resp.Ts = nowISO()
	}
	resp.Health = health

	payload, _ := json.Marshal(resp)
	sum := sha1.Sum(payload)
	etag := hex.EncodeToString(sum[:])

	if match := r.Header.Get("If-None-Match"); match != "" && match == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	w.Header().Set("ETag", etag)
	if ts, err := time.Parse(time.RFC3339, resp.Ts); err == nil {
		w.Header().Set("Last-Modified", ts.UTC().Format(http.TimeFormat))
	}
	writeJSON(w, http.StatusOK, resp)
}

func contextTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}
