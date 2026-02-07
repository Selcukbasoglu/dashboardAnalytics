package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

func (a *API) StreamQuotes(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusBadRequest)
		return
	}

	q := r.URL.Query()
	rawAssets := q.Get("assets")
	intervalSec := parseIntParam(q.Get("interval"), 10, 5, 60)

	assets := parseAssets(rawAssets, 60)
	if len(assets) == 0 {
		http.Error(w, "assets required", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	ticker := time.NewTicker(time.Duration(intervalSec) * time.Second)
	defer ticker.Stop()

	send := func() {
		ctx, cancel := context.WithTimeout(r.Context(), a.cfg.RequestTimeout)
		defer cancel()
		quotes, err := a.quotes.Fetch(ctx, assets)
		payload := map[string]any{
			"tsISO":  nowISO(),
			"quotes": quotes,
		}
		if err != nil {
			payload["error"] = err.Error()
		}
		data, _ := json.Marshal(payload)
		_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}

	send()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			send()
		}
	}
}

func parseAssets(raw string, max int) []string {
	if raw == "" {
		return []string{}
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{})
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		p = strings.ToUpper(p)
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
		if len(out) >= max {
			break
		}
	}
	return out
}
