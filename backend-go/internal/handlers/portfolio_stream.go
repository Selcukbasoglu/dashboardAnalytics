package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func (a *API) StreamPortfolio(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusBadRequest)
		return
	}

	q := r.URL.Query()
	base := normalizeBase(q.Get("base"))
	horizon := normalizeWindow(q.Get("horizon"))
	intervalSec := parseIntParam(q.Get("interval"), 15, 5, 60)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	ticker := time.NewTicker(time.Duration(intervalSec) * time.Second)
	defer ticker.Stop()

	send := func() {
		path := "/portfolio?"
		if base != "" {
			path += "base=" + base + "&"
		}
		if horizon != "" {
			path += "horizon=" + horizon + "&"
		}
		if path[len(path)-1] == '&' || path[len(path)-1] == '?' {
			path = path[:len(path)-1]
		}

		timeout := a.cfg.PortfolioTimeout
		if timeout <= 0 {
			timeout = a.cfg.RequestTimeout
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		var out any
		err := a.py.FetchJSONWithTimeout(ctx, path, &out, timeout)
		if err != nil {
			payload := map[string]any{
				"error": err.Error(),
				"ts":    nowISO(),
			}
			if data, mErr := json.Marshal(payload); mErr == nil {
				fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()
			}
			return
		}

		data, err := json.Marshal(out)
		if err != nil {
			return
		}
		fmt.Fprintf(w, "data: %s\n\n", data)
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
