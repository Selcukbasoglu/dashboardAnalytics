package handlers

import (
	"context"
	"errors"
	"net"
	"net/http"

	"macroquant-intel/backend-go/internal/services"
)

func writeUpstreamError(w http.ResponseWriter, err error, status int) {
	var upErr *services.UpstreamError
	if errors.As(err, &upErr) {
		if upErr.Status == http.StatusTooManyRequests {
			w.Header().Set("Retry-After", "60")
			writeJSON(w, http.StatusTooManyRequests, map[string]any{"error": err.Error(), "upstream_status": upErr.Status})
			return
		}
		if upErr.Status >= 400 && upErr.Status < 500 {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"error": err.Error(), "upstream_status": upErr.Status})
			return
		}
		if upErr.Status == http.StatusRequestTimeout || upErr.Status == http.StatusGatewayTimeout {
			writeJSON(w, http.StatusGatewayTimeout, map[string]any{"error": err.Error(), "upstream_status": upErr.Status})
			return
		}
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error(), "upstream_status": upErr.Status})
		return
	}

	if errors.Is(err, context.DeadlineExceeded) {
		writeJSON(w, http.StatusGatewayTimeout, map[string]any{"error": "upstream_timeout"})
		return
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		writeJSON(w, http.StatusGatewayTimeout, map[string]any{"error": "upstream_timeout"})
		return
	}
	if status != 0 {
		writeJSON(w, status, map[string]any{"error": err.Error(), "upstream_status": status})
		return
	}
	writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
}
