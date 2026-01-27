package handlers

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
)

func (a *API) PortfolioDebate(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		q := r.URL.Query()
		base := normalizeBase(q.Get("base"))
		window := normalizeWindow(q.Get("window"))
		horizon := normalizePeriod(q.Get("horizon"))
		values := url.Values{}
		if base != "" {
			values.Set("base", base)
		}
		if window != "" {
			values.Set("window", window)
		}
		if horizon != "" {
			values.Set("horizon", horizon)
		}
		path := "/api/v1/portfolio/debate"
		if encoded := values.Encode(); encoded != "" {
			path += "?" + encoded
		}
		var out any
		status, err := a.py.FetchJSONWithStatus(r.Context(), path, &out)
		if err != nil {
			writeUpstreamError(w, err, status)
			return
		}
		writeJSON(w, http.StatusOK, out)
		return
	}
	if r.Method == http.MethodPost {
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		defer r.Body.Close()
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{"error": "payload_too_large"})
			return
		}
		if !jsonLooksValid(payload) {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid_json"})
			return
		}
		var out any
		status, err := a.py.PostJSONWithStatus(r.Context(), "/api/v1/portfolio/debate", payload, &out)
		if err != nil {
			writeUpstreamError(w, err, status)
			return
		}
		writeJSON(w, http.StatusOK, out)
		return
	}
	writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method_not_allowed"})
}

func jsonLooksValid(payload []byte) bool {
	if len(bytes.TrimSpace(payload)) == 0 {
		return false
	}
	first := bytes.TrimSpace(payload)[0]
	return first == '{' || first == '['
}
