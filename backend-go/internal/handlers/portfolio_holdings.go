package handlers

import (
	"io"
	"net/http"
	"net/url"
)

func (a *API) PortfolioHoldings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		var out any
		status, err := a.py.FetchJSONWithStatus(r.Context(), "/api/v1/portfolio/holdings", &out)
		if err != nil {
			writeUpstreamError(w, err, status)
			return
		}
		writeJSON(w, http.StatusOK, out)
		return
	case http.MethodPost:
		r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
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
		status, err := a.py.PostJSONWithStatus(r.Context(), "/api/v1/portfolio/holdings", payload, &out)
		if err != nil {
			writeUpstreamError(w, err, status)
			return
		}
		writeJSON(w, http.StatusOK, out)
		return
	case http.MethodDelete:
		q := r.URL.Query()
		symbol := q.Get("symbol")
		values := url.Values{}
		if symbol != "" {
			values.Set("symbol", symbol)
		}
		path := "/api/v1/portfolio/holdings"
		if encoded := values.Encode(); encoded != "" {
			path += "?" + encoded
		}
		var out any
		status, err := a.py.DeleteJSONWithStatus(r.Context(), path, &out)
		if err != nil {
			writeUpstreamError(w, err, status)
			return
		}
		writeJSON(w, http.StatusOK, out)
		return
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method_not_allowed"})
	}
}
