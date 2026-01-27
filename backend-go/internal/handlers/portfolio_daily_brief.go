package handlers

import (
	"net/http"
	"net/url"
)

func (a *API) PortfolioDailyBrief(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	base := normalizeBase(q.Get("base"))
	window := normalizeWindow(q.Get("window"))
	period := normalizePeriod(q.Get("period"))

	values := url.Values{}
	if base != "" {
		values.Set("base", base)
	}
	if window != "" {
		values.Set("window", window)
	}
	if period != "" {
		values.Set("period", period)
	}
	path := "/api/v1/portfolio/daily-brief"
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}

	var out any
	timeout := a.cfg.PortfolioTimeout
	if timeout <= 0 {
		timeout = a.cfg.RequestTimeout
	}
	status, err := a.py.FetchJSONWithStatusTimeout(r.Context(), path, &out, timeout)
	if err != nil {
		writeUpstreamError(w, err, status)
		return
	}
	writeJSON(w, http.StatusOK, out)
}
