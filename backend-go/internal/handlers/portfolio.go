package handlers

import (
	"net/http"
)

func (a *API) Portfolio(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	base := q.Get("base")
	horizon := q.Get("horizon")
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

	var out any
	timeout := a.cfg.PortfolioTimeout
	if timeout <= 0 {
		timeout = a.cfg.RequestTimeout
	}
	err := a.py.FetchJSONWithTimeout(r.Context(), path, &out, timeout)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, out)
}
