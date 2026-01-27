package handlers

import (
	"fmt"
	"net/http"
)

func (a *API) ForecastLatest(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	tf := q.Get("tf")
	target := q.Get("target")
	if tf == "" || target == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "tf and target required"})
		return
	}
	ctx, cancel := timeboxed(r, a.cfg.RequestTimeout)
	defer cancel()

	var out map[string]any
	path := fmt.Sprintf("/forecasts/latest?tf=%s&target=%s", tf, target)
	if err := a.py.FetchJSON(ctx, path, &out); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *API) ForecastMetrics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := timeboxed(r, a.cfg.RequestTimeout)
	defer cancel()

	var out map[string]any
	if err := a.py.FetchJSON(ctx, "/forecasts/metrics", &out); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *API) EventsLatest(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	hours := q.Get("hours")
	path := "/events/latest"
	if hours != "" {
		path = fmt.Sprintf("/events/latest?hours=%s", hours)
	}
	ctx, cancel := timeboxed(r, a.cfg.RequestTimeout)
	defer cancel()

	var out map[string]any
	if err := a.py.FetchJSON(ctx, path, &out); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, out)
}
