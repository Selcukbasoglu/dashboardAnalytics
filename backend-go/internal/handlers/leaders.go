package handlers

import (
	"fmt"
	"net/http"
	"strings"

	"macroquant-intel/backend-go/internal/models"
	"macroquant-intel/backend-go/internal/services"
)

func (a *API) Leaders(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	category := strings.TrimSpace(q.Get("category"))
	if category == "" {
		category = "states"
	}
	page := parseIntParam(q.Get("page"), 1, 1, 500)
	pageSize := parseIntParam(q.Get("pageSize"), 5, 1, 50)
	cacheKey := "leaders:v1:" + strings.ToLower(category) + ":" + fmt.Sprintf("%d:%d", page, pageSize)
	if a.cache != nil {
		if b, ok := a.cache.Get(r.Context(), cacheKey); ok {
			var cached models.LeadersResponse
			if err := services.UnmarshalCache(b, &cached); err == nil {
				writeJSON(w, http.StatusOK, cached)
				return
			}
		}
	}

	ctx, cancel := timeboxed(r, a.cfg.RequestTimeout)
	defer cancel()
	resp, _ := a.getIntel(ctx, "1h", "6h", nil)

	var items []models.NewsItem
	for _, g := range resp.Leaders {
		if strings.EqualFold(g.Key, category) {
			items = g.Items
			break
		}
	}
	if items == nil {
		items = []models.NewsItem{}
	}

	total := len(items)
	start := (page - 1) * pageSize
	if start > total {
		start = total
	}
	end := start + pageSize
	if end > total {
		end = total
	}
	paged := []models.NewsItem{}
	if start < end {
		paged = items[start:end]
	}

	out := models.LeadersResponse{
		TsISO:    nowISO(),
		Category: category,
		Page:     page,
		PageSize: pageSize,
		Total:    total,
		Items:    paged,
	}
	if a.cache != nil {
		if b, err := services.MarshalCache(out); err == nil {
			_ = a.cache.Set(r.Context(), cacheKey, b, a.cfg.CacheTTLNews)
		}
	}
	writeJSON(w, http.StatusOK, out)
}
