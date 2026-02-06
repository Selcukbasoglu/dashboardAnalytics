package handlers

import (
	"fmt"
	"net/http"
	"strings"

	"macroquant-intel/backend-go/internal/models"
	"macroquant-intel/backend-go/internal/services"
)

func (a *API) News(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	page := parseIntParam(q.Get("page"), 1, 1, 500)
	pageSize := parseIntParam(q.Get("pageSize"), 5, 1, 50)
	filter := strings.TrimSpace(q.Get("filter"))
	searchText := strings.TrimSpace(q.Get("q"))
	newsTimespan := q.Get("newsTimespan")
	if newsTimespan == "" {
		newsTimespan = "6h"
	}
	cacheKey := "news:v1:" + newsTimespan + ":" + strings.ToLower(filter) + ":" + strings.ToLower(searchText) + ":" + fmt.Sprintf("%d:%d", page, pageSize)
	if a.cache != nil {
		if b, ok := a.cache.Get(r.Context(), cacheKey); ok {
			var cached models.NewsPageResponse
			if err := services.UnmarshalCache(b, &cached); err == nil {
				writeJSON(w, http.StatusOK, cached)
				return
			}
		}
	}

	ctx, cancel := timeboxed(r, a.cfg.RequestTimeout)
	defer cancel()
	resp, _, err := a.intel.GetSnapshot(ctx, "1h", newsTimespan, nil)
	items := resp.TopNews
	if err != nil || resp.TsISO == "" {
		items = []models.NewsItem{}
	}
	items = applyNewsFilter(items, filter)
	items = applyNewsSearch(items, searchText)

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

	out := models.NewsPageResponse{
		TsISO:    nowISO(),
		Page:     page,
		PageSize: pageSize,
		Total:    total,
		Filter:   filter,
		Items:    paged,
		Timespan: newsTimespan,
	}
	if a.cache != nil {
		if b, err := services.MarshalCache(out); err == nil {
			_ = a.cache.Set(r.Context(), cacheKey, b, a.cfg.CacheTTLNews)
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func applyNewsFilter(items []models.NewsItem, filter string) []models.NewsItem {
	if filter == "" || strings.EqualFold(filter, "all") || strings.EqualFold(filter, "hepsi") {
		return items
	}
	wanted := strings.ToLower(filter)
	out := make([]models.NewsItem, 0, len(items))
	for _, it := range items {
		for _, t := range it.Tags {
			if strings.ToLower(t) == wanted {
				out = append(out, it)
				break
			}
		}
	}
	return out
}

func applyNewsSearch(items []models.NewsItem, query string) []models.NewsItem {
	trimmed := strings.TrimSpace(strings.ToLower(query))
	if trimmed == "" {
		return items
	}
	tokens := strings.Fields(trimmed)
	out := make([]models.NewsItem, 0, len(items))
	for _, it := range items {
		text := strings.ToLower(it.Title + " " + it.ShortSummary + " " + strings.Join(it.Tags, " "))
		if text == "" {
			continue
		}
		if strings.Contains(text, trimmed) {
			out = append(out, it)
			continue
		}
		matchAll := true
		for _, tok := range tokens {
			if len(tok) < 2 {
				continue
			}
			if !strings.Contains(text, tok) {
				matchAll = false
				break
			}
		}
		if matchAll {
			out = append(out, it)
		}
	}
	return out
}
