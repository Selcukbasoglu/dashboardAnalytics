package handlers

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"macroquant-intel/backend-go/internal/config"
	"macroquant-intel/backend-go/internal/models"
	"macroquant-intel/backend-go/internal/services"
)

type API struct {
	cfg   config.Config
	cache services.Cache
	py    *services.PythonClient
	deriv *services.DerivativesClient
	quotes *services.QuotesClient
}

func New(cfg config.Config, cache services.Cache, py *services.PythonClient) *API {
	return &API{
		cfg:   cfg,
		cache: cache,
		py:    py,
		deriv: services.NewDerivativesClient(cfg, cache),
		quotes: services.NewQuotesClient(cfg, cache),
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (a *API) getIntel(ctx context.Context, timeframe string, newsTimespan string, watch []string) (models.IntelResponse, bool) {
	if watch == nil {
		watch = []string{}
	}
	key := intelCacheKey(timeframe, newsTimespan, watch)
	if a.cache != nil {
		if b, ok := a.cache.Get(ctx, key); ok {
			var cached models.IntelResponse
			if err := services.UnmarshalCache(b, &cached); err == nil {
				emptyFeed := len(cached.EventFeed.Regional) == 0 &&
					len(cached.EventFeed.Company) == 0 &&
					len(cached.EventFeed.Sector) == 0 &&
					len(cached.EventFeed.Personal) == 0
				if !emptyFeed {
					return cached, true
				}
			}
		}
	}

	resp, err := a.py.RunIntel(ctx, models.IntelRequest{
		Timeframe:    timeframe,
		NewsTimespan: newsTimespan,
		Watchlist:    watch,
	})
	if err != nil {
		return resp, false
	}

	if a.cache != nil {
		emptyFeed := len(resp.EventFeed.Regional) == 0 &&
			len(resp.EventFeed.Company) == 0 &&
			len(resp.EventFeed.Sector) == 0 &&
			len(resp.EventFeed.Personal) == 0
		if emptyFeed {
			return resp, false
		}
		if b, err := services.MarshalCache(resp); err == nil {
			_ = a.cache.Set(ctx, key, b, a.cfg.CacheTTLIntel)
		}
	}
	return resp, false
}

func (a *API) getIntelStream(ctx context.Context, timeframe string, newsTimespan string, watch []string) (models.IntelResponse, error) {
	if watch == nil {
		watch = []string{}
	}
	key := intelCacheKey(timeframe, newsTimespan, watch)
	if a.cache != nil {
		if b, ok := a.cache.Get(ctx, key); ok {
			var cached models.IntelResponse
			if err := services.UnmarshalCache(b, &cached); err == nil {
				return cached, nil
			}
		}
	}

	resp, err := a.py.RunIntel(ctx, models.IntelRequest{
		Timeframe:    timeframe,
		NewsTimespan: newsTimespan,
		Watchlist:    watch,
	})
	if err != nil {
		return resp, err
	}

	if a.cache != nil {
		if b, err := services.MarshalCache(resp); err == nil {
			_ = a.cache.Set(ctx, key, b, a.cfg.CacheTTLIntel)
		}
	}
	return resp, nil
}

func intelCacheKey(timeframe string, newsTimespan string, watch []string) string {
	safeWatch := make([]string, 0, len(watch))
	for _, w := range watch {
		if w == "" {
			continue
		}
		safeWatch = append(safeWatch, strings.ToLower(w))
	}
	sort.Strings(safeWatch)
	sum := sha1.Sum([]byte(strings.Join(safeWatch, ",")))
	return fmt.Sprintf("intel:v1:%s:%s:%s", timeframe, newsTimespan, hex.EncodeToString(sum[:8]))
}

func parseWatchlist(raw string, max int) []string {
	if raw == "" {
		return []string{}
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
		if len(out) >= max {
			break
		}
	}
	return out
}

func parseIntParam(v string, def int, min int, max int) int {
	if v == "" {
		return def
	}
	var out int
	_, err := fmt.Sscanf(v, "%d", &out)
	if err != nil {
		return def
	}
	if out < min {
		return min
	}
	if out > max {
		return max
	}
	return out
}

func nowISO() string {
	return time.Now().UTC().Format(time.RFC3339)
}
