package services

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"macroquant-intel/backend-go/internal/config"
)

type Quote struct {
	Symbol     string  `json:"symbol"`
	Price      float64 `json:"price"`
	ChangePct  float64 `json:"change_pct"`
	Currency   string  `json:"currency"`
	UpdatedISO string  `json:"updated_iso"`
}

type QuotesClient struct {
	hc    *http.Client
	cache Cache
	ttl   time.Duration
	baseURL string
	mu    sync.Mutex
	last  map[string]Quote
	backoffUntil time.Time
}

func NewQuotesClient(cfg config.Config, cache Cache) *QuotesClient {
	return &QuotesClient{
		hc: &http.Client{
			Timeout: cfg.RequestTimeout,
		},
		cache: cache,
		ttl:   60 * time.Second,
		baseURL: cfg.PyBaseURL,
	}
}

type yahooQuoteResponse struct {
	QuoteResponse struct {
		Result []struct {
			Symbol                   string  `json:"symbol"`
			RegularMarketPrice       float64 `json:"regularMarketPrice"`
			RegularMarketChangePct   float64 `json:"regularMarketChangePercent"`
			RegularMarketTime        int64   `json:"regularMarketTime"`
			Currency                 string  `json:"currency"`
		} `json:"result"`
	} `json:"quoteResponse"`
}

func (c *QuotesClient) Fetch(ctx context.Context, symbols []string) (map[string]Quote, error) {
	if len(symbols) == 0 {
		return map[string]Quote{}, nil
	}
	now := time.Now()
	c.mu.Lock()
	if !c.backoffUntil.IsZero() && now.Before(c.backoffUntil) {
		cached := cloneQuotes(c.last)
		c.mu.Unlock()
		if len(cached) > 0 {
			return cached, nil
		}
	} else {
		c.mu.Unlock()
	}
	key := quotesCacheKey(symbols)
	if c.cache != nil {
		if b, ok := c.cache.Get(ctx, key); ok {
			var cached map[string]Quote
			if err := UnmarshalCache(b, &cached); err == nil {
				c.storeLast(cached)
				return cached, nil
			}
		}
	}

	base := strings.TrimRight(c.baseURL, "/")
	url := fmt.Sprintf("%s/quotes/latest", base)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	q := req.URL.Query()
	q.Set("assets", strings.Join(symbols, ","))
	req.URL.RawQuery = q.Encode()

	res, err := c.hc.Do(req)
	if err != nil {
		return c.staleOrError(err)
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		return c.staleOrError(fmt.Errorf("quotes service: %s", res.Status))
	}

	var payload struct {
		TsISO  string `json:"tsISO"`
		Quotes map[string]struct {
			Price      float64 `json:"price"`
			ChangePct  float64 `json:"change_pct"`
			UpdatedISO string  `json:"updated_iso"`
		} `json:"quotes"`
	}
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		return c.staleOrError(err)
	}

	out := make(map[string]Quote, len(payload.Quotes))
	for symbol, qv := range payload.Quotes {
		updated := qv.UpdatedISO
		if updated == "" {
			updated = payload.TsISO
		}
		out[symbol] = Quote{
			Symbol:     symbol,
			Price:      qv.Price,
			ChangePct:  qv.ChangePct,
			UpdatedISO: updated,
		}
	}

	c.storeLast(out)
	if c.cache != nil {
		if b, err := MarshalCache(out); err == nil {
			_ = c.cache.Set(ctx, key, b, c.ttl)
		}
	}
	return out, nil
}

func quotesCacheKey(symbols []string) string {
	joined := strings.Join(symbols, ",")
	sum := sha1.Sum([]byte(joined))
	return fmt.Sprintf("quotes:v1:%s", hex.EncodeToString(sum[:6]))
}

func (c *QuotesClient) storeLast(data map[string]Quote) {
	if len(data) == 0 {
		return
	}
	c.mu.Lock()
	c.last = cloneQuotes(data)
	c.mu.Unlock()
}

func (c *QuotesClient) staleOrError(err error) (map[string]Quote, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.last) == 0 {
		return map[string]Quote{}, err
	}
	return cloneQuotes(c.last), err
}

func (c *QuotesClient) setBackoff(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	until := time.Now().Add(d)
	if until.After(c.backoffUntil) {
		c.backoffUntil = until
	}
}

func cloneQuotes(data map[string]Quote) map[string]Quote {
	if len(data) == 0 {
		return map[string]Quote{}
	}
	out := make(map[string]Quote, len(data))
	for k, v := range data {
		out[k] = v
	}
	return out
}
