package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"macroquant-intel/backend-go/internal/config"
	"macroquant-intel/backend-go/internal/models"
)

var errRateLimited = errors.New("rate_limited")

var backoffSteps = []time.Duration{
	1 * time.Second,
	2 * time.Second,
	4 * time.Second,
	8 * time.Second,
	16 * time.Second,
	30 * time.Second,
}

type DerivativesClient struct {
	hc    *http.Client
	cache Cache
	ttl   time.Duration
}

func NewDerivativesClient(cfg config.Config, cache Cache) *DerivativesClient {
	return &DerivativesClient{
		hc: &http.Client{
			Timeout: cfg.RequestTimeout,
		},
		cache: cache,
		ttl:   cfg.CacheTTLDerv,
	}
}

func (c *DerivativesClient) Get(ctx context.Context, exchange string, symbol string) (models.DerivativesResponse, models.DerivativesHealth) {
	start := time.Now()
	exchange = normalizeExchange(exchange)
	symbol = strings.ToUpper(symbol)
	key := fmt.Sprintf("deriv:%s:%s", exchange, symbol)
	lastGoodKey := fmt.Sprintf("deriv:lastgood:%s:%s", exchange, symbol)

	if c.cache != nil {
		if b, ok := c.cache.Get(ctx, key); ok {
			var cached models.DerivativesResponse
			if err := UnmarshalCache(b, &cached); err == nil {
				return cached, models.DerivativesHealth{
					LatencyMs: int64(time.Since(start) / time.Millisecond),
					CacheHit:  true,
				}
			}
		}
	}

	resp, warn, err := c.fetch(ctx, exchange, symbol)
	health := models.DerivativesHealth{
		LatencyMs: int64(time.Since(start) / time.Millisecond),
		CacheHit:  false,
	}

	if err != nil {
		health.Error = err.Error()
		if c.cache != nil {
			if b, ok := c.cache.Get(ctx, lastGoodKey); ok {
				var cached models.DerivativesResponse
				if err := UnmarshalCache(b, &cached); err == nil {
					age := int64(0)
					if ts, parseErr := time.Parse(time.RFC3339, cached.Ts); parseErr == nil {
						age = int64(time.Since(ts).Seconds())
					}
					health.DegradedMode = true
					health.LastGoodAgeS = age
					return cached, health
				}
			}
		}
		return resp, health
	}

	if warn != "" {
		health.Error = warn
	}

	if c.cache != nil && resp.Ts != "" {
		if b, err := MarshalCache(resp); err == nil {
			_ = c.cache.Set(ctx, key, b, c.ttl)
			_ = c.cache.Set(ctx, lastGoodKey, b, 1*time.Hour)
		}
	}

	return resp, health
}

func normalizeExchange(exchange string) string {
	return "binance"
}

func (c *DerivativesClient) fetch(ctx context.Context, exchange string, symbol string) (models.DerivativesResponse, string, error) {
	return c.fetchBinance(ctx, symbol)
}

func (c *DerivativesClient) fetchBinance(ctx context.Context, symbol string) (models.DerivativesResponse, string, error) {
	fundingSeries, fundingLatest, fundErr := c.fetchBinanceFunding(ctx, symbol)
	oiSeries, oiLatest, oiErr := c.fetchBinanceOI(ctx, symbol)

	warn := ""
	if fundErr != nil || oiErr != nil {
		if errors.Is(fundErr, errRateLimited) || errors.Is(oiErr, errRateLimited) {
			return models.DerivativesResponse{}, "", errRateLimited
		}
		if fundErr != nil {
			warn = "data_missing_funding"
		} else if oiErr != nil {
			warn = "data_missing_oi"
		}
	}

	return buildDerivativesResponse("binance", symbol, fundingSeries, fundingLatest, oiSeries, oiLatest), warn, nil
}

func (c *DerivativesClient) fetchOKX(ctx context.Context, symbol string) (models.DerivativesResponse, string, error) {
	instID := okxInstID(symbol)
	fundingSeries, fundingLatest, fundErr := c.fetchOKXFunding(ctx, instID)
	oiSeries, oiLatest, oiErr := c.fetchOKXOI(ctx, instID)

	warn := ""
	if fundErr != nil || oiErr != nil {
		if errors.Is(fundErr, errRateLimited) || errors.Is(oiErr, errRateLimited) {
			return models.DerivativesResponse{}, "", errRateLimited
		}
		if fundErr != nil {
			warn = "data_missing_funding"
		} else if oiErr != nil {
			warn = "data_missing_oi"
		}
	}

	return buildDerivativesResponse("okx", symbol, fundingSeries, fundingLatest, oiSeries, oiLatest), warn, nil
}

func (c *DerivativesClient) fetchBybit(ctx context.Context, symbol string) (models.DerivativesResponse, string, error) {
	fundingSeries, fundingLatest, fundErr := c.fetchBybitFunding(ctx, symbol)
	oiSeries, oiLatest, oiErr := c.fetchBybitOI(ctx, symbol)

	warn := ""
	if fundErr != nil || oiErr != nil {
		if errors.Is(fundErr, errRateLimited) || errors.Is(oiErr, errRateLimited) {
			return models.DerivativesResponse{}, "", errRateLimited
		}
		if fundErr != nil {
			warn = "data_missing_funding"
		} else if oiErr != nil {
			warn = "data_missing_oi"
		}
	}

	return buildDerivativesResponse("bybit", symbol, fundingSeries, fundingLatest, oiSeries, oiLatest), warn, nil
}

func buildDerivativesResponse(exchange string, symbol string, fundingSeries []models.DerivativesPoint, fundingLatest float64, oiSeries []models.DerivativesPoint, oiLatest float64) models.DerivativesResponse {
	now := time.Now().UTC().Format(time.RFC3339)
	fundingZ := computeFundingZ(fundingSeries, fundingLatest)
	oiDelta := computeOIDelta(oiSeries, oiLatest)

	return models.DerivativesResponse{
		Ts:       now,
		Exchange: exchange,
		Symbol:   symbol,
		Funding: models.DerivativesSeries{
			Latest: fundingLatest,
			Series: fundingSeries,
		},
		OI: models.DerivativesSeries{
			Latest: oiLatest,
			Series: oiSeries,
		},
		Computed: models.DerivativesComputed{
			FundingZ:   fundingZ,
			OIDeltaPct: oiDelta,
		},
	}
}

func computeFundingZ(series []models.DerivativesPoint, latest float64) float64 {
	if len(series) < 2 {
		return 0
	}
	values := make([]float64, 0, len(series))
	for _, p := range series {
		values = append(values, p.V)
	}
	mean := mean(values)
	std := stddev(values, mean)
	eps := 1e-9
	z := (latest - mean) / math.Max(std, eps)
	if z > 5 {
		z = 5
	}
	if z < -5 {
		z = -5
	}
	return z
}

func computeOIDelta(series []models.DerivativesPoint, latest float64) float64 {
	if len(series) < 2 {
		return 0
	}
	sort.Slice(series, func(i, j int) bool { return series[i].T < series[j].T })
	target := time.Now().Add(-24 * time.Hour).UnixMilli()
	closest := series[0]
	closestDiff := int64(math.Abs(float64(series[0].T - target)))
	for _, p := range series[1:] {
		diff := int64(math.Abs(float64(p.T - target)))
		if diff < closestDiff {
			closest = p
			closestDiff = diff
		}
	}
	base := closest.V
	eps := 1e-9
	return (latest - base) / math.Max(base, eps) * 100
}

func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func stddev(values []float64, mean float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		d := v - mean
		sum += d * d
	}
	return math.Sqrt(sum / float64(len(values)))
}

func (c *DerivativesClient) fetchWithBackoff(ctx context.Context, url string, out any) error {
	var lastErr error
	for i, wait := range backoffSteps {
		status, err := c.doJSON(ctx, url, out)
		if err == nil {
			return nil
		}
		lastErr = err
		if status == http.StatusTooManyRequests {
			if i == len(backoffSteps)-1 {
				return errRateLimited
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	if lastErr != nil {
		return lastErr
	}
	return errors.New("request_failed")
}

func (c *DerivativesClient) doJSON(ctx context.Context, url string, out any) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	res, err := c.hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		return res.StatusCode, fmt.Errorf("status %d", res.StatusCode)
	}
	if out == nil {
		return res.StatusCode, nil
	}
	if err := json.NewDecoder(res.Body).Decode(out); err != nil {
		return res.StatusCode, err
	}
	return res.StatusCode, nil
}

func (c *DerivativesClient) fetchBinanceFunding(ctx context.Context, symbol string) ([]models.DerivativesPoint, float64, error) {
	url := fmt.Sprintf("https://fapi.binance.com/fapi/v1/fundingRate?symbol=%s&limit=1000", symbol)
	var raw []struct {
		FundingRate string `json:"fundingRate"`
		FundingTime int64  `json:"fundingTime"`
	}
	if err := c.fetchWithBackoff(ctx, url, &raw); err != nil {
		return nil, 0, err
	}
	points := make([]models.DerivativesPoint, 0, len(raw))
	for _, r := range raw {
		v, err := strconv.ParseFloat(r.FundingRate, 64)
		if err != nil {
			continue
		}
		points = append(points, models.DerivativesPoint{T: r.FundingTime, V: v})
	}
	points = filterByDays(points, 7)
	latest := 0.0
	if len(points) > 0 {
		latest = points[len(points)-1].V
	}
	return points, latest, nil
}

func (c *DerivativesClient) fetchBinanceOI(ctx context.Context, symbol string) ([]models.DerivativesPoint, float64, error) {
	histURL := fmt.Sprintf("https://fapi.binance.com/futures/data/openInterestHist?symbol=%s&period=5m&limit=500", symbol)
	var raw []struct {
		SumOpenInterest string `json:"sumOpenInterest"`
		Timestamp       int64  `json:"timestamp"`
	}
	if err := c.fetchWithBackoff(ctx, histURL, &raw); err != nil {
		return nil, 0, err
	}
	points := make([]models.DerivativesPoint, 0, len(raw))
	for _, r := range raw {
		v, err := strconv.ParseFloat(r.SumOpenInterest, 64)
		if err != nil {
			continue
		}
		points = append(points, models.DerivativesPoint{T: r.Timestamp, V: v})
	}
	latest := 0.0
	if len(points) > 0 {
		latest = points[len(points)-1].V
	}
	return points, latest, nil
}

func (c *DerivativesClient) fetchBybitFunding(ctx context.Context, symbol string) ([]models.DerivativesPoint, float64, error) {
	url := fmt.Sprintf("https://api.bybit.com/v5/market/history-fund-rate?category=linear&symbol=%s&limit=200", symbol)
	var raw struct {
		Result struct {
			List []struct {
				FundingRate          string `json:"fundingRate"`
				FundingRateTimestamp string `json:"fundingRateTimestamp"`
			} `json:"list"`
		} `json:"result"`
	}
	if err := c.fetchWithBackoff(ctx, url, &raw); err != nil {
		return nil, 0, err
	}
	points := make([]models.DerivativesPoint, 0, len(raw.Result.List))
	for _, r := range raw.Result.List {
		v, err := strconv.ParseFloat(r.FundingRate, 64)
		if err != nil {
			continue
		}
		ts, _ := strconv.ParseInt(r.FundingRateTimestamp, 10, 64)
		points = append(points, models.DerivativesPoint{T: ts, V: v})
	}
	points = filterByDays(points, 7)
	latest := 0.0
	if len(points) > 0 {
		latest = points[0].V
	}
	return points, latest, nil
}

func (c *DerivativesClient) fetchBybitOI(ctx context.Context, symbol string) ([]models.DerivativesPoint, float64, error) {
	url := fmt.Sprintf("https://api.bybit.com/v5/market/open-interest?category=linear&symbol=%s&intervalTime=5min&limit=50", symbol)
	var raw struct {
		Result struct {
			List []struct {
				OpenInterest string `json:"openInterest"`
				Timestamp    string `json:"timestamp"`
			} `json:"list"`
		} `json:"result"`
	}
	if err := c.fetchWithBackoff(ctx, url, &raw); err != nil {
		return nil, 0, err
	}
	points := make([]models.DerivativesPoint, 0, len(raw.Result.List))
	for _, r := range raw.Result.List {
		v, err := strconv.ParseFloat(r.OpenInterest, 64)
		if err != nil {
			continue
		}
		ts, _ := strconv.ParseInt(r.Timestamp, 10, 64)
		points = append(points, models.DerivativesPoint{T: ts, V: v})
	}
	sort.Slice(points, func(i, j int) bool { return points[i].T < points[j].T })
	latest := 0.0
	if len(points) > 0 {
		latest = points[len(points)-1].V
	}
	return points, latest, nil
}

func (c *DerivativesClient) fetchOKXFunding(ctx context.Context, instID string) ([]models.DerivativesPoint, float64, error) {
	url := fmt.Sprintf("https://www.okx.com/api/v5/public/funding-rate-history?instId=%s", instID)
	var raw struct {
		Data []struct {
			FundingRate string `json:"fundingRate"`
			Ts          string `json:"ts"`
		} `json:"data"`
	}
	if err := c.fetchWithBackoff(ctx, url, &raw); err != nil {
		return nil, 0, err
	}
	points := make([]models.DerivativesPoint, 0, len(raw.Data))
	for _, r := range raw.Data {
		v, err := strconv.ParseFloat(r.FundingRate, 64)
		if err != nil {
			continue
		}
		ts, _ := strconv.ParseInt(r.Ts, 10, 64)
		points = append(points, models.DerivativesPoint{T: ts, V: v})
	}
	points = filterByDays(points, 7)
	latest := 0.0
	if len(points) > 0 {
		latest = points[0].V
	}
	return points, latest, nil
}

func (c *DerivativesClient) fetchOKXOI(ctx context.Context, instID string) ([]models.DerivativesPoint, float64, error) {
	url := fmt.Sprintf("https://www.okx.com/api/v5/public/open-interest?instType=SWAP&instId=%s", instID)
	var raw struct {
		Data []struct {
			OI string `json:"oi"`
			Ts string `json:"ts"`
		} `json:"data"`
	}
	if err := c.fetchWithBackoff(ctx, url, &raw); err != nil {
		return nil, 0, err
	}
	points := make([]models.DerivativesPoint, 0, len(raw.Data))
	for _, r := range raw.Data {
		v, err := strconv.ParseFloat(r.OI, 64)
		if err != nil {
			continue
		}
		ts, _ := strconv.ParseInt(r.Ts, 10, 64)
		points = append(points, models.DerivativesPoint{T: ts, V: v})
	}
	latest := 0.0
	if len(points) > 0 {
		latest = points[len(points)-1].V
	}
	return points, latest, nil
}

func filterByDays(points []models.DerivativesPoint, days int) []models.DerivativesPoint {
	if len(points) == 0 {
		return points
	}
	cutoff := time.Now().Add(time.Duration(-days) * 24 * time.Hour).UnixMilli()
	out := make([]models.DerivativesPoint, 0, len(points))
	for _, p := range points {
		if p.T >= cutoff {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].T < out[j].T })
	return out
}

func okxInstID(symbol string) string {
	if strings.Contains(symbol, "-") {
		if strings.HasSuffix(symbol, "-SWAP") {
			return symbol
		}
		return symbol + "-SWAP"
	}
	if strings.HasSuffix(symbol, "USDT") {
		base := strings.TrimSuffix(symbol, "USDT")
		return fmt.Sprintf("%s-USDT-SWAP", base)
	}
	return symbol + "-USDT-SWAP"
}
