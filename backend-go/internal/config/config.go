package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port              string
	PyBaseURL         string
	RedisURL          string
	CacheTTLIntel     time.Duration
	CacheTTLIntelHard time.Duration
	CacheTTLNews      time.Duration
	CacheTTLMarket    time.Duration
	CacheTTLDerv      time.Duration
	RequestTimeout    time.Duration
	IntelTimeout      time.Duration
	PortfolioTimeout  time.Duration
	RateLimitPerMin   int
	CircuitFailLimit  int
	CircuitCooldown   time.Duration
	MaxWatchlist      int
}

func Load() Config {
	return Config{
		Port:              getEnv("PORT", "8080"),
		PyBaseURL:         getEnv("PY_INTEL_BASE_URL", "http://localhost:8001"),
		RedisURL:          getEnv("REDIS_URL", "redis://localhost:6379"),
		CacheTTLIntel:     getEnvDuration("CACHE_TTL_INTEL", 30*time.Second),
		CacheTTLIntelHard: getEnvDuration("CACHE_TTL_INTEL_HARD", 120*time.Second),
		CacheTTLNews:      getEnvDuration("CACHE_TTL_NEWS", 60*time.Second),
		CacheTTLMarket:    getEnvDuration("CACHE_TTL_MARKET", 20*time.Second),
		CacheTTLDerv:      getEnvDuration("CACHE_TTL_DERIV", 90*time.Second),
		RequestTimeout:    getEnvDuration("PY_REQUEST_TIMEOUT", 12*time.Second),
		IntelTimeout:      getEnvDuration("PY_INTEL_TIMEOUT", 90*time.Second),
		PortfolioTimeout:  getEnvDuration("PY_PORTFOLIO_TIMEOUT", 75*time.Second),
		RateLimitPerMin:   getEnvInt("RATE_LIMIT_PER_MIN", 120),
		CircuitFailLimit:  getEnvInt("CIRCUIT_FAIL_LIMIT", 3),
		CircuitCooldown:   getEnvDuration("CIRCUIT_COOLDOWN", 20*time.Second),
		MaxWatchlist:      getEnvInt("MAX_WATCHLIST", 30),
	}
}

func getEnv(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}

func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return i
}

func getEnvDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return time.Duration(i) * time.Second
}
