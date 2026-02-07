package http

import (
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (s *statusWriter) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, status: 200}
		start := time.Now()
		next.ServeHTTP(sw, r)
		dur := time.Since(start)
		log.Printf("%s %s %d %s", r.Method, r.URL.Path, sw.status, dur)
	})
}

func withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("panic: %v", rec)
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error":"internal"}`))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func withRateLimit(perMin int) func(http.Handler) http.Handler {
	if perMin <= 0 {
		perMin = 120
	}
	lim := newLimiter(perMin)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !lim.Allow(clientIP(r)) {
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"error":"rate_limited"}`))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

type limiter struct {
	mu       sync.Mutex
	perMin   int
	buckets  map[string]*bucket
	lastCleanup time.Time
}

type bucket struct {
	count int
	reset time.Time
}

func newLimiter(perMin int) *limiter {
	return &limiter{perMin: perMin, buckets: make(map[string]*bucket)}
}

func (l *limiter) Allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	b, ok := l.buckets[ip]
	now := time.Now()
	if l.lastCleanup.IsZero() || now.Sub(l.lastCleanup) > time.Minute {
		for key, bucket := range l.buckets {
			if now.After(bucket.reset) {
				delete(l.buckets, key)
			}
		}
		l.lastCleanup = now
	}
	if !ok || now.After(b.reset) {
		l.buckets[ip] = &bucket{count: 1, reset: now.Add(time.Minute)}
		return true
	}

	if b.count >= l.perMin {
		return false
	}
	b.count++
	return true
}

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		if len(parts) > 0 {
			return strings.TrimSpace(parts[0])
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}
