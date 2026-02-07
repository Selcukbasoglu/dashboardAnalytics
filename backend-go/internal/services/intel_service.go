package services

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"macroquant-intel/backend-go/internal/config"
	"macroquant-intel/backend-go/internal/models"
)

type SnapshotMeta struct {
	Source    string
	Stale     bool
	Err       string
	FetchedAt string
}

type IntelSnapshot struct {
	Resp models.IntelResponse
	Meta SnapshotMeta
}

type intelCacheEntry struct {
	FetchedAt string               `json:"fetched_at"`
	Resp      models.IntelResponse `json:"resp"`
}

type intelTopic struct {
	subs     map[chan IntelSnapshot]struct{}
	cancel   context.CancelFunc
	interval time.Duration
	last     *IntelSnapshot
}

type IntelService struct {
	cfg        config.Config
	cache      Cache
	py         *PythonClient
	mu         sync.Mutex
	topics     map[string]*intelTopic
	refreshing map[string]bool
}

func NewIntelService(cfg config.Config, cache Cache, py *PythonClient) *IntelService {
	return &IntelService{
		cfg:        cfg,
		cache:      cache,
		py:         py,
		topics:     make(map[string]*intelTopic),
		refreshing: make(map[string]bool),
	}
}

func (s *IntelService) Subscribe(ctx context.Context, timeframe string, newsTimespan string, watch []string, interval time.Duration) (<-chan IntelSnapshot, func()) {
	key := intelCacheKey(timeframe, newsTimespan, watch)
	ch := make(chan IntelSnapshot, 1)
	var once sync.Once

	s.mu.Lock()
	topic := s.topics[key]
	if topic == nil {
		bgCtx, cancel := context.WithCancel(context.Background())
		topic = &intelTopic{subs: make(map[chan IntelSnapshot]struct{}), cancel: cancel, interval: interval}
		s.topics[key] = topic
		go s.runTopic(bgCtx, key, timeframe, newsTimespan, watch, interval)
	}
	topic.subs[ch] = struct{}{}
	last := topic.last
	s.mu.Unlock()

	if last != nil {
		select {
		case ch <- *last:
		default:
		}
	}

	unsubscribe := func() {
		once.Do(func() {
			s.mu.Lock()
			if t := s.topics[key]; t != nil {
				delete(t.subs, ch)
				empty := len(t.subs) == 0
				if empty {
					t.cancel()
					delete(s.topics, key)
				}
			}
			s.mu.Unlock()
			close(ch)
		})
	}

	go func() {
		<-ctx.Done()
		unsubscribe()
	}()

	return ch, unsubscribe
}

func (s *IntelService) GetSnapshot(ctx context.Context, timeframe string, newsTimespan string, watch []string) (models.IntelResponse, SnapshotMeta, error) {
	if watch == nil {
		watch = []string{}
	}
	key := intelCacheKey(timeframe, newsTimespan, watch)
	cached, fetchedAt, ok := s.getCached(ctx, key)
	if ok {
		age := time.Since(fetchedAt)
		if age <= s.cfg.CacheTTLIntel {
			return cached, SnapshotMeta{
				Source:    "cache",
				Stale:     false,
				FetchedAt: fetchedAt.UTC().Format(time.RFC3339),
			}, nil
		}
		if age <= s.intelHardTTL() {
			s.refreshAsync(key, timeframe, newsTimespan, watch)
			return cached, SnapshotMeta{
				Source:    "stale_cache",
				Stale:     true,
				FetchedAt: fetchedAt.UTC().Format(time.RFC3339),
			}, nil
		}
	}

	resp, meta, err := s.fetchAndCache(ctx, key, timeframe, newsTimespan, watch)
	if err == nil {
		return resp, meta, nil
	}
	if ok {
		meta = SnapshotMeta{
			Source:    "stale_cache",
			Stale:     true,
			Err:       err.Error(),
			FetchedAt: fetchedAt.UTC().Format(time.RFC3339),
		}
		return cached, meta, nil
	}
	return resp, SnapshotMeta{Source: "error", Err: err.Error()}, err
}

func (s *IntelService) runTopic(ctx context.Context, key string, timeframe string, newsTimespan string, watch []string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	publish := func() {
		reqCtx, cancel := context.WithTimeout(context.Background(), s.intelRequestTimeout())
		defer cancel()
		resp, meta, err := s.GetSnapshot(reqCtx, timeframe, newsTimespan, watch)
		if err != nil && resp.TsISO == "" {
			return
		}
		snap := IntelSnapshot{Resp: resp, Meta: meta}
		s.publish(key, snap)
	}

	publish()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			publish()
		}
	}
}

func (s *IntelService) publish(key string, snap IntelSnapshot) {
	s.mu.Lock()
	topic := s.topics[key]
	if topic != nil {
		topic.last = &snap
		for ch := range topic.subs {
			select {
			case ch <- snap:
			default:
			}
		}
	}
	s.mu.Unlock()
}

func (s *IntelService) refreshAsync(key string, timeframe string, newsTimespan string, watch []string) {
	s.mu.Lock()
	if s.refreshing[key] {
		s.mu.Unlock()
		return
	}
	s.refreshing[key] = true
	s.mu.Unlock()

	go func() {
		defer func() {
			s.mu.Lock()
			delete(s.refreshing, key)
			s.mu.Unlock()
		}()

		ctx, cancel := context.WithTimeout(context.Background(), s.intelRequestTimeout())
		defer cancel()
		resp, meta, err := s.fetchAndCache(ctx, key, timeframe, newsTimespan, watch)
		if err != nil || resp.TsISO == "" {
			return
		}
		s.publish(key, IntelSnapshot{Resp: resp, Meta: meta})
	}()
}

func (s *IntelService) fetchAndCache(ctx context.Context, key string, timeframe string, newsTimespan string, watch []string) (models.IntelResponse, SnapshotMeta, error) {
	resp, err := s.py.RunIntel(ctx, models.IntelRequest{
		Timeframe:    timeframe,
		NewsTimespan: newsTimespan,
		Watchlist:    watch,
	})
	if err != nil {
		return resp, SnapshotMeta{Source: "error", Err: err.Error()}, err
	}

	now := time.Now().UTC()
	if s.cache != nil && hasCacheableIntel(resp) {
		entry := intelCacheEntry{
			FetchedAt: now.Format(time.RFC3339),
			Resp:      resp,
		}
		if b, err := MarshalCache(entry); err == nil {
			_ = s.cache.Set(ctx, key, b, s.intelHardTTL())
		}
	}

	return resp, SnapshotMeta{
		Source:    "fresh",
		Stale:     false,
		FetchedAt: now.Format(time.RFC3339),
	}, nil
}

func (s *IntelService) getCached(ctx context.Context, key string) (models.IntelResponse, time.Time, bool) {
	if s.cache == nil {
		return models.IntelResponse{}, time.Time{}, false
	}
	b, ok := s.cache.Get(ctx, key)
	if !ok {
		return models.IntelResponse{}, time.Time{}, false
	}
	var entry intelCacheEntry
	if err := UnmarshalCache(b, &entry); err != nil {
		return models.IntelResponse{}, time.Time{}, false
	}
	if !hasCacheableIntel(entry.Resp) {
		return models.IntelResponse{}, time.Time{}, false
	}
	fetchedAt, err := time.Parse(time.RFC3339, entry.FetchedAt)
	if err != nil {
		return models.IntelResponse{}, time.Time{}, false
	}
	return entry.Resp, fetchedAt, true
}

func hasCacheableIntel(resp models.IntelResponse) bool {
	return strings.TrimSpace(resp.TsISO) != ""
}

func (s *IntelService) intelHardTTL() time.Duration {
	if s.cfg.CacheTTLIntelHard <= 0 {
		return s.cfg.CacheTTLIntel
	}
	if s.cfg.CacheTTLIntelHard < s.cfg.CacheTTLIntel {
		return s.cfg.CacheTTLIntel
	}
	return s.cfg.CacheTTLIntelHard
}

func (s *IntelService) intelRequestTimeout() time.Duration {
	if s.cfg.IntelTimeout > 0 {
		return s.cfg.IntelTimeout
	}
	return s.cfg.RequestTimeout
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
	return fmt.Sprintf("intel:v2:%s:%s:%s", timeframe, newsTimespan, hex.EncodeToString(sum[:8]))
}
