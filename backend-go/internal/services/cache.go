package services

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"macroquant-intel/backend-go/internal/config"
)

type Cache interface {
	Get(ctx context.Context, key string) ([]byte, bool)
	Set(ctx context.Context, key string, val []byte, ttl time.Duration) error
}

type RedisCache struct {
	client *redis.Client
}

type MemoryCache struct {
	mu    sync.Mutex
	items map[string]memItem
}

type memItem struct {
	val   []byte
	exp   time.Time
}

func NewCache(cfg config.Config) Cache {
	opt, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return NewMemoryCache()
	}
	client := redis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return NewMemoryCache()
	}
	return &RedisCache{client: client}
}

func NewMemoryCache() *MemoryCache {
	return &MemoryCache{items: make(map[string]memItem)}
}

func (r *RedisCache) Get(ctx context.Context, key string) ([]byte, bool) {
	b, err := r.client.Get(ctx, key).Bytes()
	if err != nil {
		return nil, false
	}
	return b, true
}

func (r *RedisCache) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	return r.client.Set(ctx, key, val, ttl).Err()
}

func (m *MemoryCache) Get(_ context.Context, key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	it, ok := m.items[key]
	if !ok {
		return nil, false
	}
	if !it.exp.IsZero() && time.Now().After(it.exp) {
		delete(m.items, key)
		return nil, false
	}
	return it.val, true
}

func (m *MemoryCache) Set(_ context.Context, key string, val []byte, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	exp := time.Time{}
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	m.items[key] = memItem{val: val, exp: exp}
	return nil
}

func MarshalCache(v any) ([]byte, error) {
	return json.Marshal(v)
}

func UnmarshalCache(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
