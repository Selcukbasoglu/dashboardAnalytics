from __future__ import annotations

import json
import time
from dataclasses import dataclass
from threading import RLock

import redis
from cachetools import TTLCache


@dataclass
class Cache:
    redis_client: redis.Redis | None
    memory: TTLCache
    memory_by_ttl: dict[int, TTLCache]
    key_ttl_index: dict[str, int]
    lock: RLock

    def get(self, key: str):
        if self.redis_client is not None:
            try:
                data = self.redis_client.get(key)
                if data:
                    return json.loads(data)
            except Exception:
                pass
        with self.lock:
            ttl = self.key_ttl_index.get(key)
            if ttl is not None:
                cache = self.memory_by_ttl.get(ttl)
                if cache is not None and key in cache:
                    return cache.get(key)
            for ttl, cache in self.memory_by_ttl.items():
                if key in cache:
                    self.key_ttl_index[key] = ttl
                    return cache.get(key)
        return None

    def set(self, key: str, value, ttl_seconds: int):
        if self.redis_client is not None:
            try:
                self.redis_client.setex(key, ttl_seconds, json.dumps(value))
            except Exception:
                pass
        with self.lock:
            cache = self.memory_by_ttl.get(ttl_seconds)
            if cache is None:
                cache = TTLCache(maxsize=512, ttl=ttl_seconds)
                self.memory_by_ttl[ttl_seconds] = cache
            cache[key] = value
            self.key_ttl_index[key] = ttl_seconds


def init_cache(redis_url: str, ttl_seconds: int) -> Cache:
    memory = TTLCache(maxsize=512, ttl=ttl_seconds)
    memory_by_ttl = {ttl_seconds: memory}
    key_ttl_index: dict[str, int] = {}
    lock = RLock()
    client = None
    try:
        client = redis.from_url(redis_url, socket_timeout=1)
        client.ping()
    except Exception:
        client = None
    return Cache(
        redis_client=client,
        memory=memory,
        memory_by_ttl=memory_by_ttl,
        key_ttl_index=key_ttl_index,
        lock=lock,
    )


def cache_key(*parts: str) -> str:
    return ":".join([p for p in parts if p])


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
