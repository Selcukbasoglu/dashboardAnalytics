type Entry<T> = { value: T; exp: number };
const mem = new Map<string, Entry<any>>();

export async function cacheGet<T>(key: string): Promise<T | null> {
  const hit = mem.get(key);
  if (!hit) return null;
  if (Date.now() > hit.exp) {
    mem.delete(key);
    return null;
  }
  return hit.value as T;
}

export async function cacheSet<T>(key: string, value: T, ttlSeconds: number) {
  mem.set(key, { value, exp: Date.now() + ttlSeconds * 1000 });
}
