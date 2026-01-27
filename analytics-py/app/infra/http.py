from __future__ import annotations

import time

import httpx


def get_json(url: str, params: dict | None = None, headers: dict | None = None, timeout: float = 10.0, retries: int = 2):
    last_err = None
    for attempt in range(retries + 1):
        try:
            with httpx.Client(timeout=timeout) as client:
                res = client.get(url, params=params, headers=headers)
                res.raise_for_status()
                return res.json()
        except Exception as exc:
            last_err = exc
            time.sleep(0.3 * (attempt + 1))
    raise last_err


def get_text(url: str, params: dict | None = None, headers: dict | None = None, timeout: float = 10.0, retries: int = 2) -> str:
    last_err = None
    for attempt in range(retries + 1):
        try:
            with httpx.Client(timeout=timeout) as client:
                res = client.get(url, params=params, headers=headers)
                res.raise_for_status()
                return res.text
        except Exception as exc:
            last_err = exc
            time.sleep(0.3 * (attempt + 1))
    raise last_err
