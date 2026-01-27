# MacroQuant Intel

MacroQuant Intel is a 3-service stack:
- Frontend: Next.js dashboard (consumes Go API)
- Backend: Go API gateway (`/api/v1/*`)
- Analytics: Python FastAPI engine (data collection + scoring)

## Repo Layout
- `frontend/`: Next.js app
- `backend-go/`: Go API gateway
- `analytics-py/`: Python analytics service
- `docker-compose.yml`: Redis

## Local Run
1) Redis
```
docker-compose up -d redis
```

2) Env (dev)
```
cp .env.example .env.local
# Fill in keys as needed (do not commit .env.local)
```

3) Analytics
```
cd analytics-py
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload
```

4) Backend
```
cd backend-go
go run ./cmd/api
```

5) Frontend
```
cd frontend
npm i
npm run dev
```

Frontend talks to Go at `http://localhost:8080/api/v1/*`.

## Config + Storage
- Default DB: SQLite at `analytics-py/dev.db` (override with `DATABASE_URL`).
- Config knobs live in `analytics-py/config.yaml` (override with `CONFIG_PATH` + env vars).
- Retention is enforced on ingest to keep ~1 week of data.

## Person Impact (News)
- PERSONAL/person-group news items now get a `person_event` object with stance, channels, bias, and impact scores.
- The rules live in `analytics-py/app/engine/person_impact.py` and are fully deterministic (no LLM).
- `impact_potential` feeds into news ranking alongside relevance/quality.
- Rank weights are configurable via `news_rank_weights` in `analytics-py/config.yaml`.
- No Trump special-casing is applied; all groups are handled uniformly.

## Optional Market Data Fallback Keys
Add a `.env.local` (or `.env`) file at the repo root:
```
FINNHUB_API_KEY=...
TWELVEDATA_API_KEY=...
OPENAI_API_KEY=
OPENAI_MODEL=
ENABLE_OPENAI_SUMMARY=false
PY_INTEL_BASE_URL=http://localhost:8001
DATABASE_URL=sqlite:///./dev.db
NEXT_PUBLIC_API_BASE=http://localhost:8080
```
If these are unset, fallback providers are disabled and Yahoo remains the only source.

## Seed / Mock Mode
Generate demo data without API keys (48h of 15m bars + 20 event clusters):
```
cd analytics-py
PYTHONPATH=. python scripts/seed_mock.py
```

## Optional Postgres
For now, SQLite is the supported local DB. If you need Postgres, add a driver and adapt the SQL upserts
(`INSERT OR IGNORE` -> `ON CONFLICT DO NOTHING`) plus connection handling.
