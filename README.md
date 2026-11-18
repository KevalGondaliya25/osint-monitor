# OSINT Monitor

A self-hosted news and forum monitoring pipeline. It crawls news sites (BBC, Al Jazeera, Reuters, AP, Google News, BleepingComputer, The Hacker News), Hacker News, the BleepingComputer forums and public Telegram channels, flags items that match your keywords, extracts entities and indicators (CVEs, IPs, emails), and serves everything through a FastAPI search API and a live dashboard.

**Stack:** Python 3.12 · asyncio · httpx · Playwright · BeautifulSoup · FastAPI · Kafka · PostgreSQL · MinIO · Docker

![dashboard](docs/dashboard.png)

## Architecture

```
                 ┌──────────────┐  POST /api/crawl   ┌────────────────┐
  Browser ─────▶ │  FastAPI     │ ─────────────────▶ │ Kafka          │
  dashboard      │  (api)       │                    │  osint.jobs    │
                 └──────┬───────┘                    └───────┬────────┘
                        │ reads                              │
                        ▼                                    ▼
                 ┌──────────────┐                    ┌────────────────┐
                 │ PostgreSQL   │                    │ crawler        │  scheduled + on-demand
                 │ articles,    │                    │ httpx / PW     │  per-host throttling,
                 │ crawl_runs   │                    └───────┬────────┘  retries, browser fallback
                 └──────▲───────┘                            │ new items
                        │ upsert                             ▼
                 ┌──────┴───────┐   consume          ┌────────────────┐
                 │ worker (x2)  │ ◀───────────────── │ Kafka          │
                 │ enrich + save│                    │  osint.raw     │
                 └──────┬───────┘                    └────────────────┘
                        │ raw JSON                   failures ─▶ osint.dlq
                        ▼
                 ┌──────────────┐
                 │ MinIO        │
                 └──────────────┘
```

- **crawler**: crawls every source on a schedule, and on demand when the API publishes a job. It dedups against Postgres plus an in-memory LRU, then publishes only new items to `osint.raw`, keyed by URL hash.
- **worker**: consumes `osint.raw` in batches, matches keywords, extracts entities, CVEs and IPs, archives the raw payload to MinIO and upserts into Postgres. Offsets are committed only after the batch is saved, and bad messages go to `osint.dlq`. Run as many replicas as you have partitions.
- **api**: search and filter endpoints, stats, source health, crawl triggers, and the dashboard at `/`.

## Anti-bot handling

The fetcher (`app/crawlers/fetcher.py`) is built to stay unblocked:

- per-host throttling with random jitter, so no domain gets hammered
- exponential backoff on 429 and 5xx responses, honouring `Retry-After`
- automatic fallback to headless Chromium (Playwright) when a site returns 401 or 403 to plain HTTP, with a realistic viewport, locale and `navigator.webdriver` hidden
- `render: True` on a source forces browser mode for JS-heavy sites

Build with `INSTALL_PLAYWRIGHT=1` to enable the browser tier.

## Run it

```bash
cp .env.example .env
docker compose up --build
```

- Dashboard: http://localhost:8000
- API docs: http://localhost:8000/docs
- MinIO console: http://localhost:9001 (minioadmin / minioadmin)

The first crawl starts as soon as the crawler is up. Use **Crawl now** in the dashboard to trigger another one.

### Local mode (no Docker, live data)

Without Kafka, the API runs the crawler itself and writes straight to SQLite. It crawls every source on startup and then every `CRAWL_INTERVAL` seconds (default 600), and **Crawl now** in the dashboard works too.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export DATABASE_URL=sqlite+aiosqlite:///./osint.db
uvicorn app.api:app --reload
```

Open http://localhost:8000. Set `AUTO_CRAWL=0` to serve existing data without crawling.

To run the crawler on its own without Kafka:

```bash
python -m app.crawler_service --direct          # crawl forever
python -m app.crawler_service --direct --once   # one pass, then exit
```

### Offline demo (sample data, no crawling)

```bash
export DATABASE_URL=sqlite+aiosqlite:///./osint.db AUTO_CRAWL=0
python -m scripts.seed_demo
uvicorn app.api:app --reload
```

### Tests

```bash
pytest -q
```

## API

| Method | Path | Description |
|---|---|---|
| GET | `/api/articles?q=&keyword=&source=&alerts_only=&limit=&offset=` | Search and filter collected items. Returns `items` plus `total`, `offset` and `limit` for paging |
| GET | `/api/articles/{id}` | One item with full content |
| GET | `/api/stats` | Totals, top keywords, entities and CVEs |
| GET | `/api/sources` | Each source with its last run status |
| POST | `/api/crawl` `{"source": "*"}` | Crawl all sources, or one by name (queued on Kafka, or run in-process in local mode) |
| GET | `/health` | DB and Kafka status |

## Configuration

Sources live in `app/sources.py`. Add an entry with `type` set to `rss`, `hackernews`, `telegram`, `forum` (Invision Power Board topic lists, such as the BleepingComputer forums) or `reddit`. Keywords, crawl interval, concurrency and per-host delay are set in `.env`; `AUTO_CRAWL=0` turns off in-process crawling in local mode.

Some sites block crawlers:

- **Reuters and AP News** return 401/403 and have no public RSS, so they are read through Google News RSS filtered with `site:reuters.com` / `site:apnews.com`.
- **Reddit** blocks unauthenticated JSON and RSS access (403/429). No Reddit sources are enabled; adding them needs Reddit API (OAuth) credentials.

The dashboard feed is paginated, 10 items per page, and search and filters run on the server.

## Project layout

```
app/
  api.py              FastAPI app and dashboard
  crawler_service.py  scheduler and on-demand job runner (--direct: no Kafka)
  worker.py           Kafka consumer: enrich, archive, store
  crawlers/
    fetcher.py        throttled httpx client with retries and Playwright fallback
    parsers.py        RSS / Reddit / HN / Telegram / forum parsers (pure functions)
    base.py           source dispatch
  enrich.py           keyword matching, entity and IOC extraction
  db.py               SQLAlchemy models and upsert helpers
  kafka.py            producer and consumer factories
  storage.py          MinIO raw archive
  sources.py          monitored sources
  static/index.html   dashboard
scripts/seed_demo.py  sample data for offline demos
tests/                parser and enrichment tests
```

## Notes

Crawl only public pages, respect each site's terms of service and robots.txt, and keep request rates low.
