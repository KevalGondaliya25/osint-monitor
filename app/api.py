"""FastAPI service: search API, stats, crawl triggers and the dashboard.

    uvicorn app.api:app --reload
"""
import json
import logging
from collections import Counter
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy import desc, func, or_, select

from app.config import settings
from app.db import Article, CrawlRun, Session, init_db, utcnow
from app.sources import SOURCES, SOURCES_BY_NAME

log = logging.getLogger("api")
STATIC = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    app.state.producer = None
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap,
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    try:
        await producer.start()
        app.state.producer = producer
    except Exception as exc:  # API still serves data without Kafka
        log.warning("Kafka unavailable, crawl triggers disabled: %s", exc)
        await producer.stop()
    yield
    if app.state.producer:
        await app.state.producer.stop()


app = FastAPI(title="OSINT Monitor", version="1.0.0", lifespan=lifespan)


class CrawlRequest(BaseModel):
    source: str = "*"  # a source name, or "*" for all


@app.get("/", include_in_schema=False)
async def dashboard():
    return FileResponse(STATIC / "index.html")


@app.get("/health")
async def health():
    async with Session() as s:
        await s.execute(select(1))
    return {"status": "ok", "kafka": app.state.producer is not None}


@app.get("/api/articles")
async def list_articles(
    q: str | None = Query(None, description="full-text search in title and content"),
    keyword: str | None = None,
    source: str | None = None,
    alerts_only: bool = False,
    limit: int = Query(50, le=200),
    offset: int = 0,
):
    stmt = select(Article).order_by(desc(func.coalesce(Article.published_at, Article.fetched_at)))
    if q:
        like = f"%{q}%"
        stmt = stmt.where(or_(Article.title.ilike(like), Article.content.ilike(like)))
    if source:
        stmt = stmt.where(Article.source == source)
    async with Session() as s:
        rows = (await s.execute(stmt.limit(2000 if (keyword or alerts_only) else limit).offset(offset))).scalars().all()
    items = [r.to_dict() for r in rows]
    # keyword filters run in Python so the same code works on SQLite and Postgres JSON
    if keyword:
        items = [i for i in items if keyword.lower() in i["keywords"]]
    if alerts_only:
        items = [i for i in items if i["keywords"]]
    return {"count": len(items[:limit]), "items": items[:limit]}


@app.get("/api/articles/{article_id}")
async def get_article(article_id: int):
    async with Session() as s:
        row = await s.get(Article, article_id)
    if not row:
        raise HTTPException(404, "not found")
    return row.to_dict() | {"content": row.content}


@app.get("/api/stats")
async def stats():
    since = utcnow() - timedelta(hours=24)
    async with Session() as s:
        total = await s.scalar(select(func.count(Article.id)))
        last_24h = await s.scalar(select(func.count(Article.id)).where(Article.fetched_at >= since))
        by_source = dict((await s.execute(
            select(Article.source, func.count(Article.id)).group_by(Article.source)
        )).all())
        recent = (await s.execute(
            select(Article.keywords, Article.entities, Article.indicators)
            .order_by(desc(Article.fetched_at)).limit(3000)
        )).all()

    kw, ent, cves = Counter(), Counter(), Counter()
    for keywords, entities, indicators in recent:
        kw.update(keywords or [])
        ent.update(entities or [])
        cves.update((indicators or {}).get("cves", []))
    alerts = sum(1 for k, _, _ in recent if k)
    return {
        "total": total or 0,
        "last_24h": last_24h or 0,
        "alerts": alerts,
        "sources": len(SOURCES),
        "by_source": by_source,
        "top_keywords": kw.most_common(12),
        "top_entities": ent.most_common(15),
        "top_cves": cves.most_common(10),
    }


@app.get("/api/sources")
async def sources():
    out = []
    async with Session() as s:
        for src in SOURCES:
            run = (await s.execute(
                select(CrawlRun).where(CrawlRun.source == src["name"]).order_by(desc(CrawlRun.started_at)).limit(1)
            )).scalar_one_or_none()
            out.append({
                **src,
                "last_run": run.started_at.isoformat() if run else None,
                "status": run.status if run else "never",
                "last_items": run.items if run else 0,
                "error": run.error if run else None,
            })
    return out


@app.post("/api/crawl", status_code=202)
async def trigger_crawl(req: CrawlRequest):
    if req.source != "*" and req.source not in SOURCES_BY_NAME:
        raise HTTPException(404, f"unknown source {req.source!r}")
    if not app.state.producer:
        raise HTTPException(503, "Kafka is not connected")
    await app.state.producer.send_and_wait(settings.jobs_topic, {"source": req.source})
    return {"queued": req.source}
