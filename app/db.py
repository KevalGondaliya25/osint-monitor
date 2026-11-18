"""PostgreSQL models and helpers (SQLAlchemy 2 async).

Also runs on SQLite (DATABASE_URL=sqlite+aiosqlite:///./osint.db) for quick local demos.
"""
from datetime import datetime, timezone

from sqlalchemy import JSON, DateTime, Index, Integer, String, Text, func, select
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.config import settings

JSONType = JSON().with_variant(postgresql.JSONB(), "postgresql")

engine = create_async_engine(settings.database_url, pool_pre_ping=True)
Session = async_sessionmaker(engine, expire_on_commit=False)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Article(Base):
    __tablename__ = "articles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    url_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    url: Mapped[str] = mapped_column(Text)
    source: Mapped[str] = mapped_column(String(120), index=True)
    source_type: Mapped[str] = mapped_column(String(32))
    title: Mapped[str] = mapped_column(Text)
    content: Mapped[str] = mapped_column(Text, default="")
    author: Mapped[str | None] = mapped_column(String(200), nullable=True)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    keywords: Mapped[list] = mapped_column(JSONType, default=list)
    entities: Mapped[list] = mapped_column(JSONType, default=list)
    indicators: Mapped[dict] = mapped_column(JSONType, default=dict)
    raw_key: Mapped[str | None] = mapped_column(String(300), nullable=True)  # MinIO object key

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "url": self.url,
            "source": self.source,
            "source_type": self.source_type,
            "title": self.title,
            "content": self.content[:600],
            "author": self.author,
            "published_at": self.published_at.isoformat() if self.published_at else None,
            "fetched_at": self.fetched_at.isoformat() if self.fetched_at else None,
            "keywords": self.keywords or [],
            "entities": self.entities or [],
            "indicators": self.indicators or {},
            "raw_key": self.raw_key,
        }


class CrawlRun(Base):
    __tablename__ = "crawl_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(120), index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(16), default="running")  # running | ok | error
    items: Mapped[int] = mapped_column(Integer, default=0)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)


Index("ix_runs_source_started", CrawlRun.source, CrawlRun.started_at.desc())


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


async def save_article(session: AsyncSession, data: dict) -> bool:
    """Insert one enriched item. Returns False if the URL was already stored."""
    row = {
        "url_hash": data["url_hash"],
        "url": data["url"],
        "source": data["source"],
        "source_type": data["source_type"],
        "title": data["title"],
        "content": data.get("content", ""),
        "author": data.get("author"),
        "published_at": _parse_dt(data.get("published_at")),
        "fetched_at": utcnow(),
        "keywords": data.get("keywords", []),
        "entities": data.get("entities", []),
        "indicators": data.get("indicators", {}),
        "raw_key": data.get("raw_key"),
    }
    dialect = postgresql if session.bind.dialect.name == "postgresql" else sqlite
    stmt = dialect.insert(Article).values(**row).on_conflict_do_nothing(index_elements=["url_hash"])
    result = await session.execute(stmt)
    return result.rowcount == 1


async def known_hashes(session: AsyncSession, hashes: list[str]) -> set[str]:
    if not hashes:
        return set()
    rows = await session.execute(select(Article.url_hash).where(Article.url_hash.in_(hashes)))
    return {r[0] for r in rows}


__all__ = ["Article", "CrawlRun", "Session", "engine", "init_db", "save_article", "known_hashes", "func", "utcnow"]
