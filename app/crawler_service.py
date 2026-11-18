"""Crawler service.

Two loops run side by side:
  1. scheduler  - crawls every source every CRAWL_INTERVAL seconds
  2. job runner - crawls a single source on demand when the API publishes to `osint.jobs`

Every new item is published to the `osint.raw` Kafka topic (key = URL hash, so
the same story always lands on the same partition).

    python -m app.crawler_service           # run forever
    python -m app.crawler_service --once    # one pass over all sources, then exit
    python -m app.crawler_service --direct  # no Kafka: write straight to the database
"""
import argparse
import asyncio
import logging
from collections import OrderedDict

from app.config import settings
from app.crawlers import crawl_source
from app.crawlers.fetcher import Fetcher
from app.db import CrawlRun, Session, init_db, known_hashes, save_article, utcnow
from app.enrich import enrich, url_hash
from app.kafka import make_consumer, make_producer
from app.sources import SOURCES, SOURCES_BY_NAME

log = logging.getLogger("crawler")


class SeenCache:
    """Small in-memory LRU so we don't re-publish the same URLs every cycle."""

    def __init__(self, size: int = 50_000):
        self.size, self._d = size, OrderedDict()

    def add(self, key: str) -> bool:
        if key in self._d:
            self._d.move_to_end(key)
            return False
        self._d[key] = None
        if len(self._d) > self.size:
            self._d.popitem(last=False)
        return True


class CrawlerService:
    def __init__(self):
        self.fetcher = Fetcher()
        self.seen = SeenCache()
        self.sem = asyncio.Semaphore(settings.crawl_concurrency)
        self.producer = None

    async def run_source(self, source: dict) -> int:
        async with self.sem:
            async with Session() as s:
                run = CrawlRun(source=source["name"])
                s.add(run)
                await s.commit()
            try:
                items = await crawl_source(self.fetcher, source)
                for it in items:
                    it["url_hash"] = url_hash(it["url"])
                async with Session() as s:
                    stored = await known_hashes(s, [it["url_hash"] for it in items])
                fresh = [it for it in items if it["url_hash"] not in stored and self.seen.add(it["url_hash"])]
                if self.producer:
                    for it in fresh:
                        await self.producer.send(settings.raw_topic, it, key=it["url_hash"])
                    await self.producer.flush()
                else:
                    async with Session() as s:
                        for it in fresh:
                            await save_article(s, enrich(it))
                        await s.commit()
                status, err, n = "ok", None, len(fresh)
                log.info("%-24s fetched=%-3d new=%d", source["name"], len(items), n)
            except Exception as exc:
                status, err, n = "error", f"{type(exc).__name__}: {exc}"[:1000], 0
                log.warning("%-24s FAILED %s", source["name"], err)
            async with Session() as s:
                run = await s.get(CrawlRun, run.id)
                run.status, run.error, run.items, run.finished_at = status, err, n, utcnow()
                await s.commit()
            return n

    async def run_all(self) -> int:
        results = await asyncio.gather(*(self.run_source(src) for src in SOURCES))
        total = sum(results)
        log.info("cycle done: %d new items from %d sources", total, len(SOURCES))
        return total

    async def scheduler(self) -> None:
        while True:
            await self.run_all()
            await asyncio.sleep(settings.crawl_interval)

    async def job_runner(self) -> None:
        consumer = await make_consumer(settings.jobs_topic, group_id="osint-crawler-jobs")
        try:
            async for msg in consumer:
                name = msg.value.get("source")
                targets = SOURCES if name in (None, "*") else [SOURCES_BY_NAME[name]] if name in SOURCES_BY_NAME else []
                if not targets:
                    log.warning("job for unknown source %r ignored", name)
                await asyncio.gather(*(self.run_source(t) for t in targets))
                await consumer.commit()
        finally:
            await consumer.stop()

    async def main(self, once: bool, direct: bool = False) -> None:
        await init_db()
        if not direct:
            self.producer = await make_producer()
        try:
            if once:
                await self.run_all()
            elif direct:
                await self.scheduler()
            else:
                await asyncio.gather(self.scheduler(), self.job_runner())
        finally:
            if self.producer:
                await self.producer.stop()
            await self.fetcher.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--direct", action="store_true", help="no Kafka: enrich and write straight to the database")
    args = ap.parse_args()
    asyncio.run(CrawlerService().main(args.once, args.direct))
