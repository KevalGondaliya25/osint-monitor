"""Processing worker.

Consumes `osint.raw`, enriches each item (keywords, entities, CVEs, IPs),
archives the raw payload to MinIO and stores the result in PostgreSQL.
Offsets are committed only after the batch is saved, so a crash never loses data.
Messages that keep failing go to `osint.dlq`.

    python -m app.worker
"""
import asyncio
import logging

from app.config import settings
from app.db import Session, init_db, save_article
from app.enrich import enrich
from app.kafka import make_consumer, make_producer
from app.storage import RawStore

log = logging.getLogger("worker")


async def process(item: dict, store: RawStore) -> dict:
    data = enrich(item)
    data["raw_key"] = await store.put(data["url_hash"], item)
    return data


async def main() -> None:
    await init_db()
    store = RawStore()
    consumer = await make_consumer(settings.raw_topic, group_id="osint-processor")
    dlq = await make_producer()
    try:
        while True:
            batches = await consumer.getmany(timeout_ms=2000, max_records=200)
            if not batches:
                continue
            saved = dupes = failed = 0
            async with Session() as session:
                for _tp, messages in batches.items():
                    for msg in messages:
                        try:
                            data = await process(msg.value, store)
                            async with session.begin_nested():  # savepoint: one bad row can't poison the batch
                                inserted = await save_article(session, data)
                            if inserted:
                                saved += 1
                                if data["keywords"]:
                                    log.info("ALERT [%s] %s", ", ".join(data["keywords"]), data["title"][:90])
                            else:
                                dupes += 1
                        except Exception as exc:
                            failed += 1
                            log.exception("failed to process message at offset %s", msg.offset)
                            await dlq.send(settings.dlq_topic, {"error": str(exc), "payload": msg.value})
                await session.commit()
            await consumer.commit()
            log.info("batch: saved=%d duplicates=%d failed=%d", saved, dupes, failed)
    finally:
        await consumer.stop()
        await dlq.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    asyncio.run(main())
