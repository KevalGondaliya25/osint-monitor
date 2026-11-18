"""Kafka helpers (aiokafka) with JSON (de)serialisation and startup retries."""
import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from app.config import settings

log = logging.getLogger(__name__)


def _dumps(v) -> bytes:
    return json.dumps(v, ensure_ascii=False, default=str).encode()


def _loads(v: bytes):
    return json.loads(v.decode())


async def _start(client, what: str, attempts: int = 20) -> None:
    for i in range(1, attempts + 1):
        try:
            await client.start()
            log.info("%s connected to %s", what, settings.kafka_bootstrap)
            return
        except KafkaConnectionError:
            log.warning("%s: Kafka not ready (%d/%d), retrying...", what, i, attempts)
            await asyncio.sleep(3)
    raise RuntimeError(f"{what}: could not connect to Kafka at {settings.kafka_bootstrap}")


async def make_producer() -> AIOKafkaProducer:
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap,
        value_serializer=_dumps,
        key_serializer=lambda k: k.encode() if k else None,
        acks="all",
        enable_idempotence=True,
        linger_ms=20,
        compression_type="gzip",
    )
    await _start(producer, "producer")
    return producer


async def make_consumer(*topics: str, group_id: str) -> AIOKafkaConsumer:
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=settings.kafka_bootstrap,
        group_id=group_id,
        value_deserializer=_loads,
        enable_auto_commit=False,  # commit only after the message is safely processed
        auto_offset_reset="earliest",
        max_poll_records=200,
    )
    await _start(consumer, f"consumer[{group_id}]")
    return consumer
