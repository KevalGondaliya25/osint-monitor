"""Central settings, read from environment variables."""
import os
from dataclasses import dataclass, field


def _list(name: str, default: str) -> list[str]:
    return [x.strip() for x in os.getenv(name, default).split(",") if x.strip()]


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv(
        "DATABASE_URL", "postgresql+asyncpg://osint:osint@localhost:5432/osint"
    )
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    raw_topic: str = os.getenv("RAW_TOPIC", "osint.raw")
    jobs_topic: str = os.getenv("JOBS_TOPIC", "osint.jobs")
    dlq_topic: str = os.getenv("DLQ_TOPIC", "osint.dlq")

    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "")  # empty = disabled
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    minio_bucket: str = os.getenv("MINIO_BUCKET", "osint-raw")

    crawl_interval: int = int(os.getenv("CRAWL_INTERVAL", "600"))
    crawl_concurrency: int = int(os.getenv("CRAWL_CONCURRENCY", "5"))
    per_host_delay: float = float(os.getenv("PER_HOST_DELAY", "1.5"))
    user_agent: str = os.getenv(
        "USER_AGENT",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0 Safari/537.36 OSINT-Monitor/1.0",
    )
    keywords: list[str] = field(
        default_factory=lambda: [
            k.lower()
            for k in _list(
                "KEYWORDS",
                "ransomware,data breach,phishing,sanctions,cyberattack,leak,"
                "vulnerability,zero-day,malware,election,protest,disinformation",
            )
        ]
    )


settings = Settings()
