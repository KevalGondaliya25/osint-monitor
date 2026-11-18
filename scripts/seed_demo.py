"""Load sample items into the database so the dashboard has data without crawling.

    DATABASE_URL=sqlite+aiosqlite:///./osint.db python -m scripts.seed_demo
"""
import asyncio
from datetime import datetime, timedelta, timezone

from app.db import CrawlRun, Session, init_db, save_article
from app.enrich import enrich
from app.sources import SOURCES

NOW = datetime.now(timezone.utc)

SAMPLES = [
    ("BleepingComputer", "rss", "Ransomware gang claims attack on European logistics firm, leaks 200GB of data",
     "The Akira ransomware group added Nordic Freight Group to its leak site. CISA and ENISA are tracking the incident."),
    ("The Hacker News", "rss", "Critical Fortinet flaw CVE-2026-24858 exploited as zero-day",
     "Attackers exploited CVE-2026-24858 in FortiOS before a patch was available. Malicious traffic came from 185.220.101.47."),
    ("BBC World", "rss", "EU agrees new sanctions package targeting shipping networks",
     "The European Union approved fresh sanctions aimed at shadow fleet operators and their insurers."),
    ("Al Jazeera", "rss", "Thousands join protest in Nairobi over proposed finance bill",
     "Demonstrators gathered outside Parliament Buildings as police deployed tear gas in central Nairobi."),
    ("Reuters (Google News)", "rss", "Microsoft warns of phishing campaign impersonating Teams meeting invites",
     "Microsoft Threat Intelligence said the phishing lures target finance staff at mid-sized companies."),
    ("Hacker News", "hackernews", "Show HN: I mapped 40k exposed Elasticsearch clusters leaking customer data",
     "A researcher scanned public IPv4 space and found a large data breach exposure across cloud providers."),
    ("r/OSINT", "reddit", "Best workflow for verifying geolocation of conflict footage?",
     "Looking for tips beyond Google Earth and Sentinel Hub for shadow analysis and landmark matching."),
    ("r/cybersecurity", "reddit", "New malware strain spreading through fake browser updates",
     "Several users reporting SocGholish style lures delivering an infostealer. IOCs posted below."),
    ("r/worldnews", "reddit", "Election officials report coordinated disinformation network ahead of vote",
     "Electoral Commission says hundreds of accounts were amplifying false claims about polling stations."),
    ("BBC Technology", "rss", "UK retailer confirms customer data breach after cyberattack",
     "The National Cyber Security Centre is supporting the investigation into the cyberattack."),
    ("Telegram: durov", "telegram", "We removed millions of groups and channels violating our terms this year",
     "Moderation update on spam, scam and malware distribution channels."),
    ("The Hacker News", "rss", "Researchers detail Linux kernel vulnerability CVE-2026-31337 affecting containers",
     "The vulnerability allows container escape on unpatched hosts. Proof of concept is public."),
    ("BBC World", "rss", "NATO leaders meet in Brussels to discuss air defence funding",
     "Defence ministers from NATO members discussed long-term spending commitments."),
    ("Hacker News", "hackernews", "How we cut our Kafka consumer lag from hours to seconds",
     "A write-up on partitioning strategy, batching and backpressure in a Python pipeline."),
    ("BleepingComputer", "rss", "Phishing kit sold on Telegram bypasses MFA with reverse proxy",
     "The kit, advertised on several Telegram channels, steals session cookies to bypass MFA."),
    ("Al Jazeera", "rss", "UN report warns of rising cyberattack threat to humanitarian groups",
     "The United Nations said aid agencies saw a sharp rise in ransomware and phishing incidents."),
]


async def main() -> None:
    await init_db()
    async with Session() as s:
        for i, (src, kind, title, content) in enumerate(SAMPLES):
            item = {
                "source": src, "source_type": kind, "title": title, "content": content, "author": None,
                "url": f"https://example.com/demo/{i}",
                "published_at": (NOW - timedelta(minutes=17 * i + 3)).isoformat(),
            }
            await save_article(s, enrich(item))
        for src in SOURCES:
            s.add(CrawlRun(source=src["name"], status="ok", items=3, finished_at=NOW))
        await s.commit()
    print(f"seeded {len(SAMPLES)} articles")


if __name__ == "__main__":
    asyncio.run(main())
