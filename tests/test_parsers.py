from app.crawlers.parsers import parse_hn_item, parse_reddit, parse_rss, parse_telegram
from app.enrich import enrich, extract_indicators, match_keywords

RSS = """<?xml version="1.0"?><rss version="2.0"><channel><title>t</title>
<item><title>Ransomware hits hospital</title><link>https://ex.com/a</link>
<description>&lt;p&gt;Hospital &lt;b&gt;systems&lt;/b&gt; down&lt;/p&gt;</description>
<pubDate>Tue, 17 Nov 2026 10:00:00 GMT</pubDate></item>
<item><title>No link here</title></item>
</channel></rss>"""

TELEGRAM = """<div class="tgme_widget_message">
  <div class="tgme_widget_message_text">New phishing wave. Details inside.</div>
  <a class="tgme_widget_message_date" href="https://t.me/chan/42"><time datetime="2026-11-17T09:00:00+00:00"></time></a>
</div>"""


def test_rss():
    items = parse_rss(RSS, {"name": "Feed", "type": "rss"})
    assert len(items) == 1
    assert items[0]["content"] == "Hospital systems down"
    assert items[0]["published_at"].startswith("2026-11-17T10:00")


def test_reddit():
    data = {"data": {"children": [{"data": {"permalink": "/r/x/1", "title": "T", "selftext": "body",
                                             "author": "u", "created_utc": 1763373600}}]}}
    items = parse_reddit(data, {"name": "r/x", "type": "reddit"})
    assert items[0]["url"] == "https://www.reddit.com/r/x/1"


def test_hn_skips_non_stories():
    src = {"name": "HN", "type": "hackernews"}
    assert parse_hn_item({"type": "comment", "id": 1}, src) is None
    assert parse_hn_item({"type": "story", "id": 2, "title": "x"}, src)["url"].endswith("id=2")


def test_telegram():
    items = parse_telegram(TELEGRAM, {"name": "TG", "type": "telegram", "url": "https://t.me/s/chan"})
    assert items[0]["url"] == "https://t.me/chan/42"
    assert items[0]["title"] == "New phishing wave"


def test_enrichment():
    out = enrich({"url": "https://x", "title": "CISA warns on CVE-2026-1234 zero-day",
                  "content": "Seen from 10.0.0.1, linked to Lazarus Group ransomware."})
    assert "zero-day" in out["keywords"] and "ransomware" in out["keywords"]
    assert out["indicators"]["cves"] == ["CVE-2026-1234"]
    assert "Lazarus Group" in out["entities"] and "CISA" in out["entities"]
    assert match_keywords("leaky faucet") == []          # word boundaries
    assert extract_indicators("999.1.1.1")["ips"] == []  # invalid IP ignored
