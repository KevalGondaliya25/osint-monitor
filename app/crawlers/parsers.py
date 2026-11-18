"""Pure parsing functions: raw response -> list of normalised items.

Kept separate from fetching so they are easy to unit-test.
Every item has: source, source_type, url, title, content, author, published_at (ISO 8601 or None)
"""
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import feedparser
from bs4 import BeautifulSoup


def _iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _clean_html(text: str) -> str:
    return BeautifulSoup(text or "", "html.parser").get_text(" ", strip=True)


def item(source: dict, url: str, title: str, content: str = "", author: str | None = None,
         published_at: str | None = None) -> dict:
    return {
        "source": source["name"],
        "source_type": source["type"],
        "url": url,
        "title": (title or "").strip()[:500],
        "content": (content or "").strip()[:20_000],
        "author": author,
        "published_at": published_at,
    }


def parse_rss(text: str, source: dict) -> list[dict]:
    feed = feedparser.parse(text)
    out = []
    for e in feed.entries:
        link = e.get("link")
        if not link:
            continue
        published = None
        for key in ("published", "updated"):
            if e.get(key):
                try:
                    published = _iso(parsedate_to_datetime(e[key]))
                except (TypeError, ValueError):
                    pass
                break
        summary = _clean_html(e.get("summary", ""))
        out.append(item(source, link, e.get("title", ""), summary, e.get("author"), published))
    return out


def parse_reddit(data: dict, source: dict) -> list[dict]:
    out = []
    for child in data.get("data", {}).get("children", []):
        d = child.get("data", {})
        if not d.get("permalink"):
            continue
        created = d.get("created_utc")
        published = _iso(datetime.fromtimestamp(created, tz=timezone.utc)) if created else None
        out.append(item(
            source,
            "https://www.reddit.com" + d["permalink"],
            d.get("title", ""),
            d.get("selftext", "") or d.get("url", ""),
            d.get("author"),
            published,
        ))
    return out


def parse_hn_item(d: dict, source: dict) -> dict | None:
    if not d or d.get("type") != "story" or d.get("deleted") or d.get("dead"):
        return None
    url = d.get("url") or f"https://news.ycombinator.com/item?id={d['id']}"
    published = _iso(datetime.fromtimestamp(d["time"], tz=timezone.utc)) if d.get("time") else None
    return item(source, url, d.get("title", ""), _clean_html(d.get("text", "")), d.get("by"), published)


def parse_telegram(html: str, source: dict) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    channel = source["url"].rstrip("/").rsplit("/", 1)[-1]
    out = []
    for msg in soup.select(".tgme_widget_message"):
        text_el = msg.select_one(".tgme_widget_message_text")
        link_el = msg.select_one("a.tgme_widget_message_date")
        if not text_el or not link_el:
            continue
        text = text_el.get_text(" ", strip=True)
        time_el = link_el.select_one("time[datetime]")
        published = None
        if time_el:
            try:
                published = _iso(datetime.fromisoformat(time_el["datetime"]))
            except ValueError:
                pass
        title = text.split(". ")[0][:140]
        out.append(item(source, link_el["href"], title, text, channel, published))
    return out
