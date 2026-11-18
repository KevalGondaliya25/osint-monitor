"""Sources the crawler monitors. Add or remove entries freely.

type:   rss | reddit | hackernews | telegram | forum
render: True to fetch through a headless browser (Playwright) instead of httpx

Notes on sites that block direct crawling:
- reuters.com and apnews.com return 401/403 to crawlers and have no public RSS,
  so they are read through Google News RSS filtered with `site:`.
- reddit.com blocks unauthenticated JSON/RSS access (403/429), so Reddit
  sources need Reddit API (OAuth) credentials before they can be added.
"""


def _gnews(query: str) -> str:
    return f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"


SOURCES: list[dict] = [
    {"name": "BBC World", "type": "rss", "url": "https://feeds.bbci.co.uk/news/world/rss.xml"},
    {"name": "BBC Technology", "type": "rss", "url": "https://feeds.bbci.co.uk/news/technology/rss.xml"},
    {"name": "Al Jazeera", "type": "rss", "url": "https://www.aljazeera.com/xml/rss/all.xml"},
    {"name": "Reuters (Google News)", "type": "rss", "url": _gnews("site:reuters.com")},
    {"name": "AP News (Google News)", "type": "rss", "url": _gnews("site:apnews.com")},
    {"name": "Google News: Top stories", "type": "rss",
     "url": "https://news.google.com/rss?hl=en-US&gl=US&ceid=US:en"},
    {"name": "BleepingComputer", "type": "rss", "url": "https://www.bleepingcomputer.com/feed/"},
    {"name": "The Hacker News", "type": "rss", "url": "https://feeds.feedburner.com/TheHackersNews"},
    {"name": "Hacker News", "type": "hackernews", "url": "https://hacker-news.firebaseio.com/v0"},
    {"name": "BC Forum: General Security", "type": "forum",
     "url": "https://www.bleepingcomputer.com/forums/f/45/general-security/"},
    {"name": "BC Forum: Ransomware Help", "type": "forum",
     "url": "https://www.bleepingcomputer.com/forums/f/239/ransomware-help-tech-support/"},
    {"name": "BC Forum: Am I infected?", "type": "forum",
     "url": "https://www.bleepingcomputer.com/forums/f/103/am-i-infected-what-do-i-do/"},
    {"name": "Telegram: durov", "type": "telegram", "url": "https://t.me/s/durov"},
]

SOURCES_BY_NAME = {s["name"]: s for s in SOURCES}
