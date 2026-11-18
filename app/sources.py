"""Sources the crawler monitors. Add or remove entries freely.

type:   rss | reddit | hackernews | telegram
render: True to fetch through a headless browser (Playwright) instead of httpx
"""

SOURCES: list[dict] = [
    # --- News ---
    {"name": "BBC World", "type": "rss", "url": "https://feeds.bbci.co.uk/news/world/rss.xml"},
    {"name": "BBC Technology", "type": "rss", "url": "https://feeds.bbci.co.uk/news/technology/rss.xml"},
    {"name": "Al Jazeera", "type": "rss", "url": "https://www.aljazeera.com/xml/rss/all.xml"},
    {
        "name": "Reuters (Google News)",
        "type": "rss",
        "url": "https://news.google.com/rss/search?q=site:reuters.com&hl=en-US&gl=US&ceid=US:en",
    },
    {"name": "BleepingComputer", "type": "rss", "url": "https://www.bleepingcomputer.com/feed/"},
    {"name": "The Hacker News", "type": "rss", "url": "https://feeds.feedburner.com/TheHackersNews"},
    # --- Forums / communities ---
    {"name": "Hacker News", "type": "hackernews", "url": "https://hacker-news.firebaseio.com/v0"},
    {"name": "r/cybersecurity", "type": "reddit", "url": "https://www.reddit.com/r/cybersecurity/new.json?limit=50"},
    {"name": "r/OSINT", "type": "reddit", "url": "https://www.reddit.com/r/OSINT/new.json?limit=50"},
    {"name": "r/worldnews", "type": "reddit", "url": "https://www.reddit.com/r/worldnews/new.json?limit=50"},
    # --- Telegram public channels (web preview at t.me/s/<channel>) ---
    {"name": "Telegram: durov", "type": "telegram", "url": "https://t.me/s/durov"},
]

SOURCES_BY_NAME = {s["name"]: s for s in SOURCES}
