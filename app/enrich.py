"""Lightweight enrichment: keyword hits + entity / indicator extraction.

Regex-based so it runs anywhere with no model download. Swap `extract_entities`
for spaCy (`en_core_web_sm`) if you need proper NER.
"""
import hashlib
import re

from app.config import settings

CVE_RE = re.compile(r"\bCVE-\d{4}-\d{4,7}\b", re.IGNORECASE)
IPV4_RE = re.compile(r"\b(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)\b")
EMAIL_RE = re.compile(r"\b[\w.+-]+@[\w-]+\.[\w.-]+\b")
# Two to four capitalised words in a row: "European Union", "Microsoft Exchange Server"
PROPER_RE = re.compile(r"\b[A-Z][a-zA-Z&'-]+(?:\s+[A-Z][a-zA-Z&'-]+){1,4}\b")
ACRONYM_RE = re.compile(r"\b[A-Z]{2,6}\b")

STOP_START = {"The", "A", "An", "This", "That", "These", "Those", "In", "On", "At", "For", "And",
              "But", "If", "When", "How", "What", "Why", "Who", "Its", "It", "We", "They", "He", "She",
              "New", "Critical", "Breaking", "Show", "Ask", "Update", "Report", "Researchers"}
ACRONYM_SKIP = {"US", "UK", "EU", "UN", "THE", "AND", "FOR", "NEW", "CEO", "AI", "IT", "TV", "PM", "AM",
                "ID", "OK", "USA", "BBC", "HN", "RSS", "URL", "API", "PDF", "FAQ", "CVE", "IOC", "IOCS", "GB", "TB"}
KEEP_ACRONYMS = {"NATO", "FBI", "CISA", "NSA", "GCHQ", "CIA", "IMF", "WHO", "OPEC", "ICC", "NCSC", "ENISA"}

_KW_RES = {kw: re.compile(r"\b" + re.escape(kw) + r"\b", re.IGNORECASE) for kw in settings.keywords}


def url_hash(url: str) -> str:
    return hashlib.sha256(url.strip().encode()).hexdigest()


def match_keywords(text: str) -> list[str]:
    return sorted(kw for kw, rx in _KW_RES.items() if rx.search(text))


def extract_entities(text: str, limit: int = 15) -> list[str]:
    seen: dict[str, None] = {}
    for m in PROPER_RE.findall(text):
        words = m.split()
        while words and words[0] in STOP_START:
            words = words[1:]
        if 2 <= len(words) <= 4:
            seen.setdefault(" ".join(words), None)
    for m in ACRONYM_RE.findall(text):
        if m in KEEP_ACRONYMS or (m not in ACRONYM_SKIP and len(m) >= 3):
            seen.setdefault(m, None)
    return list(seen)[:limit]


def extract_indicators(text: str) -> dict[str, list[str]]:
    return {
        "cves": sorted({c.upper() for c in CVE_RE.findall(text)}),
        "ips": sorted(set(IPV4_RE.findall(text))),
        "emails": sorted(set(EMAIL_RE.findall(text))),
    }


def enrich(item: dict) -> dict:
    text = f"{item.get('title', '')}\n{item.get('content', '')}"
    return {
        **item,
        "url_hash": url_hash(item["url"]),
        "keywords": match_keywords(text),
        "entities": extract_entities(text),
        "indicators": extract_indicators(text),
    }
