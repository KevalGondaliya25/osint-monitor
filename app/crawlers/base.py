"""Dispatch a source to the right fetch + parse routine."""
import asyncio
import logging

from .fetcher import Fetcher
from .parsers import parse_hn_item, parse_ipb_forum, parse_reddit, parse_rss, parse_telegram

log = logging.getLogger(__name__)

HN_TOP_N = 30


async def crawl_source(fetcher: Fetcher, source: dict) -> list[dict]:
    kind = source["type"]
    render = source.get("render", False)

    if kind == "rss":
        return parse_rss(await fetcher.get_text(source["url"], render=render), source)

    if kind == "reddit":
        resp = await fetcher.get(source["url"])
        return parse_reddit(resp.json(), source)

    if kind == "hackernews":
        ids = (await fetcher.get(f"{source['url']}/topstories.json")).json()[:HN_TOP_N]

        async def one(i: int):
            try:
                return parse_hn_item((await fetcher.get(f"{source['url']}/item/{i}.json", throttle=False)).json(), source)
            except Exception as exc:
                log.warning("HN item %s failed: %s", i, exc)
                return None

        # HN's Firebase API is happy with parallel calls; skip per-host throttle for it
        results = await asyncio.gather(*(one(i) for i in ids))
        return [r for r in results if r]

    if kind == "telegram":
        return parse_telegram(await fetcher.get_text(source["url"], render=render), source)

    if kind == "forum":
        return parse_ipb_forum(await fetcher.get_text(source["url"], render=render), source)

    raise ValueError(f"unknown source type: {kind}")
