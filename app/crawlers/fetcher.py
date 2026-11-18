"""Polite async HTTP fetcher.

- per-host throttling with jitter, so we never hammer one domain
- retries with exponential backoff on 429 / 5xx, honours Retry-After
- falls back to a headless browser (Playwright) when a site blocks plain HTTP
"""
import asyncio
import logging
import random
import time
from urllib.parse import urlparse

import httpx

from app.config import settings

log = logging.getLogger(__name__)

RETRY_STATUS = {429, 500, 502, 503, 504}
BLOCK_STATUS = {401, 403}


class Fetcher:
    def __init__(self, per_host_delay: float | None = None, max_retries: int = 3):
        self.per_host_delay = settings.per_host_delay if per_host_delay is None else per_host_delay
        self.max_retries = max_retries
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(20.0),
            follow_redirects=True,
            headers={
                "User-Agent": settings.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7",
                "Accept-Language": "en-US,en;q=0.9",
            },
        )
        self._last_hit: dict[str, float] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    async def _throttle(self, host: str) -> None:
        lock = self._locks.setdefault(host, asyncio.Lock())
        async with lock:
            wait = self._last_hit.get(host, 0) + self.per_host_delay - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait + random.uniform(0, 0.5))
            self._last_hit[host] = time.monotonic()

    async def get(self, url: str, throttle: bool = True, **kwargs) -> httpx.Response:
        host = urlparse(url).netloc
        for attempt in range(1, self.max_retries + 1):
            if throttle:
                await self._throttle(host)
            try:
                resp = await self.client.get(url, **kwargs)
            except httpx.TransportError as exc:
                if attempt == self.max_retries:
                    raise
                log.warning("transport error on %s (%s), retry %d", url, exc, attempt)
                await asyncio.sleep(2**attempt)
                continue

            if resp.status_code in RETRY_STATUS and attempt < self.max_retries:
                ra = resp.headers.get("Retry-After", "")
                delay = float(ra) if ra.isdigit() else 2**attempt + random.random()
                log.warning("%s returned %s, sleeping %.1fs", url, resp.status_code, delay)
                await asyncio.sleep(delay)
                continue
            resp.raise_for_status()
            return resp
        raise RuntimeError(f"unreachable: {url}")

    async def get_text(self, url: str, render: bool = False) -> str:
        """Fetch a page as text. Uses the browser when asked, or when HTTP is blocked."""
        if render:
            return await render_page(url)
        try:
            return (await self.get(url)).text
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in BLOCK_STATUS:
                log.info("%s blocked plain HTTP (%s), retrying in browser", url, exc.response.status_code)
                return await render_page(url)
            raise

    async def close(self) -> None:
        await self.client.aclose()


async def render_page(url: str, timeout_ms: int = 45_000) -> str:
    """Render a page in headless Chromium. Requires `pip install playwright && playwright install chromium`."""
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("Playwright is not installed; set INSTALL_PLAYWRIGHT=1 when building") from exc

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        try:
            ctx = await browser.new_context(
                user_agent=settings.user_agent,
                locale="en-US",
                viewport={"width": 1366, "height": 900},
            )
            await ctx.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            page = await ctx.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            await page.wait_for_timeout(random.randint(1200, 2500))  # let JS finish, look human
            return await page.content()
        finally:
            await browser.close()
