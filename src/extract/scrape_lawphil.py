import json
import random
import time
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from src.utils.config import settings
from src.utils.logger import get_logger

import unicodedata

logger = get_logger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; RA-DE-Project/1.0)"}

def _clean_href(raw: str) -> str:
    """Normalize and remove all quote-like characters anywhere in the href."""
    if not raw:
        return ""
    normalized = unicodedata.normalize("NFKC", raw).strip()
    for ch in ('"', "'", "\u201c", "\u201d", "\u2018", "\u2019"):
        normalized = normalized.replace(ch, "")
    return normalized

def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    # Lawphil mixes encodings; apparent_encoding sniffs correctly.
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text


def parse_index(index_html: str, index_url: str) -> list[dict]:
    """Parse every RA row from the single Lawphil index page."""
    soup = BeautifulSoup(index_html, "lxml")
    table = soup.find("table", id="s-menu")
    if table is None:
        raise RuntimeError("Could not find <table id='s-menu'> in the index page")

    entries: list[dict] = []
    for tr in table.find_all("tr", class_="xy"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        first_td = tds[0]
        link = first_td.find("a", href=True)
        if not link:
            continue

        href = _clean_href(link["href"])
        full_url = urljoin(index_url, href)
        ra_label = link.get_text(" ", strip=True)

        # The date sits in the same <td> after a <br>
        date_text = ""
        for br in first_td.find_all("br"):
            nxt = br.next_sibling
            if nxt:
                date_text = str(nxt).strip()
                break

        title = tds[1].get_text(" ", strip=True) if len(tds) > 1 else ""

        entries.append({
            "ra_label": ra_label,
            "url": full_url,
            "date_text": date_text,
            "title": title,
            "source_path": href,   # e.g. "ra2024/ra_12036_2024.html"
        })

    return entries


def scrape_lawphil_sample(
    sample_size: int | None = None,
    seed: int = 42,
    polite_delay: float = 0.5,
) -> int:
    sample_size = sample_size or settings.lawphil_sample_size
    out_dir = settings.raw_dir / "lawphil"
    out_dir.mkdir(parents=True, exist_ok=True)

    index_url = settings.lawphil_index_url
    logger.info("Fetching index: %s", index_url)
    index_html = fetch_html(index_url)
    (out_dir / "_index.html").write_text(index_html, encoding="utf-8")

    entries = parse_index(index_html, index_url)
    logger.info("Parsed %d RA entries from index", len(entries))

    if not entries:
        raise RuntimeError("No RA entries found — index structure may have changed")

    rng = random.Random(seed)
    sample = rng.sample(entries, min(sample_size, len(entries)))
    logger.info("Sampled %d RAs (seed=%d)", len(sample), seed)

    (out_dir / "_sample_manifest.json").write_text(
        json.dumps(sample, indent=2), encoding="utf-8"
    )

    pages_dir = out_dir / "pages"
    pages_dir.mkdir(parents=True, exist_ok=True)

    saved = 0
    for i, entry in enumerate(sample, start=1):
        slug = entry["source_path"].replace("/", "__")
        dest = pages_dir / slug
        if dest.exists():
            saved += 1
            continue
        try:
            html = fetch_html(entry["url"])
            dest.write_text(html, encoding="utf-8")
            saved += 1
            logger.info("[%d/%d] saved %s", i, len(sample), slug)
            time.sleep(polite_delay)
        except Exception as e:
            logger.warning("Failed %s: %s", entry["url"], e)

    logger.info("Scrape complete. Saved %d / %d pages.", saved, len(sample))
    return saved


if __name__ == "__main__":
    scrape_lawphil_sample()