"""Supreme Court E-Library scraper (third source).

Source is a JSON API: ``POST {base}/republic_acts/fetch_ra`` returning
DataTables-shaped responses. Classified as ``elibrary_json`` in the data
contract.

Flow:
  1. GET  {base}/republic_acts              -> HTML shell, sets session
                                               cookie, contains CSRF token
  2. POST {base}/republic_acts/fetch_ra     -> JSON:
       {draw, recordsTotal, recordsFiltered,
        data: [[title, date, html_anchor], ...]}
  3. Paginate ``start`` in steps of ``elibrary_page_size`` (default 500).

Raw JSONs land in ``data/raw/elibrary/pages/page_NNNN.json``; a run summary
lands in ``data/raw/elibrary/_manifest.json``.

Idempotency: a page file that already exists is reused, not re-fetched. If a
prior run left ``_manifest.json`` with ``complete: true`` and the caller did
not pass ``--sample`` or ``--force``, the scrape is a no-op.

TLS: the E-Library server sends an incomplete certificate chain — its leaf's
issuer is omitted. Windows CryptoAPI silently AIA-fetches the missing
intermediate; OpenSSL does not, which would break the Docker container. We
ship the intermediate under ``certs/`` and concatenate it with certifi's
roots into a runtime bundle used as ``verify=``. See ``_make_session``.
"""

import argparse
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import certifi
import requests

from src.utils.config import settings
from src.utils.io import atomic_write_json, atomic_write_text
from src.utils.logger import get_logger

logger = get_logger(__name__)

INDEX_PATH = "/republic_acts"
ENDPOINT_PATH = "/republic_acts/fetch_ra"
CSRF_RE = re.compile(r"csrf_test_name['\"]?\s*:\s*['\"]([^'\"]+)")
N_COLUMNS = 3  # [title, date, html_anchor]

# Full browser-realistic fingerprints. The E-Library sits behind a WAF that
# returns an IIS 500 on requests that look like bots. Do not shorten these.
_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

HEADERS_GET = {
    "User-Agent": _BROWSER_UA,
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

HEADERS_POST = {
    "User-Agent": _BROWSER_UA,
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}


_CERT_DIR = Path(__file__).resolve().parents[2] / "certs"
_ELIBRARY_INTERMEDIATE = _CERT_DIR / "gsgccr3evtlsca2025.pem"


def _make_session() -> requests.Session:
    """Return a requests Session bound to a deterministic CA bundle.

    The E-Library server sends an incomplete chain — its leaf's real issuer
    (``GlobalSign GCC R3 EV TLS CA 2025``) is omitted, and unrelated
    GlobalSign certificates are sent instead. Windows CryptoAPI silently
    AIA-fetches the missing intermediate; OpenSSL does not, so the
    container would fail with ``unable to get local issuer certificate``.

    We ship the intermediate under ``certs/`` (sourced from the leaf's own
    AIA extension at ``secure.globalsign.com``, chain-verified before
    commit) and concatenate it with certifi's roots into a runtime bundle
    that is passed as ``verify=`` on the session. Same verification result
    on Windows and Linux, no OS-level AIA dependency.
    """
    if not _ELIBRARY_INTERMEDIATE.exists():
        raise RuntimeError(
            f"Missing intermediate certificate: {_ELIBRARY_INTERMEDIATE}. "
            "See docs/architecture.md for the source AIA URL."
        )

    bundle_path = settings.data_dir / "ca_bundle_elibrary.pem"
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    bundle_path.write_bytes(
        Path(certifi.where()).read_bytes()
        + b"\n"
        + _ELIBRARY_INTERMEDIATE.read_bytes()
    )

    session = requests.Session()
    session.verify = str(bundle_path)
    return session


# ---------------------------------------------------------------------------
# CSRF + payload helpers
# ---------------------------------------------------------------------------

def _extract_csrf(shell_html: str) -> str:
    """Pull the CodeIgniter CSRF token out of the shell page.

    Verified pattern (Handoff v4 §7): ``csrf_test_name: "<token>"``.
    Token does not rotate between requests (verified in recon).
    """
    match = CSRF_RE.search(shell_html)
    if not match:
        raise RuntimeError(
            "CSRF token not found in shell page — page structure may have "
            "changed. Expected pattern: csrf_test_name['\"]?\\s*:\\s*['\"]([^'\"]+)"
        )
    return match.group(1)


def _build_payload(draw: int, start: int, length: int, csrf: str) -> dict[str, str]:
    """Build the DataTables server-side payload.

    Shape captured from a real browser request (verified 2026-10-04). Two
    details this endpoint is strict about — get either wrong and the server
    answers HTTP 500 with an IIS boilerplate page:

      * Per-column ``orderable`` must match the page's ``columnDefs``:
        columns 0 and 2 are ``false``, column 1 is ``true``.
      * The ``order[*]`` keys must be *absent*. The page initialises
        DataTables with ``"order": []`` and sends no ordering parameters;
        inventing ``order[0][column]=0`` makes the query builder fatal on a
        non-orderable column.
    """
    payload: dict[str, str] = {
        "draw": str(draw),
        "start": str(start),
        "length": str(length),
        "search[value]": "",
        "search[regex]": "false",
    }
    orderable_by_index = {0: "false", 1: "true", 2: "false"}
    for i in range(N_COLUMNS):
        payload[f"columns[{i}][data]"] = str(i)
        payload[f"columns[{i}][name]"] = ""
        payload[f"columns[{i}][searchable]"] = "true"
        payload[f"columns[{i}][orderable]"] = orderable_by_index[i]
        payload[f"columns[{i}][search][value]"] = ""
        payload[f"columns[{i}][search][regex]"] = "false"
    payload["csrf_test_name"] = csrf
    return payload


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def _fetch_shell(session: requests.Session, base_url: str) -> str:
    url = base_url + INDEX_PATH
    logger.info("Fetching shell: %s", url)
    r = session.get(url, headers=HEADERS_GET, timeout=30)
    r.raise_for_status()
    return r.text


def _post_page(
    session: requests.Session,
    base_url: str,
    payload: dict[str, str],
    referer: str,
) -> dict[str, Any]:
    url = base_url + ENDPOINT_PATH
    headers = {**HEADERS_POST, "Referer": referer, "Origin": base_url}
    r = session.post(url, data=payload, headers=headers, timeout=60)

    if not r.ok:
        # Show the server's own error — CodeIgniter 500s usually include a
        # stack-trace fragment in the body that names the missing field.
        body = r.text[:2000]
        logger.error(
            "POST %s -> HTTP %d\nPayload keys: %s\nBody[:2000]: %s",
            url,
            r.status_code,
            sorted(payload.keys()),
            body,
        )
        r.raise_for_status()
    try:
        return r.json()
    except ValueError as e:
        raise RuntimeError(
            f"E-Library returned non-JSON response (HTTP {r.status_code}); "
            f"first 200 chars: {r.text[:200]!r}"
        ) from e


# ---------------------------------------------------------------------------
# Main scrape
# ---------------------------------------------------------------------------

def _save_page(pages_dir: Path, page_idx: int, resp: dict[str, Any]) -> Path:
    dest = pages_dir / f"page_{page_idx:04d}.json"
    return atomic_write_json(resp, dest)


def scrape_elibrary(
    sample_pages: int | None = None,
    polite_delay: float = 1.0,
    force: bool = False,
) -> dict[str, Any]:
    """Fetch E-Library RA index pages, saving raw JSON per page.

    ``force=True`` re-fetches every page even when a complete manifest exists.
    Per-page skip-if-exists is disabled in that case, so this re-downloads the
    full index. Used by the DAG's ``force_ingest=true`` path.

    Returns the manifest dict (also written to disk).
    """

    base = settings.elibrary_base_url.rstrip("/")
    page_size = settings.elibrary_page_size

    out_dir = settings.raw_dir / "elibrary"
    pages_dir = out_dir / "pages"
    pages_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = out_dir / "_manifest.json"

    # Whole-run skip: complete manifest + no --sample override + not forced.
    if not force and sample_pages is None and manifest_path.exists():
        try:
            prior = json.loads(manifest_path.read_text())
            if prior.get("complete") is True:
                logger.info(
                    "E-Library manifest already complete "
                    "(%d pages, %d rows); skipping scrape.",
                    prior.get("pages", 0),
                    prior.get("records_total", 0),
                )
                return prior
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "Could not read existing manifest (%s); re-scraping.", e
            )

    session = _make_session()

    shell_html = _fetch_shell(session, base)
    atomic_write_text(shell_html, out_dir / "_shell.html")

    csrf = _extract_csrf(shell_html)
    logger.info("Extracted CSRF token (%d chars)", len(csrf))

    referer = base + INDEX_PATH

    # First POST: also yields recordsTotal so we know how many pages to fetch.
    first_resp = _post_page(
        session, base, _build_payload(1, 0, page_size, csrf), referer
    )
    records_total = int(first_resp.get("recordsTotal", 0))
    records_filtered = int(first_resp.get("recordsFiltered", records_total))
    if records_total <= 0:
        raise RuntimeError(
            f"E-Library reported recordsTotal={records_total}; aborting."
        )

    total_pages = (records_total + page_size - 1) // page_size
    pages_to_fetch = (
        total_pages if sample_pages is None else min(sample_pages, total_pages)
    )

    logger.info(
        "E-Library reports recordsTotal=%d (filtered=%d) -> %d pages; "
        "fetching %d.",
        records_total,
        records_filtered,
        total_pages,
        pages_to_fetch,
    )

    _save_page(pages_dir, 1, first_resp)
    fetched_rows = len(first_resp.get("data", []))
    logger.info(
        "[1/%d] page_0001.json saved (%d rows, start=0)",
        pages_to_fetch,
        fetched_rows,
    )
    time.sleep(polite_delay)

    # Remaining pages.
    for page_idx in range(2, pages_to_fetch + 1):
        start = (page_idx - 1) * page_size
        dest = pages_dir / f"page_{page_idx:04d}.json"
        if dest.exists() and not force:
            logger.info(
                "[%d/%d] page file exists, skipping fetch",
                page_idx,
                pages_to_fetch,
            )
            try:
                cached = json.loads(dest.read_text())
                fetched_rows += len(cached.get("data", []))
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "Could not read cached %s (%s); will count as 0 rows.",
                    dest.name,
                    e,
                )
            continue

        payload = _build_payload(page_idx, start, page_size, csrf)
        resp = _post_page(session, base, payload, referer)
        _save_page(pages_dir, page_idx, resp)
        rows_this_page = len(resp.get("data", []))
        fetched_rows += rows_this_page
        logger.info(
            "[%d/%d] page_%04d.json saved (%d rows, start=%d)",
            page_idx,
            pages_to_fetch,
            page_idx,
            rows_this_page,
            start,
        )
        time.sleep(polite_delay)

    complete = (sample_pages is None) and (pages_to_fetch == total_pages)
    manifest: dict[str, Any] = {
        "source": "elibrary_json",
        "base_url": base,
        "index_url": base + INDEX_PATH,
        "endpoint": base + ENDPOINT_PATH,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "page_size": page_size,
        "pages": pages_to_fetch,
        "records_total": records_total,
        "records_filtered": records_filtered,
        "records_fetched": fetched_rows,
        "complete": complete,
        "sample": sample_pages is not None,
        "sample_pages": sample_pages,
    }
    atomic_write_json(manifest, manifest_path)
    logger.info(
        "E-Library scrape done: %d pages, %d rows, complete=%s",
        pages_to_fetch,
        fetched_rows,
        complete,
    )
    return manifest


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Scrape the Supreme Court E-Library Republic Acts index "
            "(POST /republic_acts/fetch_ra)."
        )
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=None,
        metavar="N",
        help="Fetch only the first N pages (dev/testing). Default: fetch all.",
    )
    parser.add_argument(
        "--polite-delay",
        type=float,
        default=1.0,
        help="Seconds to sleep between POST requests. Default: 1.0",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Re-fetch all pages even if a complete manifest exists. "
            "Used by the DAG's force_ingest=true path."
        ),
    )
    args = parser.parse_args()
    scrape_elibrary(
        sample_pages=args.sample,
        polite_delay=args.polite_delay,
        force=args.force,
    )


if __name__ == "__main__":
    main()