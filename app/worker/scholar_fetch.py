# app/worker/scholar_fetch.py
"""
Helpers for fetching IEEE Xplore papers from a Google Scholar URL using SerpAPI.

This module:
  1) Parses a Google Scholar search URL (or plain query string).
  2) Uses SerpAPI's google_scholar engine to fetch search results.
  3) Keeps only IEEE Xplore papers (ieeexplore.ieee.org).
  4) Returns metadata records that the worker can upload to GCS as JSONL.

Environment:
  SERPAPI_KEY must be set to a valid SerpAPI key.

Returned structure (PaperRecord.to_dict()):
  {
    "doc_id": "...",       # IEEE document ID (e.g. "4804025")
    "doc_name": "...",     # paper title
    "citations": 123,      # citation count from SerpAPI
    "abstract": "...",     # snippet/abstract text
    "ieee_url": "...",     # full IEEE URL
  }
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse, parse_qs

import requests


SERPAPI_KEY = os.getenv("SERPAPI_KEY")

if not SERPAPI_KEY:
    # You can change this to a warning if you prefer lazy checking
    raise RuntimeError("SERPAPI_KEY environment variable is not set")


@dataclass
class PaperRecord:
    """Simple container for one paper metadata record."""
    doc_id: str
    doc_name: str
    citations: int
    abstract: str
    ieee_url: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _extract_query_from_scholar_url(scholar_url: str) -> Optional[str]:
    """
    Given a Google Scholar URL, extract the 'q' query parameter.

    Example:
      https://scholar.google.com/scholar?hl=en&q=deep+learning
      -> "deep learning"
    """
    try:
        parsed = urlparse(scholar_url)
        qs = parse_qs(parsed.query)
        q_list = qs.get("q") or qs.get("as_q") or []
        if not q_list:
            return None
        raw = q_list[0]
        return raw.replace("+", " ").strip()
    except Exception:
        return None


def _is_ieee_url(url: str) -> bool:
    """Return True if the URL points to IEEE Xplore."""
    if not url:
        return False
    return "ieeexplore.ieee.org" in url


def _extract_doc_id_from_ieee_url(url: str) -> str:
    """
    Try to extract IEEE document ID from an IEEE Xplore URL.

    Common IEEE URL formats:
      https://ieeexplore.ieee.org/document/4804025/
      https://ieeexplore.ieee.org/stamp/stamp.jsp?arnumber=4804025
    """
    if not url:
        return "unknown"

    # Normalize trailing slashes
    clean = url.rstrip("/")

    # Try /document/<id>
    if "/document/" in clean:
        parts = clean.split("/document/")
        tail = parts[-1]
        if "/" in tail:
            tail = tail.split("/")[0]
        if tail.isdigit():
            return tail

    # Try arnumber parameter
    parsed = urlparse(clean)
    qs = parse_qs(parsed.query)
    arn = qs.get("arnumber", [])
    if arn and arn[0].isdigit():
        return arn[0]

    # Fallback: last segment
    last = clean.split("/")[-1]
    return last or "unknown"


def scrape_google_scholar(
    base_url_or_query: str,
    max_pages: int = 10,
    per_page: int = 10,
) -> List[Dict[str, Any]]:
    """
    Low-level SerpAPI wrapper.

    If base_url_or_query looks like a URL, we try to parse the 'q' parameter;
    otherwise we treat it as a plain query string.

    Returns a list of raw dicts from SerpAPI, each corresponding to an organic result.
    """
    # Decide whether this is a full URL or just a query string
    if base_url_or_query.startswith("http://") or base_url_or_query.startswith("https://"):
        query = _extract_query_from_scholar_url(base_url_or_query)
        if not query:
            # Fallback: use the full string as query
            query = base_url_or_query
    else:
        query = base_url_or_query

    print(f"[scholar_fetch] Scraping Google Scholar via SerpAPI for query: {query!r}")

    all_results: List[Dict[str, Any]] = []

    for page in range(max_pages):
        start = page * per_page
        print(f"[scholar_fetch] Fetching page {page + 1}/{max_pages} (start={start})...")

        serpapi_params = {
            "engine": "google_scholar",
            "q": query,
            "start": start,
            "api_key": SERPAPI_KEY,
            "num": per_page,
        }

        try:
            resp = requests.get("https://serpapi.com/search", params=serpapi_params, timeout=20)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"[scholar_fetch] Error fetching page {page + 1}: {e}")
            break

        if "error" in data:
            print(f"[scholar_fetch] SerpAPI error: {data['error']}")
            break

        organic_results = data.get("organic_results", [])
        if not organic_results:
            print(f"[scholar_fetch] No more results on page {page + 1}")
            break

        all_results.extend(organic_results)

        # Simple throttle to respect SerpAPI rate limits
        time.sleep(1.0)

    print(f"[scholar_fetch] Total organic results collected: {len(all_results)}")
    return all_results


def fetch_ieee_papers_from_scholar(
    base_url_or_query: str,
    max_pages: int = 10,
    max_papers: int = 50,
) -> List[PaperRecord]:
    """
    High-level helper for the worker.

    1) Call scrape_google_scholar(...) to get SerpAPI organic results.
    2) Filter to keep only IEEE Xplore URLs.
    3) Extract:
         - doc_id  (from IEEE URL)
         - doc_name (title)
         - citations (inline_links.cited_by.total)
         - abstract (snippet)
         - ieee_url (link)
    4) Return a list of PaperRecord.
    """
    raw_results = scrape_google_scholar(base_url_or_query, max_pages=max_pages)
    papers: List[PaperRecord] = []

    for page_idx, result in enumerate(raw_results):
        if len(papers) >= max_papers:
            break

        try:
            title = result.get("title", "") or ""
            link = result.get("link", "") or ""
            snippet = result.get("snippet", "") or ""

            if not _is_ieee_url(link):
                continue

            doc_id = _extract_doc_id_from_ieee_url(link)

            citations = 0
            inline_links = result.get("inline_links", {})
            cited_by = inline_links.get("cited_by", {})
            if isinstance(cited_by, dict) and "total" in cited_by:
                try:
                    citations = int(cited_by["total"])
                except (TypeError, ValueError):
                    citations = 0

            rec = PaperRecord(
                doc_id=doc_id,
                doc_name=title or "N/A",
                citations=citations,
                abstract=snippet,
                ieee_url=link,
            )
            papers.append(rec)

            print(
                f"[scholar_fetch] IEEE paper found: {rec.doc_name[:60]!r}, "
                f"citations={rec.citations}, doc_id={rec.doc_id}"
            )

        except Exception as e:
            print(f"[scholar_fetch] Error parsing organic result on index={page_idx}: {e}")
            continue

    print(f"[scholar_fetch] Total IEEE papers collected: {len(papers)}")
    return papers


# Optional CLI for local testing
if __name__ == "__main__":
    print("Enter either a Google Scholar URL or a plain query (e.g. 'deep learning'):")
    s = input("Query or URL: ").strip()
    ieee_papers = fetch_ieee_papers_from_scholar(s, max_pages=3, max_papers=20)
    print(f"\n=== Summary ({len(ieee_papers)} papers) ===")
    for p in ieee_papers:
        print(p.to_dict())
