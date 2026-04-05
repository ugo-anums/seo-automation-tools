"""Fetch search analytics data from Google Search Console."""

import re
import time
from datetime import datetime, timedelta
from collections import defaultdict
from urllib.parse import urlparse


def fetch_search_analytics(service, site_url: str, months: int = 3, row_limit: int = 25000) -> list[dict]:
    """
    Pull query+page data from GSC for the specified date range.

    Returns a list of rows with keys: query, page, clicks, impressions, ctr, position.
    Uses pagination to retrieve all available data.
    """
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=months * 30)

    all_rows = []
    start_row = 0

    while True:
        request_body = {
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "dimensions": ["query", "page"],
            "rowLimit": row_limit,
            "startRow": start_row,
            "dataState": "final",
        }

        response = service.searchanalytics().query(
            siteUrl=site_url, body=request_body
        ).execute()

        rows = response.get("rows", [])
        if not rows:
            break

        for row in rows:
            all_rows.append({
                "query": row["keys"][0],
                "page": row["keys"][1],
                "clicks": row.get("clicks", 0),
                "impressions": row.get("impressions", 0),
                "ctr": round(row.get("ctr", 0), 4),
                "position": round(row.get("position", 0), 1),
            })

        start_row += row_limit

        if len(rows) < row_limit:
            break

        time.sleep(0.5)  # rate-limit courtesy

    return all_rows


def group_by_query(rows: list[dict]) -> dict[str, list[dict]]:
    """Group fetched rows by query keyword, keeping only queries with 2+ pages."""
    query_pages = defaultdict(list)

    for row in rows:
        query_pages[row["query"]].append({
            "page": row["page"],
            "clicks": row["clicks"],
            "impressions": row["impressions"],
            "ctr": row["ctr"],
            "position": row["position"],
        })

    # Only return queries where multiple pages are competing
    return {
        query: pages
        for query, pages in query_pages.items()
        if len(pages) >= 2
    }


def build_page_footprints(rows: list[dict]) -> dict[str, dict]:
    """
    Build the full keyword footprint for every page URL in the dataset.

    Uses the already-fetched GSC rows (query+page) to avoid extra API calls.

    Returns a dict mapping page URL → {
        "total_queries": int,
        "total_impressions": int,
        "total_clicks": int,
        "top_queries": list of top 5 queries by impressions (dicts with query, impressions, clicks, position),
    }
    """
    page_data: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        page_data[row["page"]].append({
            "query": row["query"],
            "clicks": row["clicks"],
            "impressions": row["impressions"],
            "position": row["position"],
        })

    footprints = {}
    for page_url, queries in page_data.items():
        sorted_queries = sorted(queries, key=lambda q: q["impressions"], reverse=True)
        footprints[page_url] = {
            "total_queries": len(queries),
            "total_impressions": sum(q["impressions"] for q in queries),
            "total_clicks": sum(q["clicks"] for q in queries),
            "top_queries": sorted_queries[:5],
        }

    return footprints


def extract_brand_terms(site_url: str) -> list[str]:
    """
    Derive brand terms from a GSC site URL.

    Handles both 'https://www.example.com' and 'sc-domain:example.com' formats.
    Returns the bare domain name (e.g. 'example') and common variants.
    """
    url = site_url.strip()
    if url.startswith("sc-domain:"):
        domain = url.split(":", 1)[1]
    else:
        domain = urlparse(url).hostname or url

    # Strip www. and TLD
    domain = re.sub(r"^www\.", "", domain)
    base = domain.split(".")[0]  # e.g. "example" from "example.co.uk"

    terms = {base.lower()}
    # Also catch hyphenated brands as separate words: "my-brand" → "my brand", "mybrand"
    if "-" in base:
        terms.add(base.replace("-", " ").lower())
        terms.add(base.replace("-", "").lower())

    return list(terms)


def filter_branded_queries(
    cannibalized: dict[str, list[dict]],
    brand_terms: list[str],
    extra_terms: list[str] | None = None,
) -> dict[str, list[dict]]:
    """
    Remove queries that contain any brand term.

    Args:
        cannibalized: query → pages dict from group_by_query().
        brand_terms: auto-detected brand terms from the domain.
        extra_terms: additional brand terms supplied by the user.
    """
    all_terms = [t.lower() for t in brand_terms]
    if extra_terms:
        all_terms.extend(t.lower().strip() for t in extra_terms if t.strip())

    filtered = {}
    for query, pages in cannibalized.items():
        query_lower = query.lower()
        if not any(term in query_lower for term in all_terms):
            filtered[query] = pages

    return filtered
