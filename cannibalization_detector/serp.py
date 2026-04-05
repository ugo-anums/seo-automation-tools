"""Live SERP analysis via SerpAPI for each cannibalized query."""

import os
import time
from serpapi import GoogleSearch


def analyze_serp(query: str, api_key: str | None = None) -> dict:
    """
    Fetch the live top 10 Google results for a query via SerpAPI.

    Returns a dict with:
        - organic_results: list of top 10 result dicts (title, link, snippet, position)
        - serp_intent: inferred search intent (informational / commercial / transactional / navigational)
        - dominant_types: list of dominant result types found on the SERP
        - rewarded_format: the content format Google is clearly rewarding
        - has_featured_snippet: bool
        - featured_snippet: dict with snippet details if present
    """
    key = api_key or os.environ.get("SERPAPI_API_KEY", "")
    if not key:
        raise ValueError(
            "SerpAPI key required. Set SERPAPI_API_KEY env var or pass --serpapi-key."
        )

    params = {
        "q": query,
        "api_key": key,
        "engine": "google",
        "num": 10,
        "gl": "us",
        "hl": "en",
    }

    search = GoogleSearch(params)
    raw = search.get_dict()

    # Extract organic results
    organic = []
    for r in raw.get("organic_results", [])[:10]:
        organic.append({
            "position": r.get("position", 0),
            "title": r.get("title", ""),
            "link": r.get("link", ""),
            "snippet": r.get("snippet", ""),
            "displayed_link": r.get("displayed_link", ""),
        })

    # Detect SERP features
    has_featured_snippet = "answer_box" in raw or "featured_snippet" in raw
    featured_snippet = None
    if "answer_box" in raw:
        ab = raw["answer_box"]
        featured_snippet = {
            "type": ab.get("type", "unknown"),
            "title": ab.get("title", ""),
            "snippet": ab.get("snippet", ab.get("answer", "")),
            "link": ab.get("link", ""),
        }

    has_local_pack = "local_results" in raw
    has_shopping = "shopping_results" in raw
    has_knowledge_panel = "knowledge_graph" in raw

    # Classify dominant result types from organic URLs and titles
    type_signals = {
        "blog_posts": 0,
        "service_pages": 0,
        "listicles": 0,
        "product_pages": 0,
        "directories": 0,
        "forums": 0,
    }

    for r in organic:
        link = r["link"].lower()
        title = r["title"].lower()

        if any(seg in link for seg in ["/blog/", "/article", "/post/", "/news/", "/guide"]):
            type_signals["blog_posts"] += 1
        elif any(seg in link for seg in ["/product", "/shop/", "/buy/", "/pricing"]):
            type_signals["product_pages"] += 1
        elif any(seg in link for seg in ["/services", "/solutions", "/features"]):
            type_signals["service_pages"] += 1

        if any(w in title for w in ["best ", " top ", "review", " vs "]):
            type_signals["listicles"] += 1
        if any(d in link for d in ["yelp.com", "yellowpages", "bbb.org", "g2.com"]):
            type_signals["directories"] += 1
        if any(d in link for d in ["reddit.com", "quora.com", "stackover"]):
            type_signals["forums"] += 1

    dominant_types = [
        t for t, count in sorted(type_signals.items(), key=lambda x: -x[1])
        if count >= 2
    ]
    if not dominant_types:
        dominant_types = [max(type_signals, key=type_signals.get)]

    # Extra SERP features as types
    if has_local_pack:
        dominant_types.append("local_results")
    if has_featured_snippet:
        dominant_types.append("featured_snippet")
    if has_shopping:
        dominant_types.append("shopping_results")

    # Infer search intent
    serp_intent = _infer_intent(organic, type_signals, has_local_pack, has_shopping, has_featured_snippet)

    # Determine rewarded format
    rewarded_format = _infer_rewarded_format(organic, type_signals, has_featured_snippet)

    return {
        "query": query,
        "organic_results": organic,
        "serp_intent": serp_intent,
        "dominant_types": dominant_types,
        "rewarded_format": rewarded_format,
        "has_featured_snippet": has_featured_snippet,
        "featured_snippet": featured_snippet,
    }


def _infer_intent(
    organic: list[dict],
    type_signals: dict,
    has_local: bool,
    has_shopping: bool,
    has_snippet: bool,
) -> str:
    """Classify search intent from SERP composition."""
    if has_shopping or type_signals["product_pages"] >= 3:
        return "transactional"
    if has_local and type_signals["service_pages"] >= 2:
        return "transactional"
    if type_signals["listicles"] >= 3 or (
        type_signals["listicles"] >= 2 and type_signals["product_pages"] >= 1
    ):
        return "commercial"
    if type_signals["service_pages"] >= 3:
        return "commercial"
    if type_signals["blog_posts"] >= 3 or has_snippet:
        return "informational"
    if type_signals["directories"] >= 2:
        return "navigational"

    # Fallback: if top 3 are mostly homepages or brand pages, navigational
    homepage_count = sum(
        1 for r in organic[:3]
        if r["link"].rstrip("/").count("/") <= 3
    )
    if homepage_count >= 2:
        return "navigational"

    return "informational"


def _infer_rewarded_format(
    organic: list[dict],
    type_signals: dict,
    has_snippet: bool,
) -> str:
    """Determine what content format Google is rewarding for this query."""
    if type_signals["listicles"] >= 3:
        return "listicle / comparison"
    if type_signals["blog_posts"] >= 4:
        return "long-form blog / guide"
    if type_signals["service_pages"] >= 3:
        return "service / landing page"
    if type_signals["product_pages"] >= 3:
        return "product page"
    if has_snippet and type_signals["blog_posts"] >= 2:
        return "direct-answer content"
    if type_signals["forums"] >= 2:
        return "community / forum discussion"
    return "mixed"


def analyze_serps_for_issues(
    issues: list[dict],
    api_key: str | None = None,
    delay: float = 2.0,
) -> dict[str, dict]:
    """
    Run SERP analysis for each unique query across all issues.

    Returns a dict mapping query string → SERP analysis result.
    Respects rate limits with configurable delay between calls.
    """
    results = {}
    queries = list({issue["query"] for issue in issues})

    for i, query in enumerate(queries):
        try:
            results[query] = analyze_serp(query, api_key=api_key)
        except Exception as e:
            results[query] = {
                "query": query,
                "organic_results": [],
                "serp_intent": "unknown",
                "dominant_types": [],
                "rewarded_format": "unknown",
                "has_featured_snippet": False,
                "featured_snippet": None,
                "error": str(e),
            }

        # Rate limit: delay between calls, skip after last
        if i < len(queries) - 1:
            time.sleep(delay)

    return results
