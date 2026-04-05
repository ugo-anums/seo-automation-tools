"""
Cannibalization detection engine with full-context, Claude-powered recommendations.

Pipeline per query:
  1. GSC metrics (from fetcher)
  2. Live SERP analysis via SerpAPI (from serp)
  3. Page content snapshots via Playwright (from scraper)
  4. Context packet assembled and sent to Claude
  5. Structured recommendation returned
"""

import json
import re
from collections import defaultdict
from urllib.parse import urlparse

import anthropic
import numpy as np


# ---------------------------------------------------------------------------
# Percentile-based volume scoring
# ---------------------------------------------------------------------------

def _build_impression_percentiles(cannibalized_queries: dict[str, list[dict]]) -> np.ndarray:
    """Collect total impressions per query and return the sorted array for percentile lookup."""
    totals = []
    for pages in cannibalized_queries.values():
        totals.append(sum(p["impressions"] for p in pages))
    return np.array(sorted(totals))


def _percentile_score(value: float, distribution: np.ndarray) -> float:
    """Return a 0-1 score representing where *value* falls in *distribution*."""
    if len(distribution) == 0:
        return 0.0
    idx = np.searchsorted(distribution, value, side="right")
    return idx / len(distribution)


# ---------------------------------------------------------------------------
# Severity scoring
# ---------------------------------------------------------------------------

def detect_cannibalization(cannibalized_queries: dict[str, list[dict]]) -> list[dict]:
    """
    Score cannibalization severity for each query with 2+ competing pages.

    Uses percentile-based volume scoring against the dataset's own distribution.
    Returns issues sorted by severity (highest first). Recommendations are empty
    at this stage — they are populated later by generate_recommendations().
    """
    impression_dist = _build_impression_percentiles(cannibalized_queries)
    issues = []

    for query, pages in cannibalized_queries.items():
        pages_sorted = sorted(pages, key=lambda p: p["clicks"], reverse=True)
        winner = pages_sorted[0]
        losers = pages_sorted[1:]

        total_clicks = sum(p["clicks"] for p in pages)
        total_impressions = sum(p["impressions"] for p in pages)
        wasted_impressions = sum(p["impressions"] for p in losers)

        positions = [p["position"] for p in pages]
        position_spread = max(positions) - min(positions)

        click_shares = [p["clicks"] / max(total_clicks, 1) for p in pages]
        click_entropy = -sum(
            s * np.log2(max(s, 1e-10)) for s in click_shares
        ) / max(np.log2(len(pages)), 1)

        volume_score = _percentile_score(total_impressions, impression_dist)
        spread_penalty = max(0, 1 - position_spread / 20)

        severity = round(
            (click_entropy * 40 + spread_penalty * 30 + volume_score * 30), 1
        )
        severity = min(severity, 100)

        issues.append({
            "query": query,
            "severity": severity,
            "page_count": len(pages),
            "total_clicks": total_clicks,
            "total_impressions": total_impressions,
            "wasted_impressions": wasted_impressions,
            "winner": {
                "page": winner["page"],
                "clicks": winner["clicks"],
                "impressions": winner["impressions"],
                "position": winner["position"],
                "ctr": winner["ctr"],
            },
            "competing_pages": [
                {
                    "page": p["page"],
                    "clicks": p["clicks"],
                    "impressions": p["impressions"],
                    "position": p["position"],
                    "ctr": p["ctr"],
                }
                for p in losers
            ],
            # Populated by generate_recommendations()
            "serp_analysis": None,
            "page_snapshots": {},
            "recommendation": None,
        })

    issues.sort(key=lambda x: x["severity"], reverse=True)
    return issues


# ---------------------------------------------------------------------------
# Context packet assembly
# ---------------------------------------------------------------------------

def enrich_issues(
    issues: list[dict],
    serp_results: dict[str, dict],
    page_snapshots: dict[str, dict],
    page_footprints: dict[str, dict] | None = None,
) -> list[dict]:
    """
    Attach SERP analysis, page snapshots, and keyword footprints to each issue.
    """
    for issue in issues:
        issue["serp_analysis"] = serp_results.get(issue["query"])

        snapshots = {}
        winner_url = issue["winner"]["page"]
        if winner_url in page_snapshots:
            snapshots[winner_url] = page_snapshots[winner_url]
        for p in issue["competing_pages"]:
            if p["page"] in page_snapshots:
                snapshots[p["page"]] = page_snapshots[p["page"]]
        issue["page_snapshots"] = snapshots

        # Attach footprints for every page in the issue
        footprints = {}
        if page_footprints:
            all_urls = [winner_url] + [p["page"] for p in issue["competing_pages"]]
            for url in all_urls:
                if url in page_footprints:
                    fp = page_footprints[url]
                    # Build "other queries" excluding the current cannibalized query
                    other_queries = [
                        q for q in fp["top_queries"] if q["query"] != issue["query"]
                    ]
                    footprints[url] = {
                        "total_queries": fp["total_queries"],
                        "total_impressions": fp["total_impressions"],
                        "total_clicks": fp["total_clicks"],
                        "other_top_queries": other_queries[:3],
                    }
        issue["page_footprints"] = footprints

    return issues


# ---------------------------------------------------------------------------
# Claude-powered full-context recommendations
# ---------------------------------------------------------------------------

_RECOMMENDATION_SYSTEM = """You are an expert SEO consultant analyzing keyword cannibalization with complete data: GSC metrics, live SERP analysis, actual page content from every competing URL (including current title, meta description, and H1), AND the full keyword footprint of every page.

For each issue, you will receive:
- The cannibalized query with GSC metrics for every competing page
- Live SERP top 10 results showing what Google is actually rewarding
- Full page snapshots with CURRENT TITLE, CURRENT META DESCRIPTION, CURRENT H1, H2s, word count, page type, and CTA for every competing URL
- KEYWORD FOOTPRINT for every page: total queries it ranks for, total impressions across all queries, and its top other queries

You must return a JSON object with exactly these fields:
{
  "recommended_winner": "<URL of the page that should own this query>",
  "action_type": "<one of: differentiate-by-intent | add-canonical | 301-redirect | noindex-fragments | monitor-only>",
  "serp_intent": "<informational | commercial | transactional | navigational>",
  "reasoning": "<2-4 sentence plain-English explanation of WHY this is the right action, grounded in what you see on the pages, the SERP, AND the keyword footprint data. If a page ranks for many other keywords, explicitly state how many would be affected.>",
  "page_actions": [
    {
      "url": "<page URL>",
      "page_type": "<detected page type>",
      "action": "<specific action for this page>",
      "changes": "<Write a single flowing paragraph — like a sentence in a client email — that naturally incorporates: what the current title/H1 actually says (quote it), what specifically is wrong with it for this query, and the exact change needed with why it will help. Do NOT use labels like 'Current state:' or 'Problem:' or 'Fix:' — just write it as one cohesive recommendation.>"
    }
  ]
}

CRITICAL RULES — VIOLATING THESE MAKES YOUR RECOMMENDATION HARMFUL:

1. KEYWORD FOOTPRINT PROTECTION: If a competing page ranks for MORE THAN 5 other queries OR has MORE THAN 200 total impressions across all its queries, you MUST NOT recommend a 301 redirect for that page. Use "differentiate-by-intent" or "add-canonical" instead. In your reasoning, explicitly state: "This page ranks for [N] other queries with [X] total impressions — a redirect would destroy these rankings."

2. NEVER recommend redirecting a blog post, article, or content page to the homepage or to a different page WITHOUT explicitly warning that this will destroy every other keyword ranking that page currently holds. If the page has any impressions or rankings of its own, default to differentiation instead.

3. NEVER suggest writing competitor listicles or comparison posts on a service business site.

4. NEVER include specific years in suggested title tags or headings.

5. When BOTH competing pages are below position 40 with zero clicks, default to "monitor-only" UNLESS there is a clear structural fix (duplicate content, missing canonical, identical title tags).

6. Base EVERY recommendation on what is ACTUALLY on the pages. You have the real page snapshots with current titles, meta descriptions, and H1s. NEVER suggest a title or heading change without first quoting the current title and explaining specifically what is wrong with it and why the proposed change would improve rankings for this specific query.

7. When a competing page is a blog post or article, DEFAULT to "differentiate-by-intent".

8. The "recommended_winner" must be the page that best matches what the SERP is rewarding, NOT necessarily the page with the most clicks.

9. NAVIGATIONAL QUERY RULE: For navigational queries (brand name searches) where both competing pages belong to the same domain and serve the same brand (e.g. homepage vs about page), the recommendation MUST be "monitor-only". Explain that navigational cannibalization within the same brand is not a problem — users searching a brand name landing on either the homepage or about page are being served correctly. Do not suggest changes for navigational brand queries.

10. CANONICAL TAG RULE: NEVER suggest a canonical tag from one page to another on the same site UNLESS the two pages have substantially duplicate content (same topic, same structure, overlapping text). For pages that serve different user needs — even if they share keyword overlap — recommend "differentiate-by-intent", NOT "add-canonical". A service page and a blog post about the same topic are NOT duplicate content.

11. TITLE CHANGE THRESHOLD: NEVER suggest a title change that is less than 20% different from the existing title. If the existing title already contains the right target keywords, say so explicitly in the "changes" field and recommend a different optimization lever instead — internal linking improvements, content depth expansion, schema markup additions, or page speed optimization. Only suggest a title rewrite when the current title genuinely misses the target intent.

12. PAGE ACTION STRUCTURE: Every "changes" field in page_actions MUST be written as a single flowing paragraph — like a sentence in a client email, NOT a labeled form. It must naturally incorporate: what the current title/H1 actually says (quote it verbatim), what specifically is wrong with it for this query, and the exact change needed with why it will help. Do NOT use labels like "Current state:", "Problem:", or "Fix:" — write it as one cohesive, readable recommendation. If no on-page change is needed, quote the current title/H1, explain why it's already appropriate, then suggest the alternative lever (internal linking, content depth, schema markup, etc).

Return ONLY the JSON object. No markdown fences, no commentary."""


def _build_context_prompt(issue: dict) -> str:
    """Assemble the full context packet for a single cannibalization issue."""
    lines = []

    # --- GSC metrics ---
    lines.append("=" * 60)
    lines.append(f"QUERY: \"{issue['query']}\"")
    lines.append(f"Severity score: {issue['severity']} / 100")
    lines.append(f"Total clicks: {issue['total_clicks']}  |  Total impressions: {issue['total_impressions']}")
    lines.append("")

    w = issue["winner"]
    lines.append("GSC METRICS — Winner (most clicks):")
    lines.append(f"  URL: {w['page']}")
    lines.append(f"  Clicks: {w['clicks']}  |  Impressions: {w['impressions']}  |  Position: {w['position']}  |  CTR: {w['ctr']:.2%}")
    lines.append("")

    lines.append("GSC METRICS — Competing pages:")
    for p in issue["competing_pages"]:
        lines.append(f"  URL: {p['page']}")
        lines.append(f"  Clicks: {p['clicks']}  |  Impressions: {p['impressions']}  |  Position: {p['position']}  |  CTR: {p['ctr']:.2%}")
    lines.append("")

    # --- SERP analysis ---
    serp = issue.get("serp_analysis")
    if serp and not serp.get("error"):
        lines.append("LIVE SERP ANALYSIS (Google top 10):")
        lines.append(f"  Detected intent: {serp['serp_intent']}")
        lines.append(f"  Dominant result types: {', '.join(serp['dominant_types'])}")
        lines.append(f"  Rewarded format: {serp['rewarded_format']}")
        lines.append(f"  Featured snippet: {'Yes' if serp['has_featured_snippet'] else 'No'}")
        if serp.get("featured_snippet"):
            fs = serp["featured_snippet"]
            lines.append(f"  Snippet source: {fs.get('link', 'N/A')}")
        lines.append("")
        lines.append("  Top 10 results:")
        for r in serp["organic_results"]:
            lines.append(f"    #{r['position']}: {r['title']}")
            lines.append(f"       {r['link']}")
        lines.append("")
    elif serp and serp.get("error"):
        lines.append(f"LIVE SERP ANALYSIS: FAILED — {serp['error']}")
        lines.append("")
    else:
        lines.append("LIVE SERP ANALYSIS: Not available")
        lines.append("")

    # --- Page snapshots (with prominent title/meta/H1) ---
    snapshots = issue.get("page_snapshots", {})
    if snapshots:
        lines.append("PAGE CONTENT SNAPSHOTS (you MUST reference these titles/H1s in your recommendations):")
        for url, snap in snapshots.items():
            lines.append(f"  --- {url} ---")
            if snap.get("fetch_error"):
                lines.append(f"  FETCH ERROR: {snap['fetch_error']}")
                lines.append("")
                continue
            lines.append(f"  Page type: {snap['page_type']}")
            lines.append(f"  CURRENT TITLE: \"{snap['title']}\"")
            lines.append(f"  CURRENT META DESCRIPTION: \"{snap['meta_description'][:300]}\"")
            lines.append(f"  CURRENT H1: \"{snap['h1']}\"")
            if snap["h2s"]:
                lines.append(f"  H2s: {' | '.join(snap['h2s'][:10])}")
            lines.append(f"  Word count: {snap['word_count']}")
            if snap["primary_cta"]:
                lines.append(f"  Primary CTA: {snap['primary_cta']}")
            lines.append("")
    else:
        lines.append("PAGE CONTENT SNAPSHOTS: Not available")
        lines.append("")

    # --- Keyword footprints ---
    footprints = issue.get("page_footprints", {})
    if footprints:
        lines.append("KEYWORD FOOTPRINTS (full GSC ranking data per page):")
        for url, fp in footprints.items():
            lines.append(f"  --- {url} ---")
            lines.append(f"  Total queries this page ranks for: {fp['total_queries']}")
            lines.append(f"  Total impressions across ALL queries: {fp['total_impressions']:,}")
            lines.append(f"  Total clicks across ALL queries: {fp['total_clicks']:,}")
            redirect_safe = fp["total_queries"] <= 5 and fp["total_impressions"] <= 200
            lines.append(f"  Redirect safe: {'YES — low footprint' if redirect_safe else 'NO — redirect would damage ' + str(fp['total_queries']) + ' other keyword rankings'}")
            if fp["other_top_queries"]:
                lines.append(f"  Top other queries (besides \"{issue['query']}\"):")
                for oq in fp["other_top_queries"]:
                    lines.append(f"    - \"{oq['query']}\" (impr: {oq['impressions']}, clicks: {oq['clicks']}, pos: {oq['position']})")
            else:
                lines.append("  No other queries — this page only ranks for the cannibalized query")
            lines.append("")
    else:
        lines.append("KEYWORD FOOTPRINTS: Not available")
        lines.append("")

    return "\n".join(lines)


def generate_recommendations(
    issues: list[dict],
    api_key: str | None = None,
) -> list[dict]:
    """
    Send each issue's full context packet to Claude for a structured recommendation.

    Each issue is sent individually so Claude has maximum context for reasoning.
    """
    if not issues:
        return issues

    client = anthropic.Anthropic(api_key=api_key) if api_key else anthropic.Anthropic()

    for issue in issues:
        context = _build_context_prompt(issue)

        try:
            response = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=2048,
                system=_RECOMMENDATION_SYSTEM,
                messages=[{"role": "user", "content": context}],
            )

            text = response.content[0].text.strip()
            text = re.sub(r"^```(?:json)?\s*", "", text)
            text = re.sub(r"\s*```$", "", text)

            rec = json.loads(text)

            # Validate required fields
            rec.setdefault("recommended_winner", issue["winner"]["page"])
            rec.setdefault("action_type", "monitor-only")
            rec.setdefault("serp_intent", issue.get("serp_analysis", {}).get("serp_intent", "unknown"))
            rec.setdefault("reasoning", "")
            rec.setdefault("page_actions", [])

            issue["recommendation"] = rec

        except (json.JSONDecodeError, Exception) as e:
            # Fallback: preserve raw text if JSON parse fails
            issue["recommendation"] = {
                "recommended_winner": issue["winner"]["page"],
                "action_type": "monitor-only",
                "serp_intent": issue.get("serp_analysis", {}).get("serp_intent", "unknown"),
                "reasoning": f"Automated analysis failed ({str(e)[:100]}). Manual review recommended.",
                "page_actions": [],
            }

    return issues


# ---------------------------------------------------------------------------
# Claude-powered semantic topic clustering
# ---------------------------------------------------------------------------

_CLUSTERING_SYSTEM = """You are an SEO analyst. You will receive a list of search queries that each suffer from keyword cannibalization on the same website.
Group these queries into semantic topic clusters — queries that target the same subject, concept, or user need belong together.

Return ONLY a JSON array (no markdown fences) where each element is:
{
  "topic_name": "short descriptive name for this topic cluster",
  "queries": ["query1", "query2", ...]
}

Rules:
- Every query must appear in exactly one cluster.
- Use concise, descriptive topic names (2-5 words).
- Merge near-duplicates (singular/plural, slight rewordings) into the same cluster.
- If a query doesn't fit any group, put it in its own single-query cluster.
- Return the clusters ordered by how many queries they contain (largest first)."""

_CLUSTERING_BATCH_LIMIT = 500


def cluster_by_topic(issues: list[dict], api_key: str | None = None, **_kwargs) -> list[dict]:
    """
    Send all cannibalized queries to Claude for semantic topic grouping.
    """
    if not issues:
        return []

    if len(issues) == 1:
        return [_make_topic_group(0, issues[0]["query"], issues)]

    client = anthropic.Anthropic(api_key=api_key) if api_key else anthropic.Anthropic()

    issue_map: dict[str, dict] = {}
    for issue in issues:
        issue_map[issue["query"]] = issue

    all_queries = list(issue_map.keys())
    all_clusters: list[dict] = []

    for i in range(0, len(all_queries), _CLUSTERING_BATCH_LIMIT):
        batch_queries = all_queries[i : i + _CLUSTERING_BATCH_LIMIT]

        user_prompt = (
            "Group the following search queries into semantic topic clusters.\n\n"
            "Queries:\n" + "\n".join(f"- {q}" for q in batch_queries)
        )

        try:
            response = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=4096,
                system=_CLUSTERING_SYSTEM,
                messages=[{"role": "user", "content": user_prompt}],
            )

            text = response.content[0].text.strip()
            text = re.sub(r"^```(?:json)?\s*", "", text)
            text = re.sub(r"\s*```$", "", text)

            clusters = json.loads(text)
            all_clusters.extend(clusters)
        except (json.JSONDecodeError, Exception):
            for q in batch_queries:
                all_clusters.append({"topic_name": q, "queries": [q]})

    # Map clusters back to issues
    assigned_queries = set()
    topic_groups = []

    for topic_id, cluster in enumerate(all_clusters):
        topic_name = cluster.get("topic_name", f"Topic {topic_id}")
        cluster_queries = cluster.get("queries", [])
        group_issues = []

        for q in cluster_queries:
            if q in issue_map:
                group_issues.append(issue_map[q])
                assigned_queries.add(q)

        if group_issues:
            topic_groups.append(_make_topic_group(topic_id, topic_name, group_issues))

    # Catch unassigned queries
    missed = [issue_map[q] for q in all_queries if q not in assigned_queries]
    if missed:
        topic_groups.append(_make_topic_group(len(topic_groups), "Uncategorized", missed))

    topic_groups.sort(key=lambda g: g["total_severity"], reverse=True)
    for i, g in enumerate(topic_groups):
        g["topic_id"] = i

    return topic_groups


def _make_topic_group(topic_id: int, topic_name: str, group_issues: list[dict]) -> dict:
    """Build a topic group dict from a list of issues."""
    severities = [i["severity"] for i in group_issues]
    rep_issue = max(group_issues, key=lambda x: x["severity"])
    return {
        "topic_id": topic_id,
        "topic_name": topic_name,
        "representative_query": rep_issue["query"],
        "queries": [i["query"] for i in group_issues],
        "issue_count": len(group_issues),
        "total_severity": round(sum(severities), 1),
        "avg_severity": round(sum(severities) / len(severities), 1),
        "total_wasted_impressions": sum(i["wasted_impressions"] for i in group_issues),
        "issues": group_issues,
    }
