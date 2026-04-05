#!/usr/bin/env python3
"""
GSC Keyword Cannibalization Detector — Full-Context Pipeline

1. Authenticate with Google Search Console (OAuth)
2. Pull query+page data for the configured date range
3. Detect cannibalization and filter branded queries
4. Fetch live SERP top 10 for each query (SerpAPI)
5. Scrape every competing page with Playwright
6. Send full context packets to Claude for recommendations
7. Cluster issues by semantic topic (Claude)
8. Output JSON + HTML reports

Usage:
    python run_cannibalization_detector.py --site https://www.example.com
    python run_cannibalization_detector.py --site sc-domain:example.com --months 6
    python run_cannibalization_detector.py --site https://www.example.com --exclude-branded
    python run_cannibalization_detector.py --list-sites
"""

import argparse
import sys
from pathlib import Path

from cannibalization_detector.auth import build_gsc_service, list_sites
from cannibalization_detector.fetcher import (
    fetch_search_analytics,
    group_by_query,
    build_page_footprints,
    extract_brand_terms,
    filter_branded_queries,
)
from cannibalization_detector.serp import analyze_serps_for_issues
from cannibalization_detector.scraper import scrape_pages_for_issues
from cannibalization_detector.detector import (
    detect_cannibalization,
    enrich_issues,
    generate_recommendations,
    cluster_by_topic,
)
from cannibalization_detector.report import save_json_report, save_html_report


def main():
    parser = argparse.ArgumentParser(
        description="Detect keyword cannibalization with full SERP + page context"
    )
    parser.add_argument(
        "--site",
        help="GSC property URL (e.g. https://www.example.com or sc-domain:example.com)",
    )
    parser.add_argument(
        "--months", type=int, default=3,
        help="Number of months of data to analyze (default: 3)",
    )
    parser.add_argument(
        "--min-severity", type=float, default=0,
        help="Minimum severity score to include in report (0-100, default: 0)",
    )
    parser.add_argument(
        "--output-dir", default="reports",
        help="Directory for output files (default: reports/)",
    )
    parser.add_argument(
        "--credentials", default="credentials.json",
        help="Path to OAuth credentials JSON file",
    )
    parser.add_argument(
        "--list-sites", action="store_true",
        help="List all verified sites and exit",
    )
    parser.add_argument(
        "--exclude-branded", action="store_true",
        help="Filter out queries containing brand terms",
    )
    parser.add_argument(
        "--brand-terms", default="",
        help="Comma-separated additional brand terms to exclude",
    )
    parser.add_argument(
        "--anthropic-api-key", default=None,
        help="Anthropic API key (defaults to ANTHROPIC_API_KEY env var)",
    )
    parser.add_argument(
        "--serpapi-key", default=None,
        help="SerpAPI key (defaults to SERPAPI_API_KEY env var)",
    )

    args = parser.parse_args()

    # ── Step 1: Authenticate ──────────────────────────────────────────
    print("[1/8] Authenticating with Google Search Console...")
    try:
        service = build_gsc_service(credentials_path=args.credentials)
    except FileNotFoundError as e:
        print(f"\nERROR: {e}")
        sys.exit(1)

    if args.list_sites:
        sites = list_sites(service)
        if not sites:
            print("No verified sites found.")
        else:
            print(f"\nFound {len(sites)} verified site(s):\n")
            for site in sites:
                print(f"  {site['siteUrl']}  ({site.get('permissionLevel', 'unknown')})")
        return

    if not args.site:
        print("ERROR: --site is required. Use --list-sites to see available properties.")
        sys.exit(1)

    # ── Step 2: Fetch GSC data ────────────────────────────────────────
    print(f"[2/8] Fetching {args.months} months of search data for {args.site}...")
    rows = fetch_search_analytics(service, args.site, months=args.months)
    print(f"       Retrieved {len(rows):,} query+page rows")

    if not rows:
        print("\nNo data returned. Check site URL and GSC data availability.")
        sys.exit(0)

    # ── Step 3: Detect cannibalization ────────────────────────────────
    print("[3/8] Detecting keyword cannibalization...")
    cannibalized = group_by_query(rows)
    print(f"       Found {len(cannibalized):,} queries with multiple ranking pages")

    if args.exclude_branded:
        brand_terms = extract_brand_terms(args.site)
        extra = [t.strip() for t in args.brand_terms.split(",") if t.strip()] if args.brand_terms else None
        all_terms = brand_terms + (extra or [])
        before = len(cannibalized)
        cannibalized = filter_branded_queries(cannibalized, brand_terms, extra)
        print(f"       Excluded {before - len(cannibalized):,} branded queries (terms: {', '.join(all_terms)})")

    if not cannibalized:
        print("\nNo cannibalization detected after filtering.")
        sys.exit(0)

    issues = detect_cannibalization(cannibalized)

    if args.min_severity > 0:
        issues = [i for i in issues if i["severity"] >= args.min_severity]
        print(f"       {len(issues):,} issues above severity threshold {args.min_severity}")

    if not issues:
        print("\nNo issues above the severity threshold.")
        sys.exit(0)

    # ── Step 4: Live SERP analysis ────────────────────────────────────
    print(f"[4/8] Fetching live SERP data for {len(issues)} queries (2s delay per query)...")
    serp_results = analyze_serps_for_issues(issues, api_key=args.serpapi_key, delay=2.0)
    serp_errors = sum(1 for v in serp_results.values() if v.get("error"))
    print(f"       Completed {len(serp_results) - serp_errors} SERP analyses ({serp_errors} errors)")

    # ── Step 5: Scrape competing pages ────────────────────────────────
    url_count = len({
        url
        for issue in issues
        for url in [issue["winner"]["page"]] + [p["page"] for p in issue["competing_pages"]]
    })
    print(f"[5/8] Scraping {url_count} competing pages with Playwright...")
    page_snapshots = scrape_pages_for_issues(issues)
    scrape_errors = sum(1 for v in page_snapshots.values() if v.get("fetch_error"))
    print(f"       Scraped {len(page_snapshots) - scrape_errors} pages ({scrape_errors} errors)")

    # ── Step 5b: Build keyword footprints ────────────────────────────
    print("       Building keyword footprints for all pages...")
    page_footprints = build_page_footprints(rows)
    print(f"       Built footprints for {len(page_footprints):,} pages")

    # ── Step 6: Enrich + Claude recommendations ───────────────────────
    print("[6/8] Assembling context packets and generating AI recommendations...")
    issues = enrich_issues(issues, serp_results, page_snapshots, page_footprints)
    issues = generate_recommendations(issues, api_key=args.anthropic_api_key)
    print(f"       Generated full-context recommendations for {len(issues)} issues")

    # ── Step 7: Semantic topic clustering ─────────────────────────────
    print("[7/8] Clustering issues by semantic topic...")
    topic_groups = cluster_by_topic(issues, api_key=args.anthropic_api_key)
    print(f"       Organized into {len(topic_groups)} topic group(s)")

    # ── Step 8: Generate reports ──────────────────────────────────────
    print("[8/8] Generating reports...")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    domain = args.site.replace("https://", "").replace("http://", "").replace("sc-domain:", "").replace("/", "_")
    json_path = output_dir / f"cannibalization_{domain}.json"
    html_path = output_dir / f"cannibalization_{domain}.html"

    save_json_report(topic_groups, str(json_path), args.site, len(rows))
    save_html_report(topic_groups, str(html_path), args.site)

    # ── Summary ───────────────────────────────────────────────────────
    all_issues = [i for g in topic_groups for i in g["issues"]]
    high = sum(1 for i in all_issues if i["severity"] >= 70)
    medium = sum(1 for i in all_issues if 40 <= i["severity"] < 70)
    low = sum(1 for i in all_issues if i["severity"] < 40)
    wasted = sum(g["total_wasted_impressions"] for g in topic_groups)

    # Count action types from recommendations
    action_counts = {}
    for i in all_issues:
        rec = i.get("recommendation", {})
        if isinstance(rec, dict):
            at = rec.get("action_type", "unknown")
            action_counts[at] = action_counts.get(at, 0) + 1

    action_summary = "  ".join(f"{k}: {v}" for k, v in sorted(action_counts.items(), key=lambda x: -x[1]))

    print(f"""
{'='*60}
  CANNIBALIZATION REPORT SUMMARY
{'='*60}
  Site:                 {args.site}
  Queries analyzed:     {len(rows):,}
  Cannibalized queries: {len(all_issues):,}
  Topic groups:         {len(topic_groups)}

  Severity breakdown:
    High (70+):    {high}
    Medium (40-69): {medium}
    Low (0-39):     {low}

  Action types:
    {action_summary}

  Total wasted impressions: {wasted:,}

  Reports saved:
    JSON: {json_path}
    HTML: {html_path}
{'='*60}
""")


if __name__ == "__main__":
    main()
