"""Playwright-based page content scraper for competing URLs."""

import re
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout


def scrape_page(url: str, timeout_ms: int = 15000) -> dict:
    """
    Fetch a URL with Playwright and extract on-page SEO elements.

    Returns a dict with:
        - url, title, meta_description, h1, h2s, word_count,
          page_type, primary_cta, fetch_error
    """
    result = {
        "url": url,
        "title": "",
        "meta_description": "",
        "h1": "",
        "h2s": [],
        "word_count": 0,
        "page_type": "unknown",
        "primary_cta": "",
        "fetch_error": None,
    }

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 720},
            )
            page = context.new_page()

            try:
                page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            except PlaywrightTimeout:
                result["fetch_error"] = "timeout"
                browser.close()
                return result

            # Title
            result["title"] = page.title() or ""

            # Meta description
            meta = page.query_selector('meta[name="description"]')
            if meta:
                result["meta_description"] = meta.get_attribute("content") or ""

            # H1
            h1_el = page.query_selector("h1")
            if h1_el:
                result["h1"] = (h1_el.inner_text() or "").strip()

            # H2s
            h2_els = page.query_selector_all("h2")
            result["h2s"] = [
                (el.inner_text() or "").strip()
                for el in h2_els[:15]  # cap to avoid noise
                if (el.inner_text() or "").strip()
            ]

            # Body text and word count
            body_el = page.query_selector("body")
            body_text = ""
            if body_el:
                body_text = body_el.inner_text() or ""
            words = re.findall(r"\b[a-zA-Z]+\b", body_text)
            result["word_count"] = len(words)

            # Primary CTA — look for prominent action buttons/links
            result["primary_cta"] = _extract_cta(page)

            # Page type classification
            result["page_type"] = _classify_page_type(url, result)

            browser.close()

    except Exception as e:
        result["fetch_error"] = str(e)[:200]

    return result


def _extract_cta(page) -> str:
    """Find the most prominent CTA on the page."""
    # Check common CTA selectors in priority order
    cta_selectors = [
        'a.cta, a.btn-primary, a.button-primary',
        'button.cta, button.btn-primary, button.button-primary',
        '[class*="cta"] a, [class*="hero"] a, [class*="banner"] a',
        '[class*="cta"] button, [class*="hero"] button',
    ]

    for selector in cta_selectors:
        try:
            el = page.query_selector(selector)
            if el:
                text = (el.inner_text() or "").strip()
                if text and len(text) < 60:
                    return text
        except Exception:
            continue

    # Fallback: look for links/buttons with action-oriented text
    cta_patterns = [
        "get started", "sign up", "contact us", "book a", "schedule",
        "free trial", "request a", "buy now", "shop now", "learn more",
        "get a quote", "start free", "try free", "download",
    ]
    try:
        all_buttons = page.query_selector_all("a, button")
        for el in all_buttons[:50]:
            text = (el.inner_text() or "").strip().lower()
            if any(pat in text for pat in cta_patterns) and len(text) < 60:
                return (el.inner_text() or "").strip()
    except Exception:
        pass

    return ""


def _classify_page_type(url: str, extracted: dict) -> str:
    """Classify the page type based on URL structure and content signals."""
    url_lower = url.lower()
    title_lower = extracted["title"].lower()
    h1_lower = extracted["h1"].lower()

    # Homepage
    path = url_lower.rstrip("/").split("//", 1)[-1]
    if path.count("/") <= 1:
        return "homepage"

    # Blog / article
    if any(seg in url_lower for seg in ["/blog/", "/article", "/post/", "/news/", "/guide/", "/journal/"]):
        return "blog_post"
    if extracted["word_count"] > 1000 and len(extracted["h2s"]) >= 3:
        if any(w in title_lower for w in ["how to", "what is", "guide", "tips", "ways to"]):
            return "blog_post"

    # About page
    if any(seg in url_lower for seg in ["/about", "/team", "/our-story"]):
        return "about_page"

    # Contact page
    if any(seg in url_lower for seg in ["/contact", "/get-in-touch", "/reach-us"]):
        return "contact_page"

    # Service / landing page
    if any(seg in url_lower for seg in ["/services", "/solutions", "/features", "/pricing"]):
        return "service_page"

    # Product page
    if any(seg in url_lower for seg in ["/product", "/shop/", "/store/", "/buy/"]):
        return "product_page"

    # Portfolio / case study
    if any(seg in url_lower for seg in ["/portfolio", "/case-stud", "/work/", "/projects"]):
        return "portfolio_page"

    # Content-heavy page without blog URL markers — likely a content/resource page
    if extracted["word_count"] > 800 and len(extracted["h2s"]) >= 2:
        return "content_page"

    # Short service-like page
    if extracted["word_count"] < 500 and extracted["primary_cta"]:
        return "service_page"

    return "other"


def scrape_pages_for_issues(issues: list[dict]) -> dict[str, dict]:
    """
    Scrape all unique competing page URLs across all issues.

    Returns a dict mapping URL → page scrape result.
    """
    # Collect all unique URLs
    urls = set()
    for issue in issues:
        urls.add(issue["winner"]["page"])
        for p in issue["competing_pages"]:
            urls.add(p["page"])

    results = {}
    for url in urls:
        results[url] = scrape_page(url)

    return results
