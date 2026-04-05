"""JSON and HTML report generation for cannibalization analysis."""

import json
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from jinja2 import Template


def _sanitize_for_json(topic_groups: list[dict]) -> list[dict]:
    """Deep-copy topic groups, stripping non-serializable fields for JSON output."""
    import copy
    clean = copy.deepcopy(topic_groups)
    for group in clean:
        for issue in group.get("issues", []):
            # Remove page_snapshots from JSON (too verbose) — keep summary
            snapshots = issue.pop("page_snapshots", {})
            issue["page_types"] = {
                url: snap.get("page_type", "unknown")
                for url, snap in snapshots.items()
            }
            # Remove serp raw organic results for cleaner JSON
            serp = issue.get("serp_analysis")
            if serp:
                serp.pop("organic_results", None)
    return clean


def save_json_report(topic_groups: list[dict], output_path: str, site_url: str, total_rows: int):
    """Save the full analysis as a structured JSON file."""
    clean_groups = _sanitize_for_json(topic_groups)

    report = {
        "meta": {
            "site": site_url,
            "generated_at": datetime.now().isoformat(),
            "total_queries_analyzed": total_rows,
            "cannibalized_topic_groups": len(clean_groups),
            "total_cannibalized_queries": sum(g["issue_count"] for g in clean_groups),
        },
        "summary": {
            "total_wasted_impressions": sum(g["total_wasted_impressions"] for g in clean_groups),
            "high_severity_count": sum(
                1 for g in clean_groups for i in g["issues"] if i["severity"] >= 70
            ),
            "medium_severity_count": sum(
                1 for g in clean_groups for i in g["issues"] if 40 <= i["severity"] < 70
            ),
            "low_severity_count": sum(
                1 for g in clean_groups for i in g["issues"] if i["severity"] < 40
            ),
        },
        "topic_groups": clean_groups,
    }

    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    return output_path


# ---------------------------------------------------------------------------
# HTML Report
# ---------------------------------------------------------------------------

_PAGE_TYPE_DISPLAY = {
    "homepage": "Home Page",
    "blog_post": "Blog Post",
    "content_page": "Blog Post",
    "about_page": "About Page",
    "contact_page": "Contact Page",
    "service_page": "Service Page",
    "product_page": "Product Page",
    "portfolio_page": "Portfolio Page",
    "other": "Page",
    "unknown": "Page",
}

_BLOG_URL_SEGMENTS = ("/insights/", "/blog/", "/news/", "/article/", "/articles/", "/posts/", "/post/", "/journal/", "/guide/", "/guides/")


def _display_page_type(raw_type: str, url: str = "") -> str:
    """Map internal page_type keys to clean display names, with URL-based override."""
    url_lower = url.lower()
    if any(seg in url_lower for seg in _BLOG_URL_SEGMENTS):
        return "Blog Post"
    return _PAGE_TYPE_DISPLAY.get(raw_type, raw_type.replace("_", " ").title())


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Keyword Cannibalization Audit — {{ site_url }}</title>
<style>
  :root {
    --bg: #F5F0EB;
    --surface: #FDFAF7;
    --surface2: #F5F0EB;
    --surface3: #EDE8E3;
    --border: #E5DDD5;
    --border-light: #EBE4DC;
    --text: #2C2825;
    --text-secondary: #4A433D;
    --text-muted: #8C7E74;
    --accent: #1B4332;
    --danger: #9A3412;
    --warning: #92400E;
    --success: #1B4332;
    --shadow-sm: 0 1px 2px rgba(44,40,37,0.05), 0 1px 3px rgba(44,40,37,0.04);
    --shadow-md: 0 2px 8px rgba(44,40,37,0.07), 0 1px 3px rgba(44,40,37,0.05);
    --radius: 12px;
    --radius-sm: 8px;
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Inter', 'Segoe UI', Roboto, sans-serif;
    background: var(--bg);
    color: var(--text);
    line-height: 1.6;
    padding: 2.5rem 2rem;
    -webkit-font-smoothing: antialiased;
  }
  .container { max-width: 1120px; margin: 0 auto; }

  .report-header {
    margin-bottom: 1.75rem;
    display: flex; justify-content: space-between; align-items: flex-start;
  }
  .report-header-left h1 { font-size: 1.5rem; font-weight: 700; letter-spacing: -0.02em; }
  .report-header-left .subtitle { color: var(--text-muted); font-size: 0.82rem; margin-top: 0.15rem; }
  .report-author { text-align: right; flex-shrink: 0; }
  .report-author .author-name { font-weight: 650; font-size: 0.88rem; }
  .report-author .author-url { font-size: 0.78rem; color: var(--text-muted); }

  .exec-summary {
    font-size: 0.88rem; color: var(--text-secondary);
    line-height: 1.7; margin-bottom: 2rem; max-width: 860px;
  }

  .summary-grid {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(155px, 1fr));
    gap: 0.75rem; margin-bottom: 2.5rem;
  }
  .stat-card {
    background: var(--surface); border: 1px solid var(--border-light);
    border-radius: var(--radius); padding: 1.1rem 1.25rem; box-shadow: var(--shadow-sm);
  }
  .stat-card .label {
    font-size: 0.68rem; color: var(--text-muted); text-transform: uppercase;
    letter-spacing: 0.06em; font-weight: 500; margin-bottom: 0.3rem;
  }
  .stat-card .value { font-size: 1.6rem; font-weight: 700; letter-spacing: -0.02em; }
  .stat-card .value.danger { color: var(--danger); }
  .stat-card .value.warning { color: var(--warning); }
  .stat-card .value.success { color: var(--success); }
  .stat-card .value.accent { color: var(--accent); }

  .topic-group {
    background: var(--surface); border: 1px solid var(--border-light);
    border-radius: var(--radius); margin-bottom: 1.25rem;
    box-shadow: var(--shadow-sm); overflow: hidden;
  }
  .topic-header {
    padding: 1rem 1.5rem; display: flex; justify-content: space-between;
    align-items: center; border-bottom: 1px solid var(--border-light);
  }
  .topic-title { font-weight: 650; font-size: 0.95rem; letter-spacing: -0.01em; }
  .topic-count { color: var(--text-muted); font-size: 0.8rem; font-weight: 400; margin-left: 0.4rem; }
  .topic-meta { display: flex; gap: 1.25rem; font-size: 0.78rem; color: var(--text-muted); font-weight: 500; }

  .issue-card {
    margin: 0.75rem; border: 1px solid var(--border); border-radius: var(--radius-sm);
    overflow: hidden; background: var(--surface); box-shadow: var(--shadow-sm);
    transition: box-shadow 0.15s ease;
  }
  .issue-card:hover { box-shadow: var(--shadow-md); }
  .issue-card:last-child { margin-bottom: 0.75rem; }

  .issue-collapsed {
    padding: 0.85rem 1.15rem; display: flex; align-items: baseline;
    gap: 0.5rem; cursor: pointer; user-select: none; flex-wrap: wrap;
  }
  .issue-collapsed:hover { background: var(--surface2); }

  .issue-keyword {
    font-weight: 650; font-size: 0.88rem; color: var(--text);
    letter-spacing: -0.01em; flex-shrink: 0;
  }
  .issue-severity {
    font-size: 0.78rem; color: var(--text-muted); font-weight: 400; flex-shrink: 0;
  }
  .issue-summary-text {
    font-size: 0.78rem; color: var(--text-secondary);
    min-width: 0; flex: 1;
  }
  .toggle-icon {
    flex-shrink: 0; width: 20px; height: 20px; border-radius: 50%;
    background: var(--surface3); display: flex; align-items: center;
    justify-content: center; transition: transform 0.2s ease;
    color: var(--text-muted); font-size: 0.7rem;
  }
  .issue-card.open .toggle-icon { transform: rotate(180deg); }

  .issue-details {
    display: none; border-top: 1px solid var(--border-light);
    padding: 1.25rem; background: var(--surface2);
  }
  .issue-card.open .issue-details { display: block; }
  .detail-section { margin-bottom: 1.25rem; }
  .detail-section:last-child { margin-bottom: 0; }

  .pages-table {
    width: 100%; border-collapse: collapse; font-size: 0.8rem;
    background: var(--surface); border-radius: var(--radius-sm);
    overflow: hidden; border: 1px solid var(--border);
  }
  .pages-table th {
    text-align: left; color: var(--text-muted); font-weight: 600;
    padding: 0.55rem 0.75rem; font-size: 0.65rem;
    text-transform: uppercase; letter-spacing: 0.05em;
    background: var(--surface3); border-bottom: 1px solid var(--border);
  }
  .pages-table td {
    padding: 0.55rem 0.75rem; border-top: 1px solid var(--border-light);
    color: var(--text-secondary); vertical-align: top;
  }
  .pages-table tbody tr:first-child td { border-top: none; }
  .pages-table tr.winner-row td { font-weight: 600; color: var(--text); }
  .page-url { word-break: break-all; }
  .page-url a { color: var(--accent); text-decoration: none; }
  .page-url a:hover { text-decoration: underline; }
  .winner-row .page-url a { color: var(--text); }
  .page-type-cell { font-size: 0.72rem; color: var(--text-muted); white-space: nowrap; }
  .inline-footprint {
    font-size: 0.7rem; color: var(--text-muted); font-style: italic;
    font-weight: 400; margin-top: 0.15rem; line-height: 1.4;
    word-break: break-word; white-space: normal;
  }

  .content-block {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 1rem 1.15rem;
    font-size: 0.82rem;
    color: var(--text-secondary);
    line-height: 1.65;
  }
  .content-block + .content-block { margin-top: 0.75rem; }

  .action-item {
    padding: 0.6rem 0;
    border-bottom: 1px solid var(--border);
  }
  .action-item:last-child { border-bottom: none; padding-bottom: 0; }
  .action-item:first-child { padding-top: 0; }
  .action-url {
    font-weight: 600; color: var(--text);
    margin-bottom: 0.1rem; font-size: 0.78rem; word-break: break-all;
  }
  .action-url a { color: var(--accent); text-decoration: none; }
  .action-url a:hover { text-decoration: underline; }
  .action-url .action-ptype {
    font-weight: 400; color: var(--text-muted); font-size: 0.7rem; margin-left: 0.3rem;
  }
  .action-label {
    display: inline-block; color: var(--accent);
    font-weight: 600; font-size: 0.6rem;
    text-transform: uppercase; letter-spacing: 0.05em;
    margin-bottom: 0.15rem;
  }
  .action-changes {
    color: var(--text-secondary); font-size: 0.78rem; line-height: 1.55;
  }

  .footer {
    text-align: center; color: var(--text-muted); font-size: 0.75rem;
    margin-top: 3rem; padding-top: 1.25rem; border-top: 1px solid var(--border);
  }
  .no-issues { text-align: center; padding: 4rem 2rem; color: var(--text-muted); }
  .no-issues h2 { color: var(--success); margin-bottom: 0.5rem; }
</style>
</head>
<body>
<div class="container">

  <div class="report-header">
    <div class="report-header-left">
      <h1>Keyword Cannibalization Audit</h1>
      <p class="subtitle">{{ site_url }} &middot; {{ generated_at }}</p>
    </div>
    <div class="report-author">
      <div class="author-name">UGO Anums Consulting</div>
      <div class="author-url">ugoanums.com</div>
    </div>
  </div>

  <p class="exec-summary">{{ exec_summary }}</p>

  <div class="summary-grid">
    <div class="stat-card">
      <div class="label">Cannibalized Queries</div>
      <div class="value accent">{{ total_issues }}</div>
    </div>
    <div class="stat-card">
      <div class="label">Topic Groups</div>
      <div class="value accent">{{ total_groups }}</div>
    </div>
    <div class="stat-card">
      <div class="label">High Severity</div>
      <div class="value danger">{{ high_count }}</div>
    </div>
    <div class="stat-card">
      <div class="label">Medium Severity</div>
      <div class="value warning">{{ medium_count }}</div>
    </div>
    <div class="stat-card">
      <div class="label">Low Severity</div>
      <div class="value success">{{ low_count }}</div>
    </div>
    <div class="stat-card">
      <div class="label">Wasted Impressions</div>
      <div class="value warning">{{ "{:,}".format(total_wasted) }}</div>
    </div>
  </div>

  {% if topic_groups %}
  {% for group in topic_groups %}
  <div class="topic-group">
    <div class="topic-header">
      <div>
        <span class="topic-title">{{ group.topic_name if group.topic_name else group.representative_query }}</span>
        <span class="topic-count">{{ group.issue_count }} {% if group.issue_count == 1 %}query{% else %}queries{% endif %}</span>
      </div>
      <div class="topic-meta">
        <span>Avg severity {{ group.avg_severity }}</span>
        <span>{{ "{:,}".format(group.total_wasted_impressions) }} wasted impr</span>
      </div>
    </div>

    {% for issue in group.issues %}
    {% set rec = issue.recommendation if issue.recommendation is mapping else {} %}
    {% set fps = issue.page_footprints if issue.page_footprints is mapping else {} %}
    <div class="issue-card" onclick="this.classList.toggle('open')">

      <div class="issue-collapsed">
        <span class="issue-keyword">"{{ issue.query }}"</span>
        <span class="issue-severity">({{ issue.severity }})</span>
        <span class="issue-summary-text">{{ rec.reasoning[:140] }}{% if rec.reasoning and rec.reasoning|length > 140 %}...{% endif %}</span>
        <span class="toggle-icon">&#9662;</span>
      </div>

      <div class="issue-details">

        <div class="detail-section">
          <table class="pages-table">
            <thead>
              <tr><th>Page</th><th>Type</th><th>Clicks</th><th>Impr</th><th>CTR</th><th>Pos</th></tr>
            </thead>
            <tbody>
              <tr class="winner-row">
                <td class="page-url">
                  <a href="{{ issue.winner.page }}" target="_blank" onclick="event.stopPropagation()">{{ issue.winner.page }}</a>
                  {% if issue.winner.page in fps %}
                  {% set wfp = fps[issue.winner.page] %}
                  <div class="inline-footprint">{{ wfp.total_queries }} queries &middot; {{ "{:,}".format(wfp.total_impressions) }} total impr{% if wfp.other_top_queries %} &middot; also: {% for oq in wfp.other_top_queries %}"{{ oq.query }}"{% if not loop.last %}, {% endif %}{% endfor %}{% endif %}</div>
                  {% endif %}
                </td>
                <td class="page-type-cell">{{ display_page_type(issue.page_snapshots[issue.winner.page].page_type if issue.winner.page in issue.page_snapshots else 'unknown', issue.winner.page) }}</td>
                <td>{{ "{:,}".format(issue.winner.clicks) }}</td>
                <td>{{ "{:,}".format(issue.winner.impressions) }}</td>
                <td>{{ "%.1f"|format(issue.winner.ctr * 100) }}%</td>
                <td>{{ issue.winner.position }}</td>
              </tr>
              {% for page in issue.competing_pages %}
              <tr>
                <td class="page-url">
                  <a href="{{ page.page }}" target="_blank" onclick="event.stopPropagation()">{{ page.page }}</a>
                  {% if page.page in fps %}
                  {% set cfp = fps[page.page] %}
                  <div class="inline-footprint">{{ cfp.total_queries }} queries &middot; {{ "{:,}".format(cfp.total_impressions) }} total impr{% if cfp.other_top_queries %} &middot; also: {% for oq in cfp.other_top_queries %}"{{ oq.query }}"{% if not loop.last %}, {% endif %}{% endfor %}{% endif %}</div>
                  {% endif %}
                </td>
                <td class="page-type-cell">{{ display_page_type(issue.page_snapshots[page.page].page_type if page.page in issue.page_snapshots else 'unknown', page.page) }}</td>
                <td>{{ "{:,}".format(page.clicks) }}</td>
                <td>{{ "{:,}".format(page.impressions) }}</td>
                <td>{{ "%.1f"|format(page.ctr * 100) }}%</td>
                <td>{{ page.position }}</td>
              </tr>
              {% endfor %}
            </tbody>
          </table>
        </div>

        {% if rec.reasoning %}
        <div class="detail-section">
          <div class="content-block">{{ rec.reasoning }}</div>
        </div>
        {% endif %}

        {% if rec.page_actions %}
        <div class="detail-section">
          <div class="content-block">
            {% for pa in rec.page_actions %}
            <div class="action-item">
              <div class="action-url">
                <a href="{{ pa.url }}" target="_blank" onclick="event.stopPropagation()">{{ pa.url }}</a>
                {% if pa.page_type %}<span class="action-ptype">({{ display_page_type(pa.page_type, pa.url) }})</span>{% endif %}
              </div>
              <div class="action-label">{{ pa.action }}</div>
              <div class="action-changes">{{ pa.changes }}</div>
            </div>
            {% endfor %}
          </div>
        </div>
        {% endif %}

      </div>
    </div>
    {% endfor %}
  </div>
  {% endfor %}
  {% else %}
  <div class="no-issues">
    <h2>No Cannibalization Detected</h2>
    <p>No queries with multiple competing pages were found in the analyzed data.</p>
  </div>
  {% endif %}

  <div class="footer">
    Prepared by UGO Anums &middot; ugoanums.com
  </div>
</div>
</body>
</html>"""


def _build_exec_summary(
    site_url: str,
    total_issues: int,
    high_count: int,
    medium_count: int,
    total_wasted: int,
    topic_groups: list[dict],
    all_issues: list[dict],
) -> str:
    """Generate a 3-sentence executive summary from the actual report data."""
    # Find the top priority issue
    top_issue = max(all_issues, key=lambda i: i["severity"]) if all_issues else None
    top_group = topic_groups[0] if topic_groups else None

    # Determine dominant action type
    action_counts: dict[str, int] = {}
    for i in all_issues:
        rec = i.get("recommendation", {})
        if isinstance(rec, dict):
            at = rec.get("action_type", "monitor-only")
            action_counts[at] = action_counts.get(at, 0) + 1
    dominant_action = max(action_counts, key=action_counts.get) if action_counts else "review"
    dominant_count = action_counts.get(dominant_action, 0)

    action_labels = {
        "differentiate-by-intent": "content differentiation",
        "add-canonical": "canonical tag additions",
        "301-redirect": "redirects",
        "noindex-fragments": "noindex directives",
        "monitor-only": "monitoring",
    }
    action_label = action_labels.get(dominant_action, dominant_action)

    # Sentence 1: scope
    if high_count > 0:
        s1 = (
            f"I found {total_issues} queries where multiple pages on {site_url} "
            f"are competing against each other, {high_count} of which are high severity."
        )
    elif medium_count > 0:
        s1 = (
            f"I found {total_issues} queries where multiple pages on {site_url} "
            f"are competing against each other, with {medium_count} at medium severity "
            f"and the rest low."
        )
    else:
        s1 = (
            f"I found {total_issues} queries where multiple pages on {site_url} "
            f"are competing against each other, all at low severity."
        )

    # Sentence 2: top priority
    if top_issue:
        top_rec = top_issue.get("recommendation", {})
        top_action = top_rec.get("action_type", "review") if isinstance(top_rec, dict) else "review"
        top_action_label = action_labels.get(top_action, top_action)
        s2 = (
            f"The top priority is \"{top_issue['query']}\" "
            f"(severity {top_issue['severity']}) which I recommend resolving with {top_action_label}."
        )
    else:
        s2 = "No immediate action is required."

    # Sentence 3: overall pattern
    s3 = (
        f"Across all issues, {dominant_count} of {total_issues} are best resolved through "
        f"{action_label}, with {total_wasted:,} total impressions currently split across competing pages."
    )

    return f"{s1} {s2} {s3}"


def save_html_report(topic_groups: list[dict], output_path: str, site_url: str):
    """Render and save the HTML cannibalization report."""
    total_issues = sum(g["issue_count"] for g in topic_groups)
    all_issues = [i for g in topic_groups for i in g["issues"]]
    high_count = sum(1 for i in all_issues if i["severity"] >= 70)
    medium_count = sum(1 for i in all_issues if 40 <= i["severity"] < 70)
    total_wasted = sum(g["total_wasted_impressions"] for g in topic_groups)

    exec_summary = _build_exec_summary(
        site_url, total_issues, high_count, medium_count,
        total_wasted, topic_groups, all_issues,
    )

    template = Template(HTML_TEMPLATE)
    html = template.render(
        site_url=site_url,
        generated_at=datetime.now().strftime("%B %d, %Y"),
        exec_summary=exec_summary,
        total_issues=total_issues,
        total_groups=len(topic_groups),
        high_count=high_count,
        medium_count=medium_count,
        low_count=sum(1 for i in all_issues if i["severity"] < 40),
        total_wasted=total_wasted,
        topic_groups=topic_groups,
        display_page_type=_display_page_type,
    )

    with open(output_path, "w") as f:
        f.write(html)

    return output_path
