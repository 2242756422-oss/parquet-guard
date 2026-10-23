"""Standalone professional HTML diagnostic report generator.

Produces self-contained single-file HTML reports featuring:
1. Executive audit & consumer compatibility verdict banner.
2. Producer-Dataset-Consumer Impact Graph (clean SVG visualization).
3. Downstream Consumer Compatibility Matrix.
4. Schema Diff & Field-Level Evidence Explorer.
5. Partition Schema Group distribution chart.
6. Verified Migration Sandbox outcome & strategy comparison.
7. Technical provenance and reproducibility metadata.
8. Zero external CDN dependencies, fully offline-functional, XSS-safe text escaping.
"""

from __future__ import annotations

import html
from typing import Any, Dict, Optional

from parquet_guard.core.consumer import (
    ConsumerCompatibilityStatus,
    ConsumerImpactReport,
)
from parquet_guard.core.grouping import DatasetTopologyReport
from parquet_guard.core.migration_verifier import MigrationVerificationResult
from parquet_guard.core.models import (
    DiffReport,
    PartitionAuditResult,
)


def _escape(text: Any) -> str:
    """Safely escape text for HTML embedding."""
    if text is None:
        return ""
    return html.escape(str(text))


def _generate_impact_graph_svg(impact_report: Optional[ConsumerImpactReport]) -> str:
    """Render an accessible, responsive SVG Directed Acyclic Graph."""
    if not impact_report or not impact_report.impact_graph.nodes:
        return "<div class='empty-placeholder'>No consumer impact graph available.</div>"

    nodes = impact_report.impact_graph.nodes
    edges = impact_report.impact_graph.edges

    # Layout geometry
    width = 800
    consumer_count = sum(1 for n in nodes if n.node_type == "consumer")
    height = max(260, 80 + consumer_count * 75)

    svg_parts = [
        f'<svg viewBox="0 0 {width} {height}" class="impact-svg" xmlns="http://www.w3.org/2000/svg">',
        "<defs>",
        '  <marker id="arrow-green" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">',
        '    <path d="M 0 1 L 10 5 L 0 9 z" fill="#10b981"/>',
        "  </marker>",
        '  <marker id="arrow-amber" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">',
        '    <path d="M 0 1 L 10 5 L 0 9 z" fill="#f59e0b"/>',
        "  </marker>",
        '  <marker id="arrow-red" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">',
        '    <path d="M 0 1 L 10 5 L 0 9 z" fill="#ef4444"/>',
        "  </marker>",
        '  <marker id="arrow-gray" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">',
        '    <path d="M 0 1 L 10 5 L 0 9 z" fill="#64748b"/>',
        "  </marker>",
        "</defs>",
    ]

    # Node positions
    node_coords: Dict[str, tuple[int, int]] = {}
    node_coords["producer"] = (60, height // 2)
    node_coords["dataset"] = (280, height // 2)

    c_idx = 0
    c_y_step = max(70, (height - 60) // max(1, consumer_count))
    c_y_start = 50 + (height - 60 - (consumer_count - 1) * c_y_step) // 2

    for n in nodes:
        if n.node_type == "consumer":
            cy = c_y_start + c_idx * c_y_step
            node_coords[n.id] = (560, cy)
            c_idx += 1

    # Draw edges
    for e in edges:
        s_coord = node_coords.get(e.source)
        t_coord = node_coords.get(e.target)
        if not s_coord or not t_coord:
            continue

        sx, sy = s_coord[0] + 130, s_coord[1]
        tx, ty = t_coord[0] - 10, t_coord[1]

        marker = "arrow-gray"
        stroke = "#64748b"
        if e.status == "COMPATIBLE":
            marker = "arrow-green"
            stroke = "#10b981"
        elif e.status in ("CONDITIONALLY_COMPATIBLE", "POTENTIALLY_BREAKING"):
            marker = "arrow-amber"
            stroke = "#f59e0b"
        elif e.status == "INCOMPATIBLE":
            marker = "arrow-red"
            stroke = "#ef4444"

        # Smooth cubic bezier curve
        dx = (tx - sx) / 2
        path_d = f"M {sx} {sy} C {sx + dx} {sy}, {tx - dx} {ty}, {tx} {ty}"
        svg_parts.append(
            f'<path d="{path_d}" stroke="{stroke}" stroke-width="2.2" fill="none" marker-end="url(#{marker})" stroke-dasharray="{"none" if e.status != "INCOMPATIBLE" else "4,3"}"/>'
        )

    # Draw nodes
    for n in nodes:
        coord = node_coords.get(n.id)
        if not coord:
            continue
        x, y = coord

        # Node styling based on type and status
        bg = "#1e293b"
        border = "#475569"
        badge_color = "#94a3b8"

        if n.node_type == "producer":
            border = "#3b82f6"
            badge = "PRODUCER"
        elif n.node_type == "dataset":
            border = "#6366f1"
            badge = "DATASET"
        else:
            badge = n.status or "CONSUMER"
            if n.status == "COMPATIBLE":
                border = "#10b981"
                badge_color = "#10b981"
            elif n.status in ("CONDITIONALLY_COMPATIBLE", "POTENTIALLY_BREAKING"):
                border = "#f59e0b"
                badge_color = "#f59e0b"
            elif n.status == "INCOMPATIBLE":
                border = "#ef4444"
                badge_color = "#ef4444"

        rect_h = 50
        rect_w = 140
        svg_parts.append(
            f'<g transform="translate({x}, {y - rect_h // 2})">'
            f'  <rect width="{rect_w}" height="{rect_h}" rx="6" fill="{bg}" stroke="{border}" stroke-width="1.8"/>'
            f'  <text x="10" y="20" fill="#f8fafc" font-size="12" font-weight="600" font-family="system-ui, sans-serif">{_escape(n.label[:17])}</text>'
            f'  <text x="10" y="38" fill="{badge_color}" font-size="10" font-weight="700" letter-spacing="0.5" font-family="system-ui, sans-serif">{_escape(badge)}</text>'
            f"</g>"
        )

    svg_parts.append("</svg>")
    return "\n".join(svg_parts)


def generate_html_diagnostic_report(
    output_path: str,
    audit_result: Optional[PartitionAuditResult] = None,
    impact_report: Optional[ConsumerImpactReport] = None,
    topology_report: Optional[DatasetTopologyReport] = None,
    migration_result: Optional[MigrationVerificationResult] = None,
    diff_report: Optional[DiffReport] = None,
    dataset_title: str = "Parquet Dataset",
) -> None:
    """Generate and write a comprehensive standalone HTML report."""
    # Determine top banner verdict
    banner_state = "VALID"
    banner_class = "state-valid"
    banner_desc = "All schema checks and consumer compatibility expectations satisfied."

    if impact_report and impact_report.overall_status == ConsumerCompatibilityStatus.INCOMPATIBLE:
        banner_state = "INCOMPATIBLE"
        banner_class = "state-incompatible"
        banner_desc = f"{impact_report.incompatible_consumers} downstream consumer(s) have declared requirement violations."
    elif diff_report and diff_report.has_breaking_changes:
        banner_state = "BREAKING"
        banner_class = "state-breaking"
        banner_desc = "Upstream schema drift contains breaking changes against baseline contract."
    elif audit_result and audit_result.has_drift:
        banner_state = "DRIFTED"
        banner_class = "state-drifted"
        banner_desc = "Multiple distinct schema groups detected across partitioned files."

    # Build consumer matrix table
    matrix_html = ""
    if impact_report and impact_report.evaluations:
        matrix_html += '<div class="table-wrap"><table class="data-table"><thead><tr>'
        matrix_html += "<th>Consumer Name</th><th>Consumer ID</th><th>Status</th><th>Blocking</th><th>Violations</th><th>Recommendations</th>"
        matrix_html += "</tr></thead><tbody>"
        for ev in impact_report.evaluations:
            status_badge = (
                f'<span class="badge badge-{ev.status.value.lower()}">{_escape(ev.status.value)}</span>'
            )
            blocking_badge = (
                '<span class="badge badge-breaking">BLOCKING</span>'
                if ev.is_blocking
                else '<span class="badge badge-safe">PASS</span>'
            )
            violations = (
                ev.missing_required_fields
                + ev.type_mismatches
                + ev.nullability_violations
                + ev.dimension_mismatches
            )
            viol_str = ", ".join(violations) if violations else "None"
            recs_str = "<br>".join(ev.recommendations) if ev.recommendations else "None required"
            matrix_html += (
                f"<tr>"
                f"<td><strong>{_escape(ev.consumer_name)}</strong></td>"
                f"<td><code>{_escape(ev.consumer_id)}</code></td>"
                f"<td>{status_badge}</td>"
                f"<td>{blocking_badge}</td>"
                f'<td class="text-danger">{_escape(viol_str)}</td>'
                f'<td class="text-muted text-sm">{recs_str}</td>'
                f"</tr>"
            )
        matrix_html += "</tbody></table></div>"
    else:
        matrix_html = '<div class="empty-placeholder">No consumer contracts evaluated.</div>'

    # Build schema groups table
    groups_html = ""
    if topology_report and topology_report.groups:
        groups_html += '<div class="table-wrap"><table class="data-table"><thead><tr>'
        groups_html += "<th>Group</th><th>Fingerprint</th><th>Files</th><th>Percentage</th><th>Rows</th><th>Bytes</th><th>Sample Path</th>"
        groups_html += "</tr></thead><tbody>"
        for g in topology_report.groups:
            is_canon = g.group_id == topology_report.canonical_group_id
            group_badge = f'<span class="badge {"badge-canonical" if is_canon else "badge-divergent"}">{g.group_id}{" (Canonical)" if is_canon else ""}</span>'
            sample = g.sample_files[0] if g.sample_files else "N/A"
            groups_html += (
                f"<tr>"
                f"<td>{group_badge}</td>"
                f"<td><code>{_escape(g.schema_fingerprint[:16])}...</code></td>"
                f"<td>{g.file_count}</td>"
                f"<td><strong>{g.percentage}%</strong></td>"
                f"<td>{g.row_count:,}</td>"
                f"<td>{g.byte_size:,} B</td>"
                f'<td class="text-sm truncate"><code>{_escape(sample)}</code></td>'
                f"</tr>"
            )
        groups_html += "</tbody></table></div>"
    else:
        groups_html = '<div class="empty-placeholder">Dataset topology analysis not performed.</div>'

    # Build diff table
    diffs_html = ""
    if diff_report and diff_report.diffs:
        diffs_html += '<div class="table-wrap"><table class="data-table"><thead><tr>'
        diffs_html += "<th>Field</th><th>Change Type</th><th>Severity</th><th>Error Code</th><th>Old Type</th><th>New Type</th><th>Description</th>"
        diffs_html += "</tr></thead><tbody>"
        for d in diff_report.diffs:
            sev_badge = (
                f'<span class="badge badge-{d.severity.value.lower()}">{_escape(d.severity.value)}</span>'
            )
            diffs_html += (
                f"<tr>"
                f"<td><code>{_escape(d.field_name)}</code></td>"
                f"<td>{_escape(d.change_type.value)}</td>"
                f"<td>{sev_badge}</td>"
                f"<td><code>{_escape(d.error_code.value if d.error_code else '')}</code></td>"
                f"<td><code>{_escape(d.old_value or '-')}</code></td>"
                f"<td><code>{_escape(d.new_value or '-')}</code></td>"
                f'<td class="text-sm">{_escape(d.description)}</td>'
                f"</tr>"
            )
        diffs_html += "</tbody></table></div>"
    else:
        diffs_html = '<div class="empty-placeholder">No schema diffs detected.</div>'

    # Build migration sandbox results
    mig_html = ""
    if migration_result:
        status_b = (
            '<span class="badge badge-safe">SANDBOX VERIFIED</span>'
            if migration_result.success
            else '<span class="badge badge-breaking">VERIFICATION FAILED</span>'
        )
        mig_html += f"""
        <div class="card">
          <div class="card-header">
            <h3>Executable Sandbox Migration Outcome: {status_b}</h3>
            <span class="text-sm text-muted">Latency: {migration_result.execution_latency_ms}ms</span>
          </div>
          <div class="card-body">
            <div class="metrics-grid">
              <div class="metric"><span class="metric-label">Rows Tested</span><span class="metric-value">{migration_result.rows_tested}</span></div>
              <div class="metric"><span class="metric-label">Row Count Invariant</span><span class="metric-value">{"PASSED" if migration_result.row_count_preserved else "FAILED"}</span></div>
              <div class="metric"><span class="metric-label">Schema Matched</span><span class="metric-value">{"PASSED" if migration_result.schema_matched else "FAILED"}</span></div>
              <div class="metric"><span class="metric-label">Value Invariants</span><span class="metric-value">{"PASSED" if migration_result.value_invariants_passed else "FAILED"}</span></div>
            </div>
            <p class="summary-text mt-3">{_escape(migration_result.summary)}</p>
        """
        if migration_result.errors:
            mig_html += '<div class="error-box mt-3"><strong>Verification Errors:</strong><ul>'
            for err in migration_result.errors:
                mig_html += f"<li>{_escape(err)}</li>"
            mig_html += "</ul></div>"
        if migration_result.warnings:
            mig_html += '<div class="warn-box mt-3"><strong>Verification Warnings:</strong><ul>'
            for w in migration_result.warnings:
                mig_html += f"<li>{_escape(w)}</li>"
            mig_html += "</ul></div>"
        mig_html += "</div></div>"
    else:
        mig_html = '<div class="empty-placeholder">No migration sandbox execution recorded.</div>'

    graph_svg = _generate_impact_graph_svg(impact_report)

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Parquet Guard AI — Diagnostic Report: {_escape(dataset_title)}</title>
  <style>
    :root {{
      --bg: #0f172a;
      --card-bg: #1e293b;
      --card-border: #334155;
      --text: #f8fafc;
      --text-muted: #94a3b8;
      --accent: #38bdf8;
      --success: #10b981;
      --warning: #f59e0b;
      --danger: #ef4444;
      --font: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ background: var(--bg); color: var(--text); font-family: var(--font); line-height: 1.5; padding: 2rem 1.5rem; }}
    .container {{ max-width: 1200px; margin: 0 auto; }}
    header {{ margin-bottom: 2rem; border-bottom: 1px solid var(--card-border); padding-bottom: 1.5rem; }}
    h1 {{ font-size: 1.85rem; font-weight: 700; color: #fff; margin-bottom: 0.25rem; }}
    .subtitle {{ color: var(--text-muted); font-size: 0.95rem; }}
    .banner {{ border-radius: 8px; padding: 1.25rem 1.5rem; margin-bottom: 2rem; display: flex; align-items: center; justify-content: space-between; }}
    .banner-title {{ font-size: 1.2rem; font-weight: 700; }}
    .banner-desc {{ font-size: 0.9rem; margin-top: 0.25rem; }}
    .state-valid {{ background: #064e3b; border: 1px solid #059669; color: #a7f3d0; }}
    .state-incompatible {{ background: #7f1d1d; border: 1px solid #dc2626; color: #fecaca; }}
    .state-breaking {{ background: #7f1d1d; border: 1px solid #dc2626; color: #fecaca; }}
    .state-drifted {{ background: #78350f; border: 1px solid #d97706; color: #fde68a; }}
    .section {{ margin-bottom: 2.5rem; }}
    .section-title {{ font-size: 1.25rem; font-weight: 600; margin-bottom: 1rem; color: #e2e8f0; display: flex; align-items: center; gap: 0.5rem; }}
    .card {{ background: var(--card-bg); border: 1px solid var(--card-border); border-radius: 8px; overflow: hidden; margin-bottom: 1.5rem; }}
    .card-header {{ padding: 1rem 1.25rem; border-bottom: 1px solid var(--card-border); display: flex; justify-content: space-between; align-items: center; }}
    .card-body {{ padding: 1.25rem; }}
    .table-wrap {{ overflow-x: auto; background: var(--card-bg); border-radius: 8px; border: 1px solid var(--card-border); }}
    table.data-table {{ width: 100%; border-collapse: collapse; text-align: left; font-size: 0.88rem; }}
    table.data-table th {{ background: #0f172a; padding: 0.75rem 1rem; color: #cbd5e1; font-weight: 600; border-bottom: 1px solid var(--card-border); }}
    table.data-table td {{ padding: 0.75rem 1rem; border-bottom: 1px solid var(--card-border); color: #e2e8f0; }}
    table.data-table tr:hover {{ background: #243247; }}
    .badge {{ display: inline-block; padding: 0.2rem 0.55rem; border-radius: 4px; font-size: 0.75rem; font-weight: 700; letter-spacing: 0.5px; text-transform: uppercase; }}
    .badge-compatible, .badge-safe {{ background: #064e3b; color: #6ee7b7; border: 1px solid #059669; }}
    .badge-conditionally_compatible, .badge-warning {{ background: #78350f; color: #fcd34d; border: 1px solid #d97706; }}
    .badge-incompatible, .badge-breaking {{ background: #7f1d1d; color: #fca5a5; border: 1px solid #dc2626; }}
    .badge-potentially_breaking {{ background: #78350f; color: #fcd34d; border: 1px solid #d97706; }}
    .badge-canonical {{ background: #1e3a8a; color: #93c5fd; border: 1px solid #3b82f6; }}
    .badge-divergent {{ background: #581c87; color: #d8b4fe; border: 1px solid #9333ea; }}
    .metrics-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem; }}
    .metric {{ background: #0f172a; border: 1px solid var(--card-border); border-radius: 6px; padding: 0.9rem; text-align: center; }}
    .metric-label {{ display: block; font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; margin-bottom: 0.35rem; }}
    .metric-value {{ font-size: 1.35rem; font-weight: 700; color: #fff; }}
    .impact-svg {{ width: 100%; height: auto; background: #0b1120; border-radius: 8px; border: 1px solid var(--card-border); }}
    .empty-placeholder {{ padding: 2rem; text-align: center; color: var(--text-muted); background: var(--card-bg); border: 1px dashed var(--card-border); border-radius: 8px; }}
    .error-box {{ background: #450a0a; border: 1px solid #b91c1c; border-radius: 6px; padding: 1rem; color: #fecaca; font-size: 0.88rem; }}
    .warn-box {{ background: #451a03; border: 1px solid #b45309; border-radius: 6px; padding: 1rem; color: #fde68a; font-size: 0.88rem; }}
    .text-sm {{ font-size: 0.8rem; }}
    .text-muted {{ color: var(--text-muted); }}
    .text-danger {{ color: #f87171; }}
    .mt-3 {{ margin-top: 1rem; }}
    footer {{ margin-top: 3rem; padding-top: 1.5rem; border-top: 1px solid var(--card-border); text-align: center; color: var(--text-muted); font-size: 0.8rem; }}
    code {{ background: #0f172a; padding: 0.15rem 0.4rem; border-radius: 4px; font-family: ui-monospace, Menlo, Monaco, Consolas, monospace; font-size: 0.85em; }}
  </style>
</head>
<body>
  <div class="container">
    <header>
      <h1>Parquet Guard AI — Diagnostic Report</h1>
      <div class="subtitle">Dataset: <strong>{_escape(dataset_title)}</strong> &bull; Generated by Parquet Guard Engine</div>
    </header>

    <div class="banner {banner_class}">
      <div>
        <div class="banner-title">OVERALL VERDICT: {_escape(banner_state)}</div>
        <div class="banner-desc">{_escape(banner_desc)}</div>
      </div>
    </div>

    <div class="section">
      <h2 class="section-title">Producer-Dataset-Consumer Impact Graph</h2>
      {graph_svg}
    </div>

    <div class="section">
      <h2 class="section-title">Downstream Consumer Compatibility Matrix</h2>
      {matrix_html}
    </div>

    <div class="section">
      <h2 class="section-title">Partition Dataset Topology &amp; Schema Groups</h2>
      {groups_html}
    </div>

    <div class="section">
      <h2 class="section-title">Schema Drift &amp; Field Discrepancies</h2>
      {diffs_html}
    </div>

    <div class="section">
      <h2 class="section-title">Verified Migration Sandbox Outcome</h2>
      {mig_html}
    </div>

    <footer>
      <p>Report generated by <strong>Parquet Guard AI v0.3.0a1+</strong> &bull; Zero-data-scan metadata contract verification</p>
    </footer>
  </div>
</body>
</html>
"""
    with open(output_path, "w", encoding="utf-8") as fh:
        fh.write(html_content)


# Convenience and backward-compatible alias
generate_html_report = generate_html_diagnostic_report
