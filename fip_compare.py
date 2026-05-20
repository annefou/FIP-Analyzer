#!/usr/bin/env python3
"""
FIP Compare - Cross-compare multiple FAIR Implementation Profiles.

Loads N FIPs (TriG headers + network fetch, or JSON exports) and produces:
  1. Side-by-side comparison table   (comparison_table.md)
  2. Overlap & uniqueness analysis   (overlap_analysis.md)
  3. Coverage / gap matrix           (coverage_matrix.md)
  4. Visual HTML dashboard           (dashboard.html)

Designed for the FIESTA use case (Astro / Bio / EO) but works for any N >= 2.
"""

import sys
import os
import json
import base64
import hashlib
import argparse
from io import BytesIO
from pathlib import Path
from collections import defaultdict

# Reuse the existing reader's parsing logic so the two scripts stay in sync.
from fip_reader import (
    FAIR_PRINCIPLES,
    parse_fip_header,
    fetch_nanopub,
    extract_declarations_from_index,
    parse_declaration,
    organize_by_principle,
    read_fip_from_json,
    organize_by_principle_from_json,
)

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
except ImportError:
    print("Installing matplotlib...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install",
                           "matplotlib", "numpy",
                           "--break-system-packages", "-q"])
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

try:
    from matplotlib_venn import venn2, venn3
    HAS_VENN = True
except ImportError:
    print("Installing matplotlib-venn...")
    import subprocess
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install",
                               "matplotlib-venn",
                               "--break-system-packages", "-q"])
        from matplotlib_venn import venn2, venn3
        HAS_VENN = True
    except Exception:
        HAS_VENN = False

try:
    import markdown as md_lib
except ImportError:
    print("Installing markdown...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install",
                           "markdown", "--break-system-packages", "-q"])
    import markdown as md_lib


CACHE_DIR = Path(".fip_cache")


# ---------- Cached fetch ----------------------------------------------------

def cached_fetch(uri: str):
    """Fetch a nanopub, caching by trusty-URI id under .fip_cache/."""
    CACHE_DIR.mkdir(exist_ok=True)
    np_id = uri.rstrip("/").split("/")[-1]
    cache_file = CACHE_DIR / f"{np_id}.trig"
    if cache_file.exists():
        from rdflib import ConjunctiveGraph
        g = ConjunctiveGraph()
        try:
            g.parse(str(cache_file), format="trig")
            return g
        except Exception:
            pass  # fall through to refetch

    g = fetch_nanopub(uri)
    if g is not None:
        try:
            g.serialize(destination=str(cache_file), format="trig")
        except Exception:
            pass
    return g


# ---------- Load FIPs -------------------------------------------------------

def load_fip(path: str, fetch_remote: bool = True, debug: bool = False):
    """Load one FIP. Returns (fip_info, organized) where organized matches
    fip_reader.organize_by_principle's shape."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)

    if p.suffix.lower() == ".json":
        fip_info, declarations = read_fip_from_json(str(p))
        return fip_info, organize_by_principle_from_json(declarations)

    fip_info = parse_fip_header(str(p))
    declarations = []

    if fetch_remote and fip_info.get("declaration_index"):
        print(f"   📡 Fetching declaration index for {fip_info.get('label', p.name)}...")
        index_graph = cached_fetch(fip_info["declaration_index"])
        if index_graph is None:
            print(f"   ⚠️  Could not fetch index; declarations will be empty.")
            return fip_info, organize_by_principle([])

        decl_uris = extract_declarations_from_index(index_graph, debug=debug)
        print(f"   Found {len(decl_uris)} declarations; fetching...")

        for i, decl_uri in enumerate(decl_uris[:50], start=1):
            g = cached_fetch(decl_uri)
            if g is not None:
                decl = parse_declaration(g, debug=debug)
                if decl.get("question_id"):
                    declarations.append(decl)
            print(f"\r      {i}/{min(len(decl_uris), 50)}", end="")
        print()

    return fip_info, organize_by_principle(declarations)


def short_name(fip_info: dict, fallback: str) -> str:
    """Derive a short community name from the FIP label."""
    label = (fip_info.get("label") or "").strip()
    if not label:
        return fallback
    # "FIESTA Astro FIP" -> "Astro", "FIESTA Bio FIP" -> "Bio"
    stripped = label.replace("FIP", "").strip()
    for prefix in ("FIESTA", "FAIR Implementation Profile"):
        if stripped.lower().startswith(prefix.lower()):
            stripped = stripped[len(prefix):].strip()
    return stripped or label


# ---------- Resource helpers ------------------------------------------------

def resource_key(res: dict) -> str:
    """Canonical key for matching resources across FIPs. Uses URI if present,
    otherwise falls back to the label. Nanopub-fragment variants
    (.../<id>/Foo vs .../<id>#Foo) are treated as equivalent."""
    uri = (res.get("uri") or "").strip()
    if uri:
        # Treat trailing /X as #X so trustyuri fragment-style and path-style
        # references to the same resource collapse together.
        if "#" not in uri and "/" in uri:
            base, _, last = uri.rpartition("/")
            if last and base.startswith(("http://purl.org/np/",
                                          "https://w3id.org/np/")):
                uri = f"{base}#{last}"
        return uri
    return f"label:{(res.get('label') or '').strip().lower()}"


def resource_display(res: dict) -> str:
    """Short human-readable string for a resource."""
    label = (res.get("label") or "").strip() or "(unnamed)"
    suffix = " (planned)" if res.get("type") == "planned" else (
        " (replacement)" if res.get("type") == "replacement" else "")
    return f"{label}{suffix}"


def is_stub(res: dict) -> bool:
    """A 'stub' declaration is one with no resource URI and no real label —
    the FIP Wizard sometimes emits these for unanswered questions. The
    original fip_reader printed them as '• None'; we drop them for comparison."""
    uri = (res.get("uri") or "").strip()
    label = (res.get("label") or "").strip()
    return not uri and not label


def resources_for_axis(organized: dict, principle: str, axis: str) -> list:
    """axis is 'data' or 'metadata'. Stub declarations are filtered out."""
    return [r for r in (organized.get(principle, {}).get(axis, []) or [])
            if not is_stub(r)]


# ---------- 1. Side-by-side table ------------------------------------------

def render_side_by_side(fips: list) -> str:
    """Markdown table: rows = principle/axis, columns = community."""
    lines = []
    lines.append("# FIP Side-by-Side Comparison")
    lines.append("")
    lines.append(f"Comparing {len(fips)} FAIR Implementation Profiles:")
    lines.append("")
    for f in fips:
        lines.append(f"- **{f['name']}** — {f['info'].get('label', '(no label)')}"
                     + (f" — created {f['info']['created']}" if f['info'].get('created') else ""))
    lines.append("")

    names = [f["name"] for f in fips]
    header = "| Principle | Axis | " + " | ".join(names) + " |"
    sep = "|" + "|".join(["---"] * (2 + len(names))) + "|"

    for principle, label in FAIR_PRINCIPLES.items():
        lines.append("")
        lines.append(f"## {label}")
        lines.append("")
        lines.append(header)
        lines.append(sep)
        for axis_key, axis_label in (("data", "Data"), ("metadata", "Metadata")):
            cells = []
            any_content = False
            for f in fips:
                resources = resources_for_axis(f["organized"], principle, axis_key)
                if resources:
                    any_content = True
                    cell = "<br>".join(f"• {resource_display(r)}" for r in resources)
                else:
                    cell = "—"
                cells.append(cell)
            # Suppress rows where no community has anything to make report shorter
            if any_content:
                lines.append(f"| {principle} | {axis_label} | " + " | ".join(cells) + " |")

    return "\n".join(lines) + "\n"


# ---------- 2. Overlap & uniqueness ----------------------------------------

def compute_overlap(fips: list):
    """Return a dict with overlap stats. Keys keyed by resource_key."""
    # community -> set of resource_keys (across all principles/axes)
    sets_by_community = {f["name"]: set() for f in fips}
    # Map key -> representative label (first one seen)
    key_label = {}

    for f in fips:
        for principle in FAIR_PRINCIPLES:
            for axis in ("data", "metadata"):
                for res in resources_for_axis(f["organized"], principle, axis):
                    k = resource_key(res)
                    sets_by_community[f["name"]].add(k)
                    key_label.setdefault(k, resource_display(res))

    return sets_by_community, key_label


def render_overlap(fips: list) -> str:
    sets_by_community, key_label = compute_overlap(fips)
    names = [f["name"] for f in fips]
    all_sets = [sets_by_community[n] for n in names]

    lines = ["# FAIR Supporting Resource Overlap & Uniqueness Analysis", ""]
    lines.append(f"Comparing the FAIR Supporting Resources declared by "
                 f"{len(fips)} FIPs across all FAIR principles and axes.")
    lines.append("")

    # Per-community totals
    lines.append("## FAIR Supporting Resource totals")
    lines.append("")
    lines.append("| Community | Distinct FAIR Supporting Resources declared |")
    lines.append("|---|---|")
    for n in names:
        lines.append(f"| {n} | {len(sets_by_community[n])} |")
    lines.append("")

    # Pairwise Jaccard
    if len(names) >= 2:
        lines.append("## Pairwise Jaccard similarity")
        lines.append("")
        lines.append("Jaccard = |A ∩ B| / |A ∪ B|. 1.0 = identical, 0.0 = disjoint.")
        lines.append("")
        lines.append("| | " + " | ".join(names) + " |")
        lines.append("|---|" + "|".join(["---"] * len(names)) + "|")
        for i, ni in enumerate(names):
            row = [ni]
            for j, nj in enumerate(names):
                a, b = sets_by_community[ni], sets_by_community[nj]
                if not (a or b):
                    row.append("—")
                elif i == j:
                    row.append("1.00")
                else:
                    j_idx = len(a & b) / len(a | b) if (a | b) else 0.0
                    row.append(f"{j_idx:.2f}")
            lines.append("| " + " | ".join(row) + " |")
        lines.append("")

    # Shared by ALL
    if all_sets:
        common = set.intersection(*all_sets) if all_sets else set()
        lines.append(f"## Shared by all {len(names)} communities ({len(common)})")
        lines.append("")
        if common:
            for k in sorted(common, key=lambda x: key_label.get(x, x).lower()):
                lines.append(f"- {key_label.get(k, k)}  \n  `{k}`")
        else:
            lines.append("_(none)_")
        lines.append("")

    # Shared by some but not all (only meaningful for >=3 FIPs)
    if len(names) >= 3:
        union_all = set().union(*all_sets) if all_sets else set()
        common = set.intersection(*all_sets) if all_sets else set()
        partial = defaultdict(list)  # frozenset(community names) -> resources
        for k in union_all:
            if k in common:
                continue
            holders = tuple(n for n in names if k in sets_by_community[n])
            if 2 <= len(holders) < len(names):
                partial[holders].append(k)
        if partial:
            lines.append("## Shared by some (but not all)")
            lines.append("")
            for holders in sorted(partial.keys(), key=lambda h: (-len(h), h)):
                lines.append(f"### {' + '.join(holders)} ({len(partial[holders])})")
                for k in sorted(partial[holders], key=lambda x: key_label.get(x, x).lower()):
                    lines.append(f"- {key_label.get(k, k)}  \n  `{k}`")
                lines.append("")

    # Unique to each
    lines.append("## Unique to each community")
    lines.append("")
    for i, n in enumerate(names):
        others = set().union(*[sets_by_community[m] for j, m in enumerate(names) if j != i])
        unique = sets_by_community[n] - others
        lines.append(f"### {n} only ({len(unique)})")
        lines.append("")
        if unique:
            for k in sorted(unique, key=lambda x: key_label.get(x, x).lower()):
                lines.append(f"- {key_label.get(k, k)}  \n  `{k}`")
        else:
            lines.append("_(none)_")
        lines.append("")

    return "\n".join(lines) + "\n"


# ---------- 3. Coverage / gap matrix ----------------------------------------

def render_coverage(fips: list) -> str:
    names = [f["name"] for f in fips]
    lines = ["# FIP Coverage / Gap Matrix", ""]
    lines.append("For each (principle, axis), `✓` means at least one FAIR Supporting "
                 "Resource was declared; blank means no declaration. Numbers in "
                 "parentheses show the count of FAIR Supporting Resources.")
    lines.append("")

    header = "| Principle | Axis | " + " | ".join(names) + " |"
    sep = "|" + "|".join(["---"] * (2 + len(names))) + "|"
    lines.append(header)
    lines.append(sep)

    # per-community totals for summary
    declared_count = {n: 0 for n in names}
    total_cells = 0

    for principle, _label in FAIR_PRINCIPLES.items():
        for axis_key, axis_label in (("data", "Data"), ("metadata", "Metadata")):
            row = [principle, axis_label]
            for f in fips:
                resources = resources_for_axis(f["organized"], principle, axis_key)
                if resources:
                    declared_count[f["name"]] += 1
                    row.append(f"✓ ({len(resources)})")
                else:
                    row.append("")
            lines.append("| " + " | ".join(row) + " |")
            total_cells += 1

    lines.append("")
    lines.append("## Coverage summary")
    lines.append("")
    lines.append("| Community | Cells declared | Cells empty | Coverage |")
    lines.append("|---|---|---|---|")
    for n in names:
        declared = declared_count[n]
        empty = total_cells - declared
        pct = 100.0 * declared / total_cells if total_cells else 0.0
        lines.append(f"| {n} | {declared} | {empty} | {pct:.0f}% |")
    lines.append("")

    return "\n".join(lines) + "\n"


# ---------- 4. Visual HTML dashboard ----------------------------------------

def fig_to_b64(fig) -> str:
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=110, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode("ascii")


def render_coverage_html_table(fips: list) -> str:
    """Inline HTML coverage matrix that names the standards in each cell.
    Empty cells are subtly shaded; cells with all 3 communities agreeing
    get a stronger highlight."""
    names = [f["name"] for f in fips]
    rows_html = []
    for principle, principle_label in FAIR_PRINCIPLES.items():
        for axis_key, axis_label in (("data", "Data"), ("metadata", "Metadata")):
            cells = []
            keys_per_fip = []
            for f in fips:
                resources = resources_for_axis(f["organized"], principle, axis_key)
                if resources:
                    items = "<br>".join(
                        f'<span class="r">{resource_display(r)}</span>'
                        for r in resources)
                    cells.append(f'<td class="has">{items}</td>')
                    keys_per_fip.append({resource_key(r) for r in resources})
                else:
                    cells.append('<td class="empty">—</td>')
                    keys_per_fip.append(set())
            # Highlight rows where every community declared something
            row_class = ""
            if all(s for s in keys_per_fip):
                common = set.intersection(*keys_per_fip)
                if common:
                    row_class = " class=\"all-agree\""
                else:
                    row_class = " class=\"all-present\""
            elif not any(s for s in keys_per_fip):
                row_class = " class=\"all-empty\""

            rows_html.append(
                f"<tr{row_class}>"
                f"<td class=\"princ\"><b>{principle}</b><br>"
                f"<span class=\"axis\">{axis_label}</span></td>"
                + "".join(cells)
                + "</tr>"
            )

    header = ("<tr><th>Principle / axis</th>"
              + "".join(f"<th>{n}</th>" for n in names)
              + "</tr>")
    legend = ('<p class="legend">'
              '<span class="swatch all-agree"></span> all 3 declared and overlap '
              '<span class="swatch all-present"></span> all 3 declared (different choices) '
              '<span class="swatch all-empty"></span> all 3 empty</p>')
    return (legend
            + f'<table class="coverage" style="--n-communities: {len(names)}">'
              '<thead>' + header + '</thead><tbody>'
            + "".join(rows_html) + '</tbody></table>')


# ---------- Galaxy-tool implications ---------------------------------------

def categorize_cell(keys_by_community: dict, names: list) -> tuple:
    """Categorize one (principle, axis) cell across all communities.

    Returns (category, shared_keys, empty_names, outliers).

    Categories:
      full_consensus        — all declared, all chose exactly the same resource(s)
      consensus_with_extras — all declared, at least one shared, some added more
      majority_choice       — all declared, no global overlap, but a majority
                              agrees on at least one resource (outliers differ)
      partial_aligned       — some empty; declaring communities share ≥1 resource
      partial_divergent     — some empty; declaring communities chose differently
      isolated_choice       — only one community declared
      all_divergent         — all declared, every community chose something unique
      total_gap             — nobody declared
    """
    from itertools import combinations
    non_empty = [n for n in names if keys_by_community[n]]
    empty = [n for n in names if not keys_by_community[n]]

    if not non_empty:
        return ("total_gap", set(), empty, [])

    declared_sets = [keys_by_community[n] for n in non_empty]
    shared_all = set.intersection(*declared_sets) if declared_sets else set()

    if not empty:
        if shared_all:
            if all(keys_by_community[n] == shared_all for n in non_empty):
                return ("full_consensus", shared_all, empty, [])
            return ("consensus_with_extras", shared_all, empty, [])
        # No global overlap — look for a strict-majority sub-agreement
        n = len(non_empty)
        for size in range(n - 1, 1, -1):
            for combo in combinations(non_empty, size):
                sub_shared = set.intersection(*[keys_by_community[c] for c in combo])
                if sub_shared:
                    outliers = [c for c in non_empty if c not in combo]
                    return ("majority_choice", sub_shared, [], outliers)
        return ("all_divergent", set(), empty, [])

    if len(non_empty) == 1:
        return ("isolated_choice", declared_sets[0], empty, [])

    if shared_all:
        return ("partial_aligned", shared_all, empty, [])
    return ("partial_divergent", set(), empty, [])


CATEGORY_META = {
    "full_consensus":         ("✅", "Consensus",            "consensus"),
    "consensus_with_extras":  ("✅", "Consensus (+ extras)", "consensus"),
    "majority_choice":        ("💡", "Majority choice",      "adopt"),
    "partial_aligned":        ("💡", "Adoption opportunity", "adopt"),
    "isolated_choice":        ("💭", "Single declaration",   "adopt"),
    "partial_divergent":      ("⚠️", "Divergent (with gap)", "divergent"),
    "all_divergent":          ("⚠️", "Domain-specific",      "divergent"),
    "total_gap":              ("❓", "Joint decision",       "gap"),
}


def analyze_implications(fips: list) -> list:
    """Walk every (principle, axis) cell and categorize."""
    names = [f["name"] for f in fips]
    rows = []
    for principle, principle_label in FAIR_PRINCIPLES.items():
        for axis_key, axis_label in (("data", "Data"), ("metadata", "Metadata")):
            resources_by_community = {
                f["name"]: resources_for_axis(f["organized"], principle, axis_key)
                for f in fips
            }
            keys_by_community = {n: {resource_key(r) for r in rs}
                                 for n, rs in resources_by_community.items()}
            # Map every key seen back to a display label (first occurrence wins)
            key_to_label = {}
            for rs in resources_by_community.values():
                for r in rs:
                    key_to_label.setdefault(resource_key(r), resource_display(r))

            category, shared, empty, outliers = categorize_cell(keys_by_community, names)
            rows.append({
                "principle": principle,
                "principle_label": principle_label,
                "axis": axis_label,
                "axis_key": axis_key,
                "resources_by_community": resources_by_community,
                "keys_by_community": keys_by_community,
                "key_to_label": key_to_label,
                "shared": shared,
                "empty": empty,
                "outliers": outliers,
                "non_empty": [n for n in names if n not in empty],
                "category": category,
            })
    return rows


def _labels(keys, key_to_label):
    return [key_to_label.get(k, k) for k in sorted(keys, key=lambda k: key_to_label.get(k, k).lower())]


def _community_choices_html(row, names):
    """One-line summary: 'Astro: DOI; Bio: URI; EO: DOI'."""
    parts = []
    for n in names:
        rs = row["resources_by_community"][n]
        if rs:
            parts.append(f"<b>{n}</b>: " + ", ".join(resource_display(r) for r in rs))
        else:
            parts.append(f"<b>{n}</b>: <span class='empty-inline'>—</span>")
    return " &nbsp;|&nbsp; ".join(parts)


def render_implications_html(fips: list, rows: list) -> str:
    names = [f["name"] for f in fips]
    by_group = defaultdict(list)
    for r in rows:
        by_group[CATEGORY_META[r["category"]][2]].append(r)

    sections = []

    # --- ✅ Consensus -------------------------------------------------------
    consensus_rows = by_group.get("consensus", [])
    sections.append(f"<h3>✅ Consensus — Galaxy can default to these ({len(consensus_rows)})</h3>")
    if consensus_rows:
        sections.append(
            "<p>All communities declared at least one common FAIR Supporting "
            "Resource. Galaxy tools can adopt these as defaults with high "
            "confidence — any FIESTA-aligned community will accept them out "
            "of the box.</p>"
        )
        sections.append("<table><thead><tr><th>Principle / axis</th>"
                        "<th>Shared standard</th>"
                        "<th>Galaxy implication</th></tr></thead><tbody>")
        for r in consensus_rows:
            shared_labels = ", ".join(_labels(r["shared"], r["key_to_label"]))
            extras = []
            for n in r["non_empty"]:
                extra_keys = r["keys_by_community"][n] - r["shared"]
                if extra_keys:
                    extras.append(f"{n} also uses " + ", ".join(_labels(extra_keys, r["key_to_label"])))
            note = "; ".join(extras) if extras else "All choose exactly this."
            sections.append(
                f"<tr><td><b>{r['principle']}</b> — {r['axis']}<br>"
                f"<span class='small'>{r['principle_label']}</span></td>"
                f"<td><b>{shared_labels}</b></td>"
                f"<td>{note}</td></tr>"
            )
        sections.append("</tbody></table>")
    else:
        sections.append("<p><i>No cells in this category.</i></p>")

    # --- 💡 Adoption opportunities ----------------------------------------
    adopt_rows = by_group.get("adopt", [])
    sections.append(f"<h3>💡 Recommended primary standard + community extensions ({len(adopt_rows)})</h3>")
    if adopt_rows:
        sections.append(
            "<p>One or more communities already converge on a standard. Galaxy "
            "tools should support that as the <b>primary</b> standard. When a "
            "community that chose differently — or hasn't chosen yet — wants to "
            "use a tool, they can <b>extend</b> it to also support their own "
            "choice. The burden is on the new community joining, not on the "
            "communities already aligned.</p>"
        )
        sections.append("<table><thead><tr><th>Principle / axis</th>"
                        "<th>Choices</th>"
                        "<th>Recommendation</th></tr></thead><tbody>")
        for r in adopt_rows:
            aligned = ", ".join(_labels(r["shared"], r["key_to_label"]))
            if r["category"] == "partial_aligned":
                rec = (f"Recommend Galaxy tools support <b>{aligned}</b> "
                       f"(chosen by <b>{', '.join(r['non_empty'])}</b>). "
                       f"When <b>{', '.join(r['empty'])}</b> is interested in a "
                       f"tool, they can extend it to support their own choice "
                       f"once they make one.")
            elif r["category"] == "majority_choice":
                majority = [n for n in r["non_empty"] if n not in r["outliers"]]
                outlier_bits = []
                for o in r["outliers"]:
                    diff_keys = r["keys_by_community"][o] - r["shared"]
                    if diff_keys:
                        outlier_bits.append(
                            f"<b>{o}</b> uses "
                            f"<b>{', '.join(_labels(diff_keys, r['key_to_label']))}</b>")
                outlier_text = "; ".join(outlier_bits) if outlier_bits else ""
                rec = (f"Recommend Galaxy tools support <b>{aligned}</b> as the "
                       f"primary standard (chosen by <b>{', '.join(majority)}</b>). "
                       + (outlier_text + ". " if outlier_text else "")
                       + f"When <b>{', '.join(r['outliers'])}</b> is interested in "
                         f"a tool, they can extend it to also support their choice.")
            else:  # isolated_choice
                only = r["non_empty"][0]
                rec = (f"Only <b>{only}</b> has declared a choice: <b>{aligned}</b>. "
                       f"Galaxy tools could adopt this as the primary standard; "
                       f"<b>{', '.join(r['empty'])}</b> can extend tools they want "
                       f"to use once they pick their own standard.")
            sections.append(
                f"<tr><td><b>{r['principle']}</b> — {r['axis']}<br>"
                f"<span class='small'>{r['principle_label']}</span></td>"
                f"<td>{_community_choices_html(r, names)}</td>"
                f"<td>{rec}</td></tr>"
            )
        sections.append("</tbody></table>")
    else:
        sections.append("<p><i>No cells in this category.</i></p>")

    # --- ⚠️ Divergent -------------------------------------------------------
    div_rows = by_group.get("divergent", [])
    sections.append(f"<h3>⚠️ Domain-specific — tools must be built pluggable ({len(div_rows)})</h3>")
    if div_rows:
        sections.append(
            "<p>Each community chose a different FAIR Supporting Resource, "
            "typically because the choice is genuinely domain-specific (file "
            "formats, domain vocabularies). Galaxy tools in this area should "
            "be built with a pluggable / configurable layer from day one. A "
            "new community joining brings their own FAIR Supporting Resource "
            "and extends the tool with it.</p>"
        )
        sections.append("<table><thead><tr><th>Principle / axis</th>"
                        "<th>Choices</th>"
                        "<th>Implication</th></tr></thead><tbody>")
        for r in div_rows:
            if r["category"] == "all_divergent":
                impl = ("All 3 communities chose different FAIR Supporting "
                        "Resources — these look genuinely domain-specific. "
                        "Build the tool with a pluggable layer; each community "
                        "brings its own.")
            else:
                impl = (f"Declared communities disagree, and <b>{', '.join(r['empty'])}</b> "
                        f"hasn't chosen yet. Build the tool pluggable; "
                        f"<b>{', '.join(r['empty'])}</b> plugs in their choice "
                        f"when they make one.")
            sections.append(
                f"<tr><td><b>{r['principle']}</b> — {r['axis']}<br>"
                f"<span class='small'>{r['principle_label']}</span></td>"
                f"<td>{_community_choices_html(r, names)}</td>"
                f"<td>{impl}</td></tr>"
            )
        sections.append("</tbody></table>")
    else:
        sections.append("<p><i>No cells in this category.</i></p>")

    # --- ❓ Joint decision needed -----------------------------------------
    gap_rows = by_group.get("gap", [])
    sections.append(f"<h3>❓ Joint decisions needed ({len(gap_rows)})</h3>")
    if gap_rows:
        sections.append(
            "<p>None of the three communities has declared anything for these "
            "principles. The FIESTA communities should agree together on what "
            "to use here — and until they do, Galaxy tools cannot make "
            "meaningful choices for these aspects of FAIR.</p>"
        )
        sections.append("<ul>")
        for r in gap_rows:
            sections.append(
                f"<li><b>{r['principle']} — {r['axis']}</b>: "
                f"<span class='small'>{r['principle_label']}</span></li>"
            )
        sections.append("</ul>")
    else:
        sections.append("<p><i>No cells in this category.</i></p>")

    return "\n".join(sections)


def render_implications_md(fips: list, rows: list) -> str:
    """Plain-markdown version for the linked .md/.html sibling."""
    names = [f["name"] for f in fips]
    by_group = defaultdict(list)
    for r in rows:
        by_group[CATEGORY_META[r["category"]][2]].append(r)

    out = ["# Galaxy-tool implications", ""]
    out.append(f"For each FAIR principle and axis, the choices by **{', '.join(names)}** "
               "fall into one of four categories below.")
    out.append("")

    def cell_choices(r):
        parts = []
        for n in names:
            rs = r["resources_by_community"][n]
            parts.append(f"**{n}**: " + (", ".join(resource_display(x) for x in rs)
                                          if rs else "—"))
        return " | ".join(parts)

    out.append("## ✅ Consensus")
    out.append("")
    for r in by_group.get("consensus", []):
        shared_labels = ", ".join(_labels(r["shared"], r["key_to_label"]))
        out.append(f"- **{r['principle']} {r['axis']}** ({r['principle_label']}) "
                   f"→ shared: **{shared_labels}**. {cell_choices(r)}")
    out.append("")

    out.append("## 💡 Recommended primary standard + community extensions")
    out.append("")
    out.append("Galaxy tools should support the primary standard below. Communities "
               "that chose differently — or haven't chosen yet — extend the tool "
               "when they want to use it.")
    out.append("")
    for r in by_group.get("adopt", []):
        aligned = ", ".join(_labels(r["shared"], r["key_to_label"]))
        if r["category"] == "majority_choice":
            majority = [n for n in r["non_empty"] if n not in r["outliers"]]
            rec = (f"recommend supporting **{aligned}** (used by **{', '.join(majority)}**); "
                   f"**{', '.join(r['outliers'])}** extend tools to support their choice")
        elif r["category"] == "partial_aligned":
            rec = (f"recommend supporting **{aligned}** (used by **{', '.join(r['non_empty'])}**); "
                   f"**{', '.join(r['empty'])}** extend when interested")
        else:  # isolated_choice
            rec = (f"only **{r['non_empty'][0]}** chose **{aligned}** — adopt as primary; "
                   f"**{', '.join(r['empty'])}** extend later")
        out.append(f"- **{r['principle']} {r['axis']}** ({r['principle_label']}) "
                   f"→ {cell_choices(r)}. {rec}.")
    out.append("")

    out.append("## ⚠️ Domain-specific — build tools pluggable")
    out.append("")
    out.append("Choices look genuinely domain-specific. Build tools with a pluggable "
               "layer; each community plugs in their own choice.")
    out.append("")
    for r in by_group.get("divergent", []):
        out.append(f"- **{r['principle']} {r['axis']}** ({r['principle_label']}) "
                   f"→ {cell_choices(r)}.")
    out.append("")

    out.append("## ❓ Joint decisions needed")
    out.append("")
    for r in by_group.get("gap", []):
        out.append(f"- **{r['principle']} {r['axis']}** — {r['principle_label']}")
    out.append("")
    return "\n".join(out)


def plot_venn(fips: list):
    sets_by_community, _ = compute_overlap(fips)
    names = [f["name"] for f in fips]
    fig, ax = plt.subplots(figsize=(5.5, 5.5))
    if len(names) == 2:
        venn2([sets_by_community[n] for n in names], set_labels=names, ax=ax)
    elif len(names) == 3:
        venn3([sets_by_community[n] for n in names], set_labels=names, ax=ax)
    else:
        ax.axis("off")
        ax.text(0.5, 0.5, f"Venn diagram only supported for 2 or 3 FIPs (got {len(names)}).",
                ha="center", va="center")
    ax.set_title("FAIR Supporting Resource overlap across communities")
    return fig


DASHBOARD_CSS = """
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
         max-width: 1300px; margin: 2em auto; padding: 0 1em; color: #222; }
  h1 { border-bottom: 2px solid #3b8bba; padding-bottom: .3em; }
  h2 { color: #2a5d7a; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 1.5em; }
  .card { background: #fafafa; border: 1px solid #ddd; border-radius: 8px;
          padding: 1em; }
  .card.full { grid-column: 1 / -1; }
  .card img { max-width: 100%; height: auto; display: block; margin: auto; }
  ul { line-height: 1.6; }
  .links a { margin-right: 1em; }
  table { border-collapse: collapse; width: 100%; font-size: 0.9em; }
  th, td { border: 1px solid #ccc; padding: 0.4em 0.6em; vertical-align: top;
           text-align: left; }
  th { background: #eef3f7; }
  table.coverage { table-layout: fixed; }
  table.coverage th:first-child,
  table.coverage td.princ { width: 14%; }
  table.coverage th:not(:first-child),
  table.coverage td.has,
  table.coverage td.empty { width: calc((100% - 14%) / var(--n-communities, 3));
                            word-wrap: break-word; }
  table.coverage td.princ { background: #f4f6f8; white-space: nowrap;
                            font-size: 0.92em; }
  table.coverage td.princ .axis { color: #666; font-size: 0.85em; }
  table.coverage td.has { background: #ecf7ec; }
  table.coverage td.empty { color: #aaa; background: #fafafa;
                            text-align: center; }
  table.coverage tr.all-agree td.has { background: #cdebcd; }
  table.coverage tr.all-empty td.empty { background: #f5e9e9; color: #b66; }
  .r { display: inline-block; }
  .legend { font-size: 0.85em; color: #555; }
  .legend .swatch { display: inline-block; width: 14px; height: 14px;
                    border: 1px solid #ccc; margin: 0 0.3em 0 1em;
                    vertical-align: middle; }
  .legend .swatch.all-agree { background: #cdebcd; }
  .legend .swatch.all-present { background: #ecf7ec; }
  .legend .swatch.all-empty { background: #f5e9e9; }
"""


def render_report_page(title: str, body_html: str) -> str:
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>{title}</title>
<style>{DASHBOARD_CSS}</style></head>
<body>
<p><a href="index.html">← Back to dashboard</a></p>
{body_html}
</body></html>
"""


def render_dashboard(fips: list, rows: list) -> str:
    venn_img = None
    if HAS_VENN and 2 <= len(fips) <= 3:
        venn_img = fig_to_b64(plot_venn(fips))

    names_html = "".join(
        f"<li><b>{f['name']}</b> — {f['info'].get('label', '(no label)')}"
        + (f" — created {f['info']['created']}" if f['info'].get('created') else "")
        + "</li>"
        for f in fips
    )

    venn_block = (
        '<div class="card full">'
        f'<h2>FAIR Supporting Resource overlap across communities</h2>'
        f'<img src="data:image/png;base64,{venn_img}" alt="Venn diagram"/>'
        '</div>'
    ) if venn_img else ""

    coverage_table_html = render_coverage_html_table(fips)
    implications_html = render_implications_html(fips, rows)

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>FIP Comparison Dashboard</title>
<style>{DASHBOARD_CSS}</style>
</head>
<body>
<h1>FIP Comparison Dashboard</h1>
<p>Comparing {len(fips)} FAIR Implementation Profiles:</p>
<ul>{names_html}</ul>
<p class="links">
  See also:
  <a href="comparison_table.html">Side-by-side table</a>
  <a href="overlap_analysis.html">Overlap analysis</a>
  <a href="coverage_matrix.html">Coverage matrix (markdown view)</a>
  <a href="implications.html">Galaxy implications (markdown view)</a>
</p>

<div class="card full">
  <h2>Galaxy tool implications</h2>
  <p>Each cell below shows what FAIR Supporting Resources each community
     declared for a given FAIR principle. The grouped recommendations suggest
     what Galaxy tools should default to, where configurability is needed, and
     where the FIESTA communities still need to agree on a shared FAIR
     Supporting Resource.</p>
  {implications_html}
</div>

<div class="card full">
  <h2>Coverage matrix — FAIR Supporting Resources declared per community</h2>
  {coverage_table_html}
</div>

{venn_block}
</body>
</html>
"""


# ---------- CLI -------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Cross-compare multiple FAIR Implementation Profiles.")
    parser.add_argument("inputs", nargs="+",
                        help="FIP files (.trig or .json). At least 2.")
    parser.add_argument("--names", default="",
                        help="Comma-separated community names matching input order. "
                             "Default: derived from FIP label.")
    parser.add_argument("--no-fetch", action="store_true",
                        help="Skip network fetch for .trig inputs (header only).")
    parser.add_argument("--output", default="fip_comparison_output",
                        help="Output directory (default: fip_comparison_output)")
    parser.add_argument("--debug", action="store_true", help="Verbose parsing output.")
    args = parser.parse_args()

    if len(args.inputs) < 2:
        parser.error("Need at least 2 FIPs to compare.")

    name_overrides = [n.strip() for n in args.names.split(",")] if args.names else []

    fips = []
    for i, path in enumerate(args.inputs):
        print(f"\n🔍 Loading FIP {i+1}/{len(args.inputs)}: {path}")
        info, organized = load_fip(path,
                                   fetch_remote=(not args.no_fetch),
                                   debug=args.debug)
        if i < len(name_overrides) and name_overrides[i]:
            name = name_overrides[i]
        else:
            name = short_name(info, Path(path).stem)
        fips.append({"name": name, "info": info, "organized": organized, "path": path})

    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)

    print(f"\n📝 Writing reports to {out}/")
    rows = analyze_implications(fips)
    reports = {
        "comparison_table": ("Side-by-side comparison", render_side_by_side(fips)),
        "overlap_analysis": ("Overlap & uniqueness", render_overlap(fips)),
        "coverage_matrix": ("Coverage matrix", render_coverage(fips)),
        "implications":     ("Galaxy implications", render_implications_md(fips, rows)),
    }
    for stem, (title, md_text) in reports.items():
        (out / f"{stem}.md").write_text(md_text)
        html_body = md_lib.markdown(md_text, extensions=["tables"])
        (out / f"{stem}.html").write_text(render_report_page(title, html_body))
        print(f"   ✅ {stem}.md + {stem}.html")
    (out / "index.html").write_text(render_dashboard(fips, rows))
    print(f"   ✅ index.html")

    print(f"\n🎉 Done. Open {out}/index.html in a browser.")


if __name__ == "__main__":
    main()
