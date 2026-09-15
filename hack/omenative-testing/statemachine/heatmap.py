#!/usr/bin/env python3
"""heatmap.py --grid <machine>.grid.json --out <machine>.html

Renders the expanded (state x event x window) grid of one rollout state
machine as a single self-contained HTML page: one table per window, states
as rows, event[variant] as columns, colored by outcome and marked by status.
Input is the JSON envelope written by `rolloutspec-grid --format json`.
"""
import argparse
import html
import json
from collections import Counter
from pathlib import Path

WINDOWS = ("before", "boundary", "after")

OUTCOME_CLASS = {
    "transition": "o-transition",
    "ignore": "o-ignore",
    "defer": "o-defer",
    "reject": "o-reject",
    "escalate": "o-escalate",
    "n/a": "o-na",
    "DECIDE": "o-decide",
}

STATUS_CLASS = {"review": "s-review", "current": "s-current", "decided": "s-decided"}

# STATE_COL_PX and TRANSITION_COL_PX pin the two sticky row-label columns to
# a fixed width so the transition column's sticky offset can equal the state
# column's width exactly; overflow:hidden + text-overflow:ellipsis keeps a
# long state id from blowing out the layout instead of wrapping under it.
STATE_COL_PX = 200
TRANSITION_COL_PX = 90

CSS = f"""
body{{font:13px/1.4 -apple-system,Segoe UI,Helvetica,Arial,sans-serif;margin:16px}}
table{{border-collapse:collapse;margin:8px 0 24px}}
th,td{{border:1px solid #ccc;padding:2px 6px;white-space:nowrap;font-size:12px}}
th{{position:sticky;top:0;background:#fafafa;z-index:1}}
th.rot{{writing-mode:vertical-rl;transform:rotate(180deg);height:150px;text-align:left}}
th.state,td.state{{font-weight:600;background:#fafafa;position:sticky;left:0;z-index:2;
  width:{STATE_COL_PX}px;max-width:{STATE_COL_PX}px;overflow:hidden;text-overflow:ellipsis}}
th.transition,td.transition{{background:#fff;position:sticky;left:{STATE_COL_PX}px;z-index:2;
  width:{TRANSITION_COL_PX}px;max-width:{TRANSITION_COL_PX}px;overflow:hidden;text-overflow:ellipsis}}
th.state,th.transition{{z-index:3}}
tr.state-start td{{border-top:2px solid #666}}
.o-transition{{background:#c8e6c9}}.o-ignore{{background:#e0e0e0}}.o-defer{{background:#ffe0b2}}
.o-reject{{background:#e1bee7}}.o-escalate{{background:#ffcdd2}}.o-na{{background:#fff;color:#bbb}}
.o-decide{{background:#ff0;font-weight:700}}.o-missing{{background:#000;color:#fff}}
.s-review{{background-image:repeating-linear-gradient(45deg,transparent 0 4px,rgba(0,0,0,.14) 4px 8px)}}
.s-current{{outline:1px dashed #666;outline-offset:-2px}}.s-decided{{outline:2px solid #222;outline-offset:-2px}}
.legend span{{display:inline-block;padding:2px 8px;margin-right:6px;border:1px solid #999}}
.counts{{color:#444}}
"""


class GridCoverageError(RuntimeError):
    """Raised when one or more grid entries fall outside every (state,
    column, window) coordinate the renderer iterates, so they were silently
    dropped instead of appearing in any table."""


def load_envelope(path):
    return json.loads(Path(path).read_text())


def columns(events):
    """One column per (event, variant); a variant-less event is one column."""
    cols = []
    for e in events:
        for v in e.get("variants") or [""]:
            label = e["id"] if v == "" else f'{e["id"]}[{v}]'
            cols.append((e["id"], v, label))
    return cols


def rows_for_window(env, window):
    """One row per state. In boundary/after, a state with more than one
    outgoing transition gets one sub-row per transition id."""
    rows = []
    for s in env["states"]:
        sid = s["id"]
        if window == "before":
            rows.append((sid, ""))
            continue
        tids = sorted({g["transition"] for g in env["grid"] if g["state"] == sid and g["window"] == window})
        if len(tids) <= 1:
            rows.append((sid, tids[0] if tids else ""))
        else:
            rows.extend((sid, t) for t in tids)
    return rows


def index_grid(grid):
    return {(g["state"], g["event"], g["variant"], g["window"], g["transition"]): g for g in grid}


def window_counts(grid, window):
    c = Counter()
    for g in grid:
        if g["window"] != window:
            continue
        if g["outcome"] == "n/a":
            c["n/a"] += 1
        elif g["outcome"] == "DECIDE":
            c["DECIDE"] += 1
        else:
            c[g["status"]] += 1
    return c


def cell_title(g):
    parts = [f"id: {g['id']}"]
    for key in ("to", "transition", "note", "reason", "guard"):
        if g.get(key):
            parts.append(f"{key}: {g[key]}")
    parts.append("placement: " + (g.get("placement") or "—"))
    if g.get("history"):
        parts.append("history: " + ", ".join(g["history"]))
    if g.get("invariants"):
        parts.append("invariants: " + ", ".join(g["invariants"]))
    return "\n".join(parts)


def render_cell(g):
    if g is None:
        return '<td class="o-missing" title="no cell in grid">?</td>'
    classes = [OUTCOME_CLASS.get(g["outcome"], "o-missing")]
    if g["outcome"] not in ("n/a",):
        classes.append(STATUS_CLASS.get(g["status"], ""))
    text = "·" if g["outcome"] == "n/a" else g["outcome"]
    return f'<td class="{" ".join(c for c in classes if c)}" title="{html.escape(cell_title(g))}">{html.escape(text)}</td>'


def render_table(env, window, cols, idx):
    """Renders one window's table and returns (html, keys) where keys is the
    set of grid coordinates actually looked up, for coverage checking."""
    out = ["<table>", '<tr><th class="state">state</th><th class="transition">transition</th>']
    out += [f'<th class="rot">{html.escape(label)}</th>' for _, _, label in cols]
    out.append("</tr>")
    seen = set()
    prev_sid = None
    for sid, tid in rows_for_window(env, window):
        row_class = ' class="state-start"' if sid != prev_sid else ""
        prev_sid = sid
        out.append(f'<tr{row_class}><td class="state" title="{html.escape(sid)}">{html.escape(sid)}</td>'
                    f'<td class="transition">{html.escape(tid)}</td>')
        for ev, var, _ in cols:
            key = (sid, ev, var, window, tid)
            g = idx.get(key)
            if g is not None:
                seen.add(key)
            out.append(render_cell(g))
        out.append("</tr>")
    out.append("</table>")
    return "\n".join(out), seen


def render_counts(c):
    parts = [f"{k}: {c[k]}" for k in sorted(c)]
    parts.append(f"total: {sum(c.values())}")
    return '<p class="counts">' + " · ".join(parts) + "</p>"


def render_legend():
    items = "".join(f'<span class="{cls}">{html.escape(o)}</span>' for o, cls in OUTCOME_CLASS.items())
    status = ('<span class="s-review">review (hatched)</span>'
              '<span class="s-current">current (dashed)</span>'
              '<span class="s-decided">decided (solid)</span>')
    return f'<p class="legend">{items}</p><p class="legend">{status}</p>'


def render(env):
    cols = columns(env["events"])
    idx = index_grid(env["grid"])
    frozen = "true" if env.get("frozen") else "false"
    parts = ["<!doctype html><html><head><meta charset=\"utf-8\">",
             f"<title>{html.escape(env['machine'])} rollout state machine</title>",
             f"<style>{CSS}</style></head><body>",
             f"<h1>{html.escape(env['machine'])}</h1>",
             f"<p>{html.escape(env.get('scope', ''))} · frozen: {frozen}</p>",
             render_legend()]
    rendered = set()
    for w in WINDOWS:
        table_html, keys = render_table(env, w, cols, idx)
        rendered |= keys
        parts.append(f"<h2>window: {w}</h2>")
        parts.append(render_counts(window_counts(env["grid"], w)))
        parts.append(table_html)
    parts.append("</body></html>")
    missing = len(env["grid"]) - len(rendered)
    if missing:
        raise GridCoverageError(
            f"{missing} of {len(env['grid'])} grid entries fall outside states x columns x "
            "windows and were never rendered (check for a stray state, event, or window value)")
    return "\n".join(parts)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--grid", required=True, help="grid JSON from rolloutspec-grid --format json")
    ap.add_argument("--out", required=True, help="HTML output path")
    args = ap.parse_args(argv)
    env = load_envelope(args.grid)
    try:
        page = render(env)
    except GridCoverageError as e:
        raise SystemExit(f"heatmap: {e}")
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(page)
    print(f"wrote {args.out}: {len(env['grid'])} grid cells, {len(env['states'])} states, {len(columns(env['events']))} columns")


if __name__ == "__main__":
    main()
