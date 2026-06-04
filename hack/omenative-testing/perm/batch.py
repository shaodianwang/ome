#!/usr/bin/env python3
"""batch.py [--conc N] [cases...] — run many run.py cases with bounded concurrency.

Defaults: cases = p01..p25 (the single-Component lifecycle set), conc = 8.
Each case runs as its own `python3 run.py <case>` subprocess (own watcher),
logs to results/logs/<case>.log, and writes its own results/<case>-<ts>.tsv.
After all finish, prints a summary table from the latest TSV per case.

Examples:
  python3 batch.py                       # p01..p25 @ conc 8
  python3 batch.py --conc 12             # p01..p25 @ conc 12
  python3 batch.py --conc 6 p26 p27 p28  # explicit cases
  python3 batch.py p26 p50               # subset @ default conc
"""
import concurrent.futures as cf
import glob
import os
import re
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).parent.resolve()
# Cluster + namespace come from the environment so the harness isn't pinned to one
# developer's cluster: OME_TEST_KUBECONFIG (else the ambient KUBECONFIG, else
# ~/.kube/config); OME_TEST_NS (default "sandbox" — where the coordtest fixtures live).
KUBECONFIG = os.environ.get("OME_TEST_KUBECONFIG") or os.environ.get("KUBECONFIG") or os.path.expanduser("~/.kube/config")
NS = os.environ.get("OME_TEST_NS", "sandbox")
# One run stamp shared with every run.py subprocess (via OME_RUN_TS) so a whole
# batch's results land together under results/<RUN_TS>/<feature>/<topo>/.
RUN_TS = os.environ.get("OME_RUN_TS") or time.strftime("%Y%m%d-%H%M%S")
ENV = {**os.environ, "KUBECONFIG": KUBECONFIG, "OME_RUN_TS": RUN_TS}


def case_category(case_id):
    """(feature, topo) for a case — mirrors run.py.case_category so each log
    lands in the same results/<RUN_TS>/<feature>/<topo>/ leaf as its TSV."""
    matches = sorted((HERE / "isvcs").glob(f"**/{case_id}.yaml"))
    if not matches:
        return ("uncategorized", "unknown")
    yf = matches[0]
    rel = yf.relative_to(HERE / "isvcs")
    feature = rel.parts[0] if len(rel.parts) > 1 else "uncategorized"
    topo = "unknown"
    for tok in yf.read_text().splitlines()[0].split():
        if tok.startswith("topo="):
            topo = tok[len("topo="):]
            break
    return (feature, topo)


def run_one(case_id):
    feature, topo = case_category(case_id)
    log_path = HERE / "results" / RUN_TS / feature / topo / f"{case_id}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    with open(log_path, "w") as log:
        proc = subprocess.run(
            ["python3", "-u", str(HERE / "run.py"), case_id],   # -u: unbuffered → live-tailable per-case log
            stdout=log, stderr=subprocess.STDOUT, env=ENV, cwd=str(HERE),
        )
    return case_id, proc.returncode, time.time() - t0


def latest_tsv(case_id):
    # This run's TSVs live under results/<RUN_TS>/<feature>/<topo>/.
    files = glob.glob(str(HERE / "results" / RUN_TS / "**" / f"{case_id}-*.tsv"), recursive=True)
    if not files:  # fallback: latest across any run (e.g. re-summarizing older runs)
        files = glob.glob(str(HERE / "results" / "**" / f"{case_id}-*.tsv"), recursive=True)
    return max(files, key=os.path.getmtime) if files else None


def summarize(cases):
    # TSV columns (run.py): id desc Ne Nd Nr ceil_e ceil_d ceil_r peak_e peak_d peak_r
    #                       final_e final_d final_r conv_secs policy verdict note
    rows = []
    for c in cases:
        f = latest_tsv(c)
        if not f:
            rows.append((c, "NO_RESULT", "-", "-", "-", "-", ""))
            continue
        with open(f) as fh:
            lines = fh.read().splitlines()
        if len(lines) < 2:
            rows.append((c, "NO_RESULT", "-", "-", "-", "-", ""))
            continue
        col = lines[1].split("\t")
        def g(i):
            return col[i] if i < len(col) else "-"
        rows.append((c, g(16), g(8), g(9), g(14), g(15), g(17)))  # verdict peak_e peak_d conv policy note
    out_lines = []
    def emit(s=""):
        print(s)
        out_lines.append(s)
    emit("\n" + "=" * 90)
    emit(f"BATCH SUMMARY  (run {RUN_TS})")
    emit("=" * 90)
    hdr = ("case", "verdict", "peak_e", "peak_d", "conv_s", "policy", "note")
    emit("{:<5} {:<13} {:>6} {:>6} {:>8} {:<12} {}".format(*hdr))
    for r in rows:
        case, verdict, pe, pd, conv, pol, note = r
        try:
            conv = f"{float(conv):.1f}" if conv not in ("-", "-1", "") else conv
        except ValueError:
            pass
        emit("{:<5} {:<13} {:>6} {:>6} {:>8} {:<12} {}".format(case, verdict, pe, pd, conv, pol, note[:60]))
    npass = sum(1 for r in rows if r[1] == "PASS")
    emit("-" * 90)
    emit(f"PASS {npass}/{len(rows)}")
    fails = [r for r in rows if r[1] != "PASS"]
    if fails:
        emit("\nNON-PASS:")
        for r in fails:
            emit(f"  {r[0]}  {r[1]}  {r[6]}")
    sumdir = HERE / "results" / RUN_TS
    sumdir.mkdir(parents=True, exist_ok=True)
    (sumdir / "summary.txt").write_text("\n".join(out_lines) + "\n")
    print(f"\nsummary written: {sumdir / 'summary.txt'}")


def expand_cases(tokens):
    """Normalize case tokens. Handles ranges ('p01-p50', '26-50') and a single
    space-joined argument (zsh does not word-split unquoted $VAR, so a caller's
    `$CASES` can arrive as one string) — split it back out."""
    out = []
    for tok in tokens:
        for part in str(tok).split():
            m = re.fullmatch(r"p?(\d+)-p?(\d+)", part)
            if m:
                a, b = int(m.group(1)), int(m.group(2))
                out.extend(f"p{n:02d}" for n in range(a, b + 1))
            elif part:
                out.append(part if part.startswith("p") else f"p{int(part):02d}")
    return out


def main():
    argv = sys.argv[1:]
    conc = 8
    cases = []
    i = 0
    while i < len(argv):
        if argv[i] == "--conc":
            conc = int(argv[i + 1]); i += 2
        else:
            cases.append(argv[i]); i += 1
    cases = expand_cases(cases)
    if not cases:
        cases = [f"p{n:02d}" for n in range(1, 26)]   # p01..p25

    print(f"=== batch: {len(cases)} cases @ concurrency {conc} ===")
    print(f"    cases: {cases[0]}..{cases[-1]}  ({', '.join(cases)})")
    # Apply runtime once up front so the N subprocesses don't race on it.
    print("=== applying runtime (once) ===")
    subprocess.run(["kubectl", "apply", "-f", str(HERE / "runtime")], env=ENV)
    print()

    t0 = time.time()
    results = {}
    with cf.ThreadPoolExecutor(max_workers=conc) as ex:
        futs = {ex.submit(run_one, c): c for c in cases}
        done = 0
        for fut in cf.as_completed(futs):
            case_id, rc, secs = fut.result()
            done += 1
            v = latest_tsv(case_id)
            verdict = "?"
            if v:
                with open(v) as fh:
                    ls = fh.read().splitlines()
                if len(ls) >= 2:
                    cols = ls[1].split("\t")
                    verdict = cols[16] if len(cols) > 16 else "?"
            print(f"  [{done}/{len(cases)}] {case_id} done in {secs:.0f}s  rc={rc}  verdict={verdict}")
            results[case_id] = verdict

    print(f"\n=== all {len(cases)} cases finished in {time.time() - t0:.0f}s ===")
    summarize(cases)

    # Safety sweep: delete any ISVC that lingered (run.py deletes on its own
    # path, but a killed subprocess could leave one behind).
    leftover = []
    for c in cases:
        r = subprocess.run(["kubectl", "-n", NS, "get", "isvc", c],
                           env=ENV, capture_output=True, text=True)
        if r.returncode == 0:
            leftover.append(c)
    if leftover:
        print(f"\n=== sweeping {len(leftover)} leftover ISVCs: {leftover} ===")
        subprocess.run(["kubectl", "-n", NS, "delete", "isvc", *leftover, "--wait=false"], env=ENV)


if __name__ == "__main__":
    main()
