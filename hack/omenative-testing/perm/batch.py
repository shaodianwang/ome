#!/usr/bin/env python3
"""batch.py [--conc N] [--pod-budget N] [--junit PATH] [cases...] — run cases with bounded concurrency.

Defaults: cases = p01..p25 (the single-Component lifecycle set), conc = 8.
Each case runs as its own `python3 run.py <case>` subprocess (own watcher),
logs to results/logs/<case>.log, and writes its own results/<case>-<ts>.tsv.
After all finish, prints a summary table from the latest TSV per case.

Examples:
  python3 batch.py                       # p01..p25 @ conc 8
  python3 batch.py --conc 12             # p01..p25 @ conc 12
  python3 batch.py --conc 6 p26 p27 p28  # explicit cases
  python3 batch.py p26 p50               # subset @ default conc
  python3 batch.py --shard 0/4           # CI: this shard's quarter of the set
  python3 batch.py --junit /tmp/r.xml    # CI: machine-readable per-case results
  python3 batch.py --pod-budget 64       # cap concurrent declared peak pods

CI contract: exits nonzero when any case ends in a verdict that is neither PASS
nor an XFAIL (parked debt). --shard partitions the case list round-robin so N
agents cover it disjointly; --junit writes one testcase per case.
"""
import concurrent.futures as cf
import glob
import json
import os
import re
import shutil
import subprocess
import sys
import time
import pathlib
import xml.etree.ElementTree as ET
from pathlib import Path

import runtime_fixtures
# The runner owns how an outage marks the webhook configurations it cuts
# and how they are put back; the sweep reads both from it.
from run import WEBHOOK_OUTAGE_ANNO, restore_webhook_configurations

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


def case_header_fields(case_id):
    """Line-1 k=v header fields of a case (parsed like run.py.parse_header,
    values kept as strings — enough to classify fault= kinds)."""
    matches = sorted((HERE / "isvcs").glob(f"**/{case_id}.yaml"))
    if not matches:
        return {}
    first = matches[0].read_text().splitlines()[0].lstrip("#").strip()
    fields = {}
    for tok in first.split():
        if "=" in tok and not tok.startswith("("):
            k, _, v = tok.partition("=")
            fields[k] = v
    return fields


def case_peak_pods(case_id):
    """Maximum serving pods declared by the case header and ISVC worker shape.

    Returns None when a multi-node shape cannot be resolved, which callers
    treat as requiring isolated execution.
    """
    matches = sorted((HERE / "isvcs").glob(f"**/{case_id}.yaml"))
    if len(matches) != 1:
        return None
    fields = case_header_fields(case_id)
    topology = fields.get("topo", "")
    documents = re.split(r"(?m)^---\s*$", matches[0].read_text())
    isvc = next((doc for doc in documents
                 if re.search(r"(?m)^kind:\s*InferenceService\s*$", doc)), "")
    if not isvc:
        return None

    total = 0
    for component, suffix in (("engine", "e"), ("decoder", "d"), ("router", "r")):
        replicas = int(fields.get(f"ceil_{suffix}", fields.get(f"N{suffix}", "0")))
        if replicas == 0:
            continue
        block_match = re.search(
            rf"(?ms)^  {component}:\s*\n(.*?)(?=^  [A-Za-z][A-Za-z0-9_-]*:\s*(?:\n|$)|\Z)",
            isvc,
        )
        block = block_match.group(1) if block_match else ""
        inline = re.search(r"(?m)^    worker:\s*\{[^}]*\bsize:\s*(\d+)", block)
        nested = re.search(r"(?m)^    worker:\s*\n(?:^      .*\n)*?^      size:\s*(\d+)", block)
        worker_size = int((inline or nested).group(1)) if (inline or nested) else 0
        if topology.endswith("-mn") and component != "router" and worker_size == 0:
            return None
        total += replicas * (1 + worker_size)
    return total


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


def run_concurrent_cases(cases, concurrency, pod_budget, case_peaks, record, runner=run_one):
    """Run a case pool while respecting worker and declared-pod limits."""
    pending = [(case_id, case_peaks.get(case_id) or 0) for case_id in cases]
    active = {}
    active_pods = 0

    with cf.ThreadPoolExecutor(max_workers=concurrency) as executor:
        while pending or active:
            while pending and len(active) < concurrency:
                selected = next((idx for idx, (_, peak) in enumerate(pending)
                                 if pod_budget is None or active_pods + peak <= pod_budget), None)
                if selected is None:
                    break
                case_id, peak = pending.pop(selected)
                future = executor.submit(runner, case_id)
                active[future] = peak
                active_pods += peak

            if not active:
                raise RuntimeError("no case fits the configured pod budget")

            completed, _ = cf.wait(active, return_when=cf.FIRST_COMPLETED)
            for future in completed:
                active_pods -= active.pop(future)
                record(*future.result())


def print_failure_logs(case_ids, heading="FAILED CASE LOGS"):
    """Include failed subprocess output in the CI log when artifacts are unavailable."""
    print("\n" + "=" * 90)
    print(heading)
    print("=" * 90)
    for case_id in case_ids:
        feature, topo = case_category(case_id)
        log_path = HERE / "results" / RUN_TS / feature / topo / f"{case_id}.log"
        print(f"\n--- {case_id}: {log_path.relative_to(HERE)} ---")
        try:
            content = log_path.read_text(errors="replace")
        except OSError as exc:
            print(f"(unable to read case log: {exc.strerror or 'unknown error'})")
            continue
        print(content, end="" if content.endswith("\n") else "\n")


def parked_debt_rows(cases):
    """(case, kind, ref, desc) for every notimpl=1 / infeasible=1 case in the
    sweep — the XFAIL debt ledger. notimpl refs the linked GitHub issue (the
    header's issue=, enforced by run.py --validate-only); infeasible refs the
    case's line-2 '# reason:' comment."""
    rows = []
    for c in cases:
        matches = sorted((HERE / "isvcs").glob(f"**/{c}.yaml"))
        if not matches:
            continue
        lines = matches[0].read_text().splitlines()
        fields = case_header_fields(c)
        if fields.get("notimpl") == "1":
            kind = "notimpl"
            ref = f"#{fields['issue']}" if fields.get("issue") else "NO ISSUE"
        elif fields.get("expect") == "parked":
            # The case asserts a documented behaviour the product is meant
            # to outgrow; it names the issue that tracks the change.
            kind = "parked"
            ref = f"#{fields['issue']}" if fields.get("issue") else "NO ISSUE"
        elif fields.get("infeasible") == "1":
            kind = "infeasible"
            m = re.match(r"^\s*#\s*reason:\s*(.+)", lines[1]) if len(lines) > 1 else None
            ref = m.group(1).strip() if m else "(no reason comment)"
        else:
            continue
        # One-line description = the header minus its trailing k=v block.
        toks = lines[0].lstrip("#").strip().split()
        while toks and "=" in toks[-1] and not toks[-1].startswith("("):
            toks.pop()
        rows.append((c, kind, ref, " ".join(toks)))
    return rows


def latest_tsv(case_id):
    # This run's TSVs live under results/<RUN_TS>/<feature>/<topo>/.
    files = glob.glob(str(HERE / "results" / RUN_TS / "**" / f"{case_id}-*.tsv"), recursive=True)
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
    # INFRA-FAIL = the fault apparatus broke (quota/cordon/manager-pod plumbing),
    # not the product — counted apart from FAIL so it reads as a rerun, not a bug.
    ninfra = sum(1 for r in rows if r[1] == "INFRA-FAIL")
    emit("-" * 90)
    emit(f"PASS {npass}/{len(rows)}" + (f"   INFRA-FAIL {ninfra} (fault apparatus, not product)" if ninfra else ""))
    fails = [r for r in rows if r[1] != "PASS"]
    if fails:
        emit("\nNON-PASS:")
        for r in fails:
            emit(f"  {r[0]}  {r[1]}  {r[6]}")
    # Parked-debt ledger: printed even when every such case XFAIL-passes — an
    # XFAIL is tracked debt, not a green light, and a silent XFAIL is how a
    # parked bug ships unnoticed.
    debt = parked_debt_rows(cases)
    if debt:
        verdicts = {r[0]: r[1] for r in rows}
        emit("\nPARKED DEBT (%d case(s) marked notimpl/infeasible in this sweep):" % len(debt))
        emit("{:<5} {:<11} {:<13} {}".format("case", "kind", "verdict", "issue/reason — description"))
        for case, kind, ref, desc in debt:
            emit("{:<5} {:<11} {:<13} {} — {}".format(case, kind, verdicts.get(case, "?"), ref, desc))
    sumdir = HERE / "results" / RUN_TS
    sumdir.mkdir(parents=True, exist_ok=True)
    (sumdir / "summary.txt").write_text("\n".join(out_lines) + "\n")
    print(f"\nsummary written: {sumdir / 'summary.txt'}")


def expand_cases(tokens):
    """Normalize case tokens. Handles ranges ('p01-p50', '26-50') and a single
    space-joined argument (zsh does not word-split unquoted $VAR, so a caller's
    `$CASES` can arrive as one string) — split it back out. Range ids with no
    case file are SKIPPED (with a note) instead of fanning out as NO_RESULT
    noise; an explicitly-named case is kept so run.py errors loudly on it."""
    out, skipped = [], []
    for tok in tokens:
        for part in str(tok).split():
            m = re.fullmatch(r"p?(\d+)-p?(\d+)", part)
            if m:
                a, b = int(m.group(1)), int(m.group(2))
                for n in range(a, b + 1):
                    cid = f"p{n:02d}"
                    if list((HERE / "isvcs").glob(f"**/{cid}.yaml")):
                        out.append(cid)
                    else:
                        skipped.append(cid)
            elif part:
                out.append(part if part.startswith("p") else f"p{int(part):02d}")
    if skipped:
        print(f"    (range expansion skipped {len(skipped)} id(s) with no case file: {', '.join(skipped)})")
    return out


def is_green(verdict):
    """A verdict CI should accept: a pass, or parked debt.

    XFAIL covers both the infeasible-shape and known-bug forms ("XFAIL (known
    bug: #N)"), which the summary already surfaces in its PARKED DEBT table.
    Everything else — FAIL, INIT_TIMEOUT, APPLY_FAIL, INFRA-FAIL, or a verdict
    that could not be read back ("?") — means the sweep did not validate the
    case and is reported as a failure.
    """
    v = (verdict or "").strip()
    return v == "PASS" or v.startswith("XFAIL") or v == "SKIP"


def is_skipped(verdict):
    """A verdict recorded as skipped in the report: parked debt (XFAIL), or a
    case whose fault apparatus this cluster lacks (SKIP, with the reason in
    the case's note)."""
    v = str(verdict or "").strip()
    return v.startswith("XFAIL") or v == "SKIP"


def apply_shard(cases, spec):
    """Return this shard's slice of cases. spec is "I/N", 0-based I.

    Round-robin by index rather than contiguous blocks, so the long-running
    families (gangs, migration) spread across shards instead of landing in one.
    """
    try:
        idx_s, total_s = spec.split("/", 1)
        idx, total = int(idx_s), int(total_s)
    except ValueError:
        raise SystemExit(f"--shard expects I/N (0-based I), got {spec!r}")
    if total < 1 or not (0 <= idx < total):
        raise SystemExit(f"--shard I/N needs N>=1 and 0<=I<N, got {spec!r}")
    picked = [c for n, c in enumerate(cases) if n % total == idx]
    print(f"=== shard {idx}/{total}: {len(picked)} of {len(cases)} cases ===")
    return picked


def write_junit(path, results, elapsed):
    """Emit a JUnit report so CI annotates per case instead of only the summary.

    Parked debt (XFAIL) is recorded as skipped, not passed: it is tracked debt
    and should read that way in the CI UI. So is a SKIP, a case whose fault
    apparatus this cluster lacks.
    """
    suite = ET.Element("testsuite", name="omenative-eval",
                       tests=str(len(results)),
                       failures=str(sum(1 for v in results.values() if not is_green(v))),
                       skipped=str(sum(1 for v in results.values() if is_skipped(v))),
                       time=f"{elapsed:.1f}")
    for case in sorted(results):
        verdict = results[case]
        tc = ET.SubElement(suite, "testcase", classname="omenative-eval",
                           name=f"{case} ({case_category(case)})")
        if is_skipped(verdict):
            ET.SubElement(tc, "skipped", message=str(verdict))
        elif not is_green(verdict):
            ET.SubElement(tc, "failure", message=str(verdict),
                          type=str(verdict).split()[0] if verdict else "UNKNOWN")
    out = pathlib.Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    ET.ElementTree(suite).write(out, encoding="utf-8", xml_declaration=True)


def cluster_wide_fault(fields):
    """True when a case's apparatus reaches past its own pods, so it runs
    serially after the concurrent pool: a namespace-wide quota, a cordoned or
    dead node, the manager restarted, every node tainted, every node's
    spare CPU taken by fillers (a preemption, as a fault or as the kill, or
    a packed pool), or the admission webhook made unreachable (every ISVC
    write in the cluster is refused while it holds). A competing writer
    patches the case's own objects and stays in the pool."""
    fault = str(fields.get("fault", "")).split(":", 1)[0]
    return (fault in ("quota", "cordon", "ctrlrestart", "node", "taint", "preempt", "webhook")
            or fields.get("via") == "preempt" or fields.get("pool") == "packed")


def sweep_fault_artifacts():
    """Remove only resources marked as harness-owned fault apparatus."""
    subprocess.run(["kubectl", "-n", NS, "delete", "resourcequota",
                    "-l", "perm.test/fault", "--ignore-not-found"], env=ENV)
    sweep_preempt_apparatus()
    sweep_pending_taints()
    sweep_webhook_outage()
    r = subprocess.run(["kubectl", "get", "nodes", "-l", "perm.test/cordoned", "-o", "json"],
                       env=ENV, capture_output=True, text=True)
    if r.returncode != 0:
        return
    try:
        marked = json.loads(r.stdout).get("items", [])
    except Exception:
        marked = []
    for item in marked:
        node = (item.get("metadata") or {}).get("name", "")
        if not node:
            continue
        if (item.get("spec") or {}).get("unschedulable"):
            print(f"=== sweeping leftover harness cordon: uncordon {node} ===")
            subprocess.run(["kubectl", "uncordon", node], env=ENV)
        subprocess.run(["kubectl", "label", "node", node, "perm.test/cordoned-"], env=ENV)
    sweep_stopped_kubelets()


def sweep_preempt_apparatus():
    """Delete the filler pods and PriorityClasses a preempt case left behind
    (a case that died before its cleanup); both carry harness labels."""
    subprocess.run(["kubectl", "-n", NS, "delete", "pod", "-l", "perm.test/filler", "--ignore-not-found",
                    "--wait=false"], env=ENV)
    subprocess.run(["kubectl", "delete", "priorityclass", "-l", "perm.test/fault", "--ignore-not-found"], env=ENV)


def sweep_webhook_outage():
    """Point the webhooks back at the manager's Service when a webhook case
    died before its cleanup: the outage annotation on each cut configuration
    holds the service reference every webhook had, and the runner restores
    from it."""
    done = restore_webhook_configurations()
    if done:
        print(f"=== sweeping leftover webhook outage ({WEBHOOK_OUTAGE_ANNO} on the webhook configurations): {done} ===")


def sweep_pending_taints():
    """Remove the harness NoSchedule taint from every node still carrying it."""
    r = subprocess.run(["kubectl", "get", "nodes", "-o", "json"], env=ENV, capture_output=True, text=True)
    if r.returncode != 0:
        return
    try:
        items = json.loads(r.stdout).get("items", [])
    except Exception:
        items = []
    for item in items:
        node = (item.get("metadata") or {}).get("name", "")
        taints = (item.get("spec") or {}).get("taints") or []
        if node and any(t.get("key") == "perm.test/pending" for t in taints):
            print(f"=== sweeping leftover harness taint: untaint {node} ===")
            subprocess.run(["kubectl", "taint", "nodes", node, "perm.test/pending:NoSchedule-"], env=ENV)


def sweep_stopped_kubelets(docker=None):
    """Start the kubelet again on every node the harness marked as stopped
    (a node fault whose case died before its cleanup), through the docker
    that owns the KIND nodes; without that docker the marker is left for the
    host that has it."""
    docker = docker or os.environ.get("OME_TEST_DOCKER", "docker")
    r = subprocess.run(["kubectl", "get", "nodes", "-l", "perm.test/kubelet-stopped", "-o", "json"],
                       env=ENV, capture_output=True, text=True)
    if r.returncode != 0:
        return
    try:
        marked = json.loads(r.stdout).get("items", [])
    except Exception:
        marked = []
    if marked and shutil.which(docker) is None:
        print(f"=== {len(marked)} node(s) marked {'perm.test/kubelet-stopped'} but {docker} is not on PATH; left for the host that owns them ===")
        return
    for item in marked:
        node = (item.get("metadata") or {}).get("name", "")
        if not node:
            continue
        print(f"=== sweeping leftover harness kubelet stop: start kubelet on {node} ===")
        d = subprocess.run([docker, "exec", node, "systemctl", "start", "kubelet"], capture_output=True, text=True)
        if d.returncode == 0:
            subprocess.run(["kubectl", "label", "node", node, "perm.test/kubelet-stopped-"], env=ENV)


def main():
    argv = sys.argv[1:]
    conc = 8
    shard = None
    junit_path = None
    pod_budget = None
    cases = []
    i = 0
    while i < len(argv):
        if argv[i] == "--conc":
            conc = int(argv[i + 1]); i += 2
        elif argv[i] == "--shard":
            shard = argv[i + 1]; i += 2
        elif argv[i] == "--junit":
            junit_path = argv[i + 1]; i += 2
        elif argv[i] in ("--pod-budget", "--exclusive-pods"):
            pod_budget = int(argv[i + 1]); i += 2
        else:
            cases.append(argv[i]); i += 1
    cases = expand_cases(cases)
    if not cases:
        cases = [f"p{n:02d}" for n in range(1, 26)]   # p01..p25

    if shard is not None:
        cases = apply_shard(cases, shard)
        if not cases:
            print(f"=== shard {shard} has no cases assigned; nothing to run ===")
            return 0

    def needs_exclusive_run(case_id):
        if cluster_wide_fault(case_header_fields(case_id)):
            return True
        if pod_budget is None:
            return False
        peak = case_peaks[case_id]
        return peak is None or peak >= pod_budget

    case_peaks = {case_id: case_peak_pods(case_id) for case_id in cases}
    exclusive_cases = [c for c in cases if needs_exclusive_run(c)]
    conc_cases = [c for c in cases if c not in exclusive_cases]

    budget_text = f", pod budget {pod_budget}" if pod_budget is not None else ""
    print(f"=== batch: {len(cases)} cases @ concurrency {conc}{budget_text} ===")
    print(f"    cases: {cases[0]}..{cases[-1]}  ({', '.join(cases)})")
    if exclusive_cases:
        print(f"    partition: {len(conc_cases)} concurrent, then {len(exclusive_cases)} "
              f"cluster-wide or high-pod case(s) SERIAL after the pool drains "
              f"({', '.join(exclusive_cases)})")
    # Apply runtime once up front so the N subprocesses don't race on it.
    # The fixtures are templates — the PVC and the ClusterBaseModel's storageUri
    # have to name NS, or the ISVC validator rejects every case.
    print("=== applying runtime (once) ===")
    subprocess.run(["kubectl", "apply", "-n", NS, "-f", "-"], env=ENV,
                   input=runtime_fixtures.render(kind=runtime_fixtures.kind_from_env()),
                   text=True)
    print()

    t0 = time.time()
    results = {}
    streamed_failures = set()
    done = 0

    def record(case_id, rc, secs):
        nonlocal done
        done += 1
        v = latest_tsv(case_id)
        verdict = "?"
        if v:
            with open(v) as fh:
                ls = fh.read().splitlines()
            if len(ls) >= 2:
                cols = ls[1].split("\t")
                verdict = cols[16] if len(cols) > 16 else "?"
        if rc != 0 and is_green(verdict):
            verdict = f"PROCESS_EXIT_{rc}"
        print(f"  [{done}/{len(cases)}] {case_id} done in {secs:.0f}s  rc={rc}  verdict={verdict}")
        results[case_id] = verdict
        if not is_green(verdict):
            print_failure_logs([case_id], heading=f"FAILED CASE LOG: {case_id}")
            streamed_failures.add(case_id)

    if conc_cases:
        run_concurrent_cases(conc_cases, conc, pod_budget, case_peaks, record)
    for c in exclusive_cases:
        record(*run_one(c))
        sweep_fault_artifacts()

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

    sweep_fault_artifacts()

    if junit_path:
        write_junit(junit_path, results, time.time() - t0)
        print(f"=== junit: {junit_path} ===")

    failed = sorted(c for c, v in results.items() if not is_green(v))
    if failed:
        print(f"\n=== FAILED {len(failed)}/{len(results)}: "
              f"{', '.join(f'{c}={results[c]}' for c in failed)} ===")
        unstreamed = [case_id for case_id in failed if case_id not in streamed_failures]
        if unstreamed:
            print_failure_logs(unstreamed)
        return 1
    print(f"\n=== all {len(results)} case(s) green ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
