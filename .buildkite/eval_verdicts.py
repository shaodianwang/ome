#!/usr/bin/env python3
"""Record one eval round's verdicts from the lanes' verdict files and archives.

Every eval lane job uploads two artifacts under ``<results dir>``.
``<build>-<job>.verdicts.json`` is the harness's own record of the job: one
object per case with its verdict, and the lane the job ran (feature,
topology, scheduler). ``<build>-<job>.tgz`` is an archive of the directory
the harness wrote for that job: ``lane.tsv`` (the lane: feature, topology,
scheduler, force-delete twin or not), one
``<feature>/<topology>/<case>-<epoch>.tsv`` per case the harness finished,
whose second line is the case's result row with its ``verdict`` column, and
the diagnostics. The verdict file is a few kilobytes and the archive is not,
so a job is read from its verdict file when it has one and from its archive
only when it has none (the harness stopped before its summary).

This script reads them and writes ``eval-verdicts.tsv`` with the columns
build, commit, feature, topology, scheduler, case, verdict: one row per
(case, scheduler) the round ran. A lane with neither artifact yields no row,
a case the harness never finished yields no row, and a file, archive or
result that cannot be read is reported and skipped. The targeted eval mode
of ``generate_pipeline.py`` reads this file through the Buildkite API, so the
format is defined here and nowhere else.

Standard library only; Python 3.9.

Usage: eval_verdicts.py [--archives DIR] [--out FILE] [--build N] [--commit SHA]

Exit status: 0 when a file was written, 1 when the build carries no verdict
file and no result archive at all (nothing to record, so the round must not
be marked as recorded), 2 on a usage error.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tarfile
from pathlib import Path
from typing import Callable, Iterable, NamedTuple, Tuple

# What the eval runner uploads per lane job, relative to the repository.
RESULTS_DIR = "hack/omenative-testing/perm/results"
# The record the runner writes into the job's result directory.
LANE_FILE = "lane.tsv"
LANE_COLUMNS = ("feature", "topology", "scheduler", "force_delete")
# The harness's record of one job (batch.py writes it as verdicts.json in the
# run directory; the runner uploads it as <build>-<job>.verdicts.json): a
# JSON object whose "format" names this format, with the lane ("build",
# "job", "step", "lane", "scheduler", "commit", "run", "seconds", "counts")
# and "cases", one object per case: "case", "feature", "topology",
# "scheduler", "verdict", "reason", "exit", "seconds", "fault", "log" and
# "result", the case's result file under the run directory, null when the
# harness never finished the case.
JOB_VERDICTS_FILE = "verdicts.json"
JOB_VERDICTS_SUFFIX = "." + JOB_VERDICTS_FILE
JOB_VERDICTS_FORMAT = "ome-eval-job-verdicts/1"
# The round's verdict file, uploaded by the recording step as an artifact.
VERDICTS_FILE = "eval-verdicts.tsv"
COLUMNS = ("build", "commit", "feature", "topology", "scheduler", "case", "verdict")
# The build meta-data that marks a build as a recorded full round.
ROUND_META_KEY = "ome:eval-round"
ROUND_META_VALUE = "full"
# The one verdict that needs no rerun; every other word is kept as the
# harness wrote it. A lane the harness skipped (SKIP: its apparatus is
# missing on the cluster, the reason in the case's note) recurs every build
# for the same reason, and a parked case (XFAIL, alone or with its reason)
# is debt the ledger already carries, so neither is a failure to rerun nor
# a passing lane to sample; FAIL, INFRA-FAIL, TRIGGER_FAIL, INIT_TIMEOUT and
# any other word are failures.
PASS = "PASS"
SKIP = "SKIP"
XFAIL = "XFAIL"
# A case result file: <case>-<epoch>.tsv, the case id as the harness names it.
RESULT_FILE = re.compile(r"^(?P<case>p[0-9]+)-(?P<stamp>[0-9]+)\.tsv$")
CASE_ID = re.compile(r"^p[0-9]+$")


def is_set_aside(verdict: str) -> bool:
    """Whether a verdict is SKIP or an XFAIL form: neither a failure nor a
    pass for the targeted selection."""
    word = verdict.strip()
    return word == SKIP or word.startswith(XFAIL)


class Verdict(NamedTuple):
    build: str
    commit: str
    feature: str
    topology: str
    scheduler: str
    case: str
    verdict: str


# What one lane job contributes before the build and commit are attached.
Row = Tuple[str, str, str, str, str]  # (feature, topology, scheduler, case, verdict)


def _table(text: str, required: Iterable[str]) -> tuple[dict[str, int], list[list[str]]]:
    """A tab-separated text as ({column: index}, rows); the header must name
    every required column and every row must reach the last of them."""
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        raise ValueError("empty table")
    columns = {name: index for index, name in enumerate(lines[0].split("\t"))}
    missing = [name for name in required if name not in columns]
    if missing:
        raise ValueError("header lacks " + ", ".join(missing))
    width = max(columns[name] for name in required) + 1
    rows = [line.split("\t") for line in lines[1:]]
    for number, row in enumerate(rows, start=2):
        if len(row) < width:
            raise ValueError(f"line {number} has {len(row)} fields, fewer than the {width} the header names")
    return columns, rows


def write_verdicts(rows: Iterable[Verdict]) -> str:
    """The verdict file's text: the header, then one line per row."""
    lines = ["\t".join(COLUMNS)]
    for row in rows:
        fields = [str(field) for field in row]
        if any("\t" in field or "\n" in field for field in fields):
            raise ValueError("a verdict field may not contain a tab or a newline: " + repr(row))
        lines.append("\t".join(fields))
    return "\n".join(lines) + "\n"


def read_verdicts(text: str) -> list[Verdict]:
    """The rows of a verdict file; a header or row that does not fit the
    format raises ValueError."""
    columns, rows = _table(text, COLUMNS)
    return [Verdict(*(row[columns[name]].strip() for name in COLUMNS)) for row in rows]


def _lane(archive: tarfile.TarFile) -> tuple[str, str, str]:
    """(feature, topology, scheduler) from the archive's one lane record."""
    members = [m for m in archive.getmembers() if m.isfile() and Path(m.name).name == LANE_FILE]
    if len(members) != 1:
        raise ValueError(f"{len(members)} {LANE_FILE} members, not one")
    text = archive.extractfile(members[0]).read().decode("utf-8")
    columns, rows = _table(text, LANE_COLUMNS[:3])
    if len(rows) != 1:
        raise ValueError(f"{LANE_FILE} has {len(rows)} rows, not one")
    row = rows[0]
    return tuple(row[columns[name]].strip() for name in LANE_COLUMNS[:3])


def _case_verdict(name: str, text: str) -> tuple[str, int, str]:
    """(case, epoch stamp, verdict) from one case result file."""
    match = RESULT_FILE.match(Path(name).name)
    if match is None:
        raise ValueError("not a case result file name")
    columns, rows = _table(text, ("id", "verdict"))
    if len(rows) != 1:
        raise ValueError(f"{len(rows)} result rows, not one")
    case = rows[0][columns["id"]].strip()
    if case != match.group("case"):
        raise ValueError(f"row names case {case!r}, the file {match.group('case')!r}")
    verdict = rows[0][columns["verdict"]].strip()
    if not verdict:
        raise ValueError("empty verdict")
    return case, int(match.group("stamp")), verdict


def read_archive(path: Path, warn: Callable[[str], None]) -> tuple[tuple[str, str, str], dict[str, str]]:
    """The lane of one archive and {case: verdict} for every result it holds;
    the newest result of a case wins, and a result that cannot be read is
    reported through `warn` and left out."""
    with tarfile.open(path, "r:gz") as archive:
        lane = _lane(archive)
        newest: dict[str, tuple[int, str]] = {}
        for member in archive.getmembers():
            name = Path(member.name).name
            if not member.isfile() or not name.endswith(".tsv") or name == LANE_FILE:
                continue
            try:
                text = archive.extractfile(member).read().decode("utf-8")
                case, stamp, verdict = _case_verdict(member.name, text)
            except (ValueError, UnicodeDecodeError) as error:
                warn(f"{path.name}: {member.name}: {error}")
                continue
            if case not in newest or stamp > newest[case][0]:
                newest[case] = (stamp, verdict)
    return lane, {case: verdict for case, (_, verdict) in newest.items()}


def _job_case(item, lane_scheduler) -> list[Row]:
    """The row of one case object of a verdict file: none when the harness
    never finished the case (result null)."""
    if not isinstance(item, dict):
        raise ValueError("not an object")
    case = item.get("case")
    if not isinstance(case, str) or not CASE_ID.match(case):
        raise ValueError(f"case {case!r} is not a case id")
    if item.get("result") is None:
        return []
    verdict = item.get("verdict")
    if not isinstance(verdict, str) or not verdict.strip():
        raise ValueError(f"{case}: empty verdict")
    feature, topology = item.get("feature"), item.get("topology")
    for name, value in (("feature", feature), ("topology", topology)):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{case}: empty {name}")
    scheduler = item.get("scheduler", lane_scheduler)
    if scheduler is None:
        scheduler = ""
    if not isinstance(scheduler, str):
        raise ValueError(f"{case}: scheduler is not a string")
    return [(feature.strip(), topology.strip(), scheduler.strip(), case, " ".join(verdict.split()))]


def read_job_verdicts(text: str, warn: Callable[[str], None], source: str = JOB_VERDICTS_FILE) -> list[Row]:
    """The rows of one job's verdict file: one per case the harness finished,
    under the lane's scheduler unless the case names its own. A case object
    that does not fit the format is reported through `warn` and left out; a
    document that is not a verdict file raises ValueError."""
    try:
        document = json.loads(text)
    except ValueError as error:
        raise ValueError(f"not JSON: {error}") from None
    if not isinstance(document, dict):
        raise ValueError("not a JSON object")
    if document.get("format") != JOB_VERDICTS_FORMAT:
        raise ValueError(f"format {document.get('format')!r}, not {JOB_VERDICTS_FORMAT!r}")
    cases = document.get("cases")
    if not isinstance(cases, list):
        raise ValueError("cases is not a list")
    rows: list[Row] = []
    for index, item in enumerate(cases):
        try:
            rows.extend(_job_case(item, document.get("scheduler")))
        except ValueError as error:
            warn(f"{source}: cases[{index}]: {error}")
    return rows


def is_job_file(path: Path) -> bool:
    return path.name.endswith(JOB_VERDICTS_SUFFIX)


def job_of(path: Path) -> str:
    """The <build>-<job> stem the two artifacts of one lane job share."""
    if is_job_file(path):
        return path.name[: -len(JOB_VERDICTS_SUFFIX)]
    return path.stem


def collect(sources: Iterable[Path], build: str, commit: str, warn: Callable[[str], None]) -> list[Verdict]:
    """Every verdict of the round, one per (case, scheduler), in a stable
    order: from each job's verdict file, and from the archive of a job that
    has no verdict file. A file or archive that cannot be read is reported
    and yields nothing."""
    paths = sorted(Path(source) for source in sources)
    recorded = {job_of(path) for path in paths if is_job_file(path)}
    rows: dict[tuple[str, str], Verdict] = {}
    for path in paths:
        try:
            if is_job_file(path):
                found = read_job_verdicts(path.read_text(encoding="utf-8"), warn, path.name)
            elif job_of(path) in recorded:
                continue
            else:
                (feature, topology, scheduler), verdicts = read_archive(path, warn)
                found = [(feature, topology, scheduler, case, verdict) for case, verdict in verdicts.items()]
        except (OSError, tarfile.TarError, ValueError, UnicodeDecodeError) as error:
            warn(f"{path.name}: skipped: {error}")
            continue
        for feature, topology, scheduler, case, verdict in found:
            key = (case, scheduler)
            if key in rows:
                warn(f"{path.name}: {case} under {scheduler or 'the default scheduler'} was already recorded; keeping the first")
                continue
            rows[key] = Verdict(build, commit, feature, topology, scheduler, case, verdict)
    return sorted(rows.values(), key=lambda row: (row.scheduler, row.feature, row.topology, int(row.case[1:])))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Record one eval round's verdicts from the lanes' verdict files and result archives.")
    parser.add_argument("--archives", default=RESULTS_DIR, metavar="DIR", help=f"where the lanes' verdict files and result archives were downloaded (default {RESULTS_DIR})")
    parser.add_argument("--out", default=VERDICTS_FILE, metavar="FILE", help=f"the verdict file to write (default {VERDICTS_FILE})")
    parser.add_argument("--build", default=os.environ.get("BUILDKITE_BUILD_NUMBER", ""), help="the build number (default BUILDKITE_BUILD_NUMBER)")
    parser.add_argument("--commit", default=os.environ.get("BUILDKITE_COMMIT", ""), help="the commit (default BUILDKITE_COMMIT)")
    args = parser.parse_args(argv)
    if not args.build or not args.commit:
        parser.error("--build and --commit are required when BUILDKITE_BUILD_NUMBER and BUILDKITE_COMMIT are unset")
    directory = Path(args.archives)
    files = sorted(directory.glob("*" + JOB_VERDICTS_SUFFIX))
    archives = sorted(directory.glob("*.tgz"))
    if not files and not archives:
        sys.stderr.write(f"no verdict file or result archive under {args.archives}: nothing to record\n")
        return 1

    def warn(message: str) -> None:
        sys.stderr.write("warning: " + message + "\n")

    rows = collect(files + archives, args.build, args.commit, warn)
    Path(args.out).write_text(write_verdicts(rows))
    recorded = {job_of(path) for path in files}
    fallbacks = sum(1 for path in archives if job_of(path) not in recorded)
    passed = sum(1 for row in rows if row.verdict == PASS)
    aside = sum(1 for row in rows if is_set_aside(row.verdict))
    print(
        f"{args.out}: {len(rows)} verdicts from {len(files)} verdict files and {fallbacks} archives, "
        f"{passed} {PASS}, {aside} skipped or parked, {len(rows) - passed - aside} other"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
