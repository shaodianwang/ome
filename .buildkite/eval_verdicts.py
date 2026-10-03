#!/usr/bin/env python3
"""Record one eval round's verdicts from the lanes' result archives.

Every eval lane job uploads ``<results dir>/<build>-<job>.tgz``, an archive
of the directory the harness wrote for that job: ``lane.tsv`` (the lane the
job ran: feature, topology, scheduler, force-delete twin or not), and one
``<feature>/<topology>/<case>-<epoch>.tsv`` per case the harness finished,
whose second line is the case's result row with its ``verdict`` column.

This script reads every archive of the build and writes ``eval-verdicts.tsv``
with the columns build, commit, feature, topology, scheduler, case, verdict:
one row per (case, scheduler) the round ran. A lane whose archive is missing
yields no row, a case the harness never finished yields no row, and an
archive or result file that cannot be read is reported and skipped. The
targeted eval mode of ``generate_pipeline.py`` reads this file through the
Buildkite API, so the format is defined here and nowhere else.

Standard library only; Python 3.9.

Usage: eval_verdicts.py [--archives DIR] [--out FILE] [--build N] [--commit SHA]

Exit status: 0 when a file was written, 1 when the build carries no result
archive at all (nothing to record, so the round must not be marked as
recorded), 2 on a usage error.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import tarfile
from pathlib import Path
from typing import Callable, Iterable, NamedTuple

# What the eval runner uploads per lane job, relative to the repository.
RESULTS_DIR = "hack/omenative-testing/perm/results"
# The record the runner writes into the job's result directory.
LANE_FILE = "lane.tsv"
LANE_COLUMNS = ("feature", "topology", "scheduler", "force_delete")
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


def collect(archives: Iterable[Path], build: str, commit: str, warn: Callable[[str], None]) -> list[Verdict]:
    """Every verdict of the round, one per (case, scheduler), in a stable
    order; an archive that cannot be read is reported and yields nothing."""
    rows: dict[tuple[str, str], Verdict] = {}
    for path in sorted(archives):
        try:
            (feature, topology, scheduler), verdicts = read_archive(path, warn)
        except (OSError, tarfile.TarError, ValueError, UnicodeDecodeError) as error:
            warn(f"{path.name}: skipped: {error}")
            continue
        for case, verdict in verdicts.items():
            key = (case, scheduler)
            if key in rows:
                warn(f"{path.name}: {case} under {scheduler or 'the default scheduler'} was already recorded; keeping the first")
                continue
            rows[key] = Verdict(build, commit, feature, topology, scheduler, case, verdict)
    return sorted(rows.values(), key=lambda row: (row.scheduler, row.feature, row.topology, int(row.case[1:])))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Record one eval round's verdicts from the lanes' result archives.")
    parser.add_argument("--archives", default=RESULTS_DIR, metavar="DIR", help=f"where the lane archives were downloaded (default {RESULTS_DIR})")
    parser.add_argument("--out", default=VERDICTS_FILE, metavar="FILE", help=f"the verdict file to write (default {VERDICTS_FILE})")
    parser.add_argument("--build", default=os.environ.get("BUILDKITE_BUILD_NUMBER", ""), help="the build number (default BUILDKITE_BUILD_NUMBER)")
    parser.add_argument("--commit", default=os.environ.get("BUILDKITE_COMMIT", ""), help="the commit (default BUILDKITE_COMMIT)")
    args = parser.parse_args(argv)
    if not args.build or not args.commit:
        parser.error("--build and --commit are required when BUILDKITE_BUILD_NUMBER and BUILDKITE_COMMIT are unset")
    archives = sorted(Path(args.archives).glob("*.tgz"))
    if not archives:
        sys.stderr.write(f"no result archive under {args.archives}: nothing to record\n")
        return 1

    def warn(message: str) -> None:
        sys.stderr.write("warning: " + message + "\n")

    rows = collect(archives, args.build, args.commit, warn)
    Path(args.out).write_text(write_verdicts(rows))
    passed = sum(1 for row in rows if row.verdict == PASS)
    aside = sum(1 for row in rows if is_set_aside(row.verdict))
    print(
        f"{args.out}: {len(rows)} verdicts from {len(archives)} archives, "
        f"{passed} {PASS}, {aside} skipped or parked, {len(rows) - passed - aside} other"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
