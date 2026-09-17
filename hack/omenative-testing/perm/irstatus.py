"""Per-Instance status rows of an InferenceReplica, in either stored form.

The manager persists the rows as a dense list (``instanceStatuses``) or, when
that is smaller, as ``instanceStatusColumns`` with ``instanceStatusEncoding``
set to ``ColumnarV2``. The harness observes stored objects through kubectl,
so it expands the columns itself. This mirrors the decoder in
pkg/controller/v1beta1/irstatus; the golden file under testdata/ is written
by goldengen from that Go codec and keeps the two in lockstep.
"""

import re

MARKER = "instanceStatusEncoding"
COLUMNS = "instanceStatusColumns"
ROWS = "instanceStatuses"
COLUMNAR_V2 = "ColumnarV2"

_INDEX_SET = re.compile(
    r"^(0|[1-9][0-9]*)(-(0|[1-9][0-9]*))?(,(0|[1-9][0-9]*)(-(0|[1-9][0-9]*))?)*$")
_MAX_INDEX = 2**31 - 1
_ENTRY_RECORDS = ("conditions", "readySince", "operation", "lastFailure")


class DecodeError(ValueError):
    """The stored per-Instance representation is malformed."""


def instance_statuses(status):
    """Dense rows of an InferenceReplica status, whichever representation it
    carries. None when it carries neither, which is a build without
    per-Instance status. Raises DecodeError for a malformed representation."""
    status = status or {}
    marker = status.get(MARKER)
    columns = status.get(COLUMNS)
    rows = status.get(ROWS)
    if marker is None:
        if columns is not None:
            raise DecodeError(f"{COLUMNS} present without {MARKER}")
        return rows
    if marker != COLUMNAR_V2:
        raise DecodeError(f"unknown {MARKER} {marker!r}")
    if rows:
        raise DecodeError(f"{COLUMNAR_V2} status also carries {ROWS}")
    if not isinstance(columns, dict):
        raise DecodeError(f"{COLUMNAR_V2} status without {COLUMNS}")
    return decode_columns(columns)


def parse_index_set(raw):
    """Canonical index set ("0-8,10,12-14") to the ascending list of indices."""
    if not isinstance(raw, str) or not _INDEX_SET.match(raw):
        raise DecodeError(f"index set {raw!r} is not canonical")
    indices = []
    previous_last = None
    for term in raw.split(","):
        first, dash, last = term.partition("-")
        first = int(first)
        last = int(last) if dash else first
        if last > _MAX_INDEX:
            raise DecodeError(f"index set {raw!r} exceeds the int32 range")
        if dash and first >= last:
            raise DecodeError(f"index set {raw!r} has a range that is not ascending")
        if previous_last is not None:
            if first <= previous_last:
                raise DecodeError(f"index set {raw!r} is not ascending and disjoint")
            if first == previous_last + 1:
                raise DecodeError(f"index set {raw!r} has adjacent terms")
        indices.extend(range(first, last + 1))
        previous_last = last
    return indices


def decode_columns(columns):
    """Expand a ColumnarV2 payload into dense rows in the stored row order."""
    members = parse_index_set(columns.get("members"))
    member_set = set(members)
    rows = {index: {"index": index} for index in members}

    positions = members
    order = columns.get("rowOrder")
    if order is not None:
        if (not isinstance(order, list) or len(order) != len(members)
                or len(set(order)) != len(order) or set(order) != member_set):
            raise DecodeError("rowOrder is not a permutation of members")
        if order == members:
            raise DecodeError("rowOrder spells the ascending order, which must be absent")
        positions = order

    def assign_groups(name, key, valid, required=False):
        groups = columns.get(name)
        if groups is None:
            if required:
                raise DecodeError(f"{name} is required")
            return
        if not isinstance(groups, list) or not groups:
            raise DecodeError(f"{name} must be a nonempty list")
        covered = set()
        previous = None
        for group in groups:
            value = group.get("value")
            if not valid(value):
                raise DecodeError(f"{name} value {value!r} is outside its domain")
            if previous is not None and not previous < value:
                raise DecodeError(f"{name} groups are not in canonical order")
            previous = value
            for index in parse_index_set(group.get("indexes")):
                if index not in member_set:
                    raise DecodeError(f"{name} names non-member {index}")
                if index in covered:
                    raise DecodeError(f"{name} groups overlap at {index}")
                covered.add(index)
                rows[index][key] = value
        if required and covered != member_set:
            raise DecodeError(f"{name} does not cover every member")

    def nonempty_string(value):
        return isinstance(value, str) and value != ""

    def nonzero_int(value):
        return isinstance(value, int) and not isinstance(value, bool) and value != 0

    def positive_int(value):
        return isinstance(value, int) and not isinstance(value, bool) and value > 0

    assign_groups("phases", "phase", nonempty_string, required=True)
    assign_groups("runningRevisions", "runningRevision", nonempty_string)
    assign_groups("targetRevisions", "targetRevision", nonempty_string)
    assign_groups("incarnations", "incarnation", nonzero_int)
    assign_groups("podCounts", "podCount", positive_int)
    assign_groups("servingPodCounts", "servingPodCount", positive_int)
    assign_groups("availablePodCounts", "availablePodCount", positive_int)

    for name, key, value in (("admitted", "admitted", True), ("activeOrdinalOne", "activeOrdinal", 1)):
        raw = columns.get(name)
        if raw is None:
            continue
        for index in parse_index_set(raw):
            if index not in member_set:
                raise DecodeError(f"{name} names non-member {index}")
            rows[index][key] = value

    entries = columns.get("entries")
    if entries is not None:
        if not isinstance(entries, list) or not entries:
            raise DecodeError("entries must be a nonempty list")
        previous = None
        for entry in entries:
            index = entry.get("index")
            if not isinstance(index, int) or isinstance(index, bool) or index not in member_set:
                raise DecodeError(f"entry index {index!r} is not a member")
            if previous is not None and index <= previous:
                raise DecodeError("entries are not in ascending index order")
            previous = index
            records = {key: entry[key] for key in _ENTRY_RECORDS if entry.get(key)}
            if not records:
                raise DecodeError(f"entry {index} carries no record")
            rows[index].update(records)

    return [rows[index] for index in positions]
