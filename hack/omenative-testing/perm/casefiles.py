"""casefiles.py — the write path the case generators share.

A generator owns exactly the case ids it mints; every other file in the
folders it writes into is hand-written and out of its reach. Nothing here
deletes a file, and a minted id is written over an existing file only when
that file is recognisably the generator's own: same id and same description
on line 1. Otherwise the regeneration refuses before writing anything.

Line 1 of a case is `# <id>: <description><sep><key=value ...>`. The keys a
generator emits for a case are its own; every other token in the key region
(cell=, notimpl=, issue=, inv_xfail=, ...) and the `# reason:` lines directly
after line 1 (the one comment the harness reads, on line 2) form the
hand-maintained layer, which rides through a regeneration unchanged, each
token at the position it holds on disk. The description, the emitted keys
and the body — everything from the first non-reason line on — are
generator-owned: a hand edit to one of them is drift, which `check` reports;
the fix is to edit the generator, or to move the case to an id the generator
does not mint.
"""
import sys
from pathlib import Path


class Case:
    """One minted case: where it lives and what the generator renders for it.

    kv is the ordered list of (key, value) tokens the generator owns; extra is
    the `# reason:` lines it emits after line 1; body is everything else."""

    def __init__(self, folder, cid, desc, kv, body, extra=(), sep="  "):
        self.folder, self.cid, self.desc, self.sep = folder, cid, desc, sep
        self.kv = [(k, str(v)) for k, v in kv]
        self.extra = list(extra)
        self.body = body

    @property
    def rel(self):
        return f"{self.folder}/{self.cid}.yaml"

    @property
    def prefix(self):
        """Line 1 up to the key region: what identifies a file as this case."""
        return f"# {self.cid}: {self.desc}{self.sep}"

    def recognises(self, disk):
        """Whether the file text `disk` is this case (possibly with a hand
        layer, possibly stale): same id and description, then only key=value
        tokens up to the end of line 1."""
        line1 = disk.split("\n", 1)[0]
        return (line1.startswith(self.prefix)
                and all("=" in w for w in line1[len(self.prefix):].split()))

    def text(self, disk=None):
        """The file to write: the render, carrying over the hand layer of
        `disk` (the file's current text) when there is one."""
        keys = [k for k, _ in self.kv]
        tokens = [f"{k}={v}" for k, v in self.kv]
        extra = list(self.extra)
        if disk is not None:
            line1, disk_extra, _ = split(disk)
            tokens = _merge_tokens(tokens, keys, line1[len(self.prefix):].split())
            extra += [line for line in disk_extra if line not in extra]
        return "\n".join([self.prefix + " ".join(tokens)] + extra) + "\n" + self.body


REASON = "# reason:"


def split(text):
    """(line 1, the `# reason:` lines after it, the body) of a case file. Any
    other comment after line 1 is body: bodies may open with their own prose."""
    lines = text.split("\n")
    n = 1
    while n < len(lines) and lines[n].startswith(REASON):
        n += 1
    return lines[0], lines[1:n], "\n".join(lines[n:])


def _merge_tokens(tokens, keys, disk_words):
    """The generator's tokens with every other token of the on-disk key region
    inserted right after the token that precedes it on disk. An on-disk value
    for an emitted key is dropped (the render's value wins); a token whose key
    the generator does not emit is kept, hand-written or not, so a key the
    generator stops emitting is removed by hand."""
    out, out_keys = list(tokens), list(keys)
    anchor = None
    for word in disk_words:
        key = word.partition("=")[0]
        if key in keys:
            anchor = key
            continue
        pos = out_keys.index(anchor) + 1 if anchor is not None else 0
        out.insert(pos, word)
        out_keys.insert(pos, key)
        anchor = key
    return out


def _held_by(disk):
    line1 = disk.split("\n", 1)[0].lstrip("#").strip()
    return line1 if len(line1) <= 90 else line1[:87] + "..."


def plan(cases, root):
    """What a regeneration would do under `root`: ({rel: text} to write —
    minted files that are missing or differ from the regeneration — and the
    reasons it must not run at all)."""
    writes, problems = {}, []
    for c in cases:
        path = Path(root) / c.rel
        disk = path.read_text() if path.exists() else None
        if disk is not None and not c.recognises(disk):
            problems.append(
                f"{path}: holds '{_held_by(disk)}', not this generator's "
                f"'{c.cid}: {c.desc}' — a hand-written case, or a renumbered or "
                f"reworded matrix; give one of the two a different id (or update "
                f"the description on disk when only the wording changed)")
            continue
        text = c.text(disk)
        if text != disk:
            writes[c.rel] = text
    return writes, problems


def unminted(cases, root):
    """{folder: [case ids]} of the files in the generator's folders that it
    does not mint: hand-written cases (or ones it stopped minting), which a
    regeneration leaves alone."""
    minted = {c.rel for c in cases}
    out = {}
    for folder in sorted({c.folder for c in cases}):
        d = Path(root) / folder
        ids = [f.stem for f in sorted(d.glob("*.yaml")) if f"{folder}/{f.name}" not in minted]
        if ids:
            out[folder] = ids
    return out


def check(cases, root, tool):
    """The `--check` mode: the tree under `root` must equal the regeneration
    byte for byte. Prints every problem; returns the exit code."""
    writes, problems = plan(cases, root)
    for rel in sorted(writes):
        path = Path(root) / rel
        problems.append(f"missing on disk (regenerate): {path}" if not path.exists() else
                        f"drifted from the generator (a hand edit to an owned part, or a "
                        f"generator edit without a regen): {path}")
    if problems:
        for p in problems:
            print(p, file=sys.stderr)
        print(f"\n{tool} --check: FAIL — {len(problems)} problem(s); edit the generator "
              f"and run `python3 {tool}`, or move a hand-maintained case to an id it "
              f"does not mint", file=sys.stderr)
        return 1
    print(f"{tool} --check: OK — {len(cases)} generated file(s) byte-stable")
    return 0


def write(cases, root):
    """Regenerate under `root`: write the minted files that changed, delete
    nothing. Exits nonzero, having written nothing, when a minted id is held
    by a file the generator does not recognise. Returns the paths written."""
    writes, problems = plan(cases, root)
    if problems:
        for p in problems:
            print(p, file=sys.stderr)
        sys.exit(f"refusing to regenerate: {len(problems)} minted id(s) held by cases this "
                 f"generator does not own; nothing was written")
    for rel, text in sorted(writes.items()):
        path = Path(root) / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    return sorted(writes)
