// Serving-runtime presentation logic shared by the Runtimes panel and the
// deployment page's runtime card: image, topology, and "serves" summaries plus
// command-line layout.

import type { RuntimeComponent, RuntimeItem } from "./api";

/** Last path segment of an image ref — drops the registry/repo prefix for display. */
export const imageShort = (img?: string): string => (img ? (img.split("/").pop() ?? img) : "—");

/** Join up to two entries, else "first +N". */
const summ = (a?: string[]): string =>
  !a || a.length === 0 ? "—" : a.length <= 2 ? a.join(", ") : `${a[0]} +${a.length - 1}`;

/** What a runtime serves at a glance: architectures if declared, else formats. */
export const serves = (r: RuntimeItem): string =>
  r.architectures?.length ? summ(r.architectures) : summ(r.formats);

/** Everything a runtime declares it can serve, as deduplicated tag labels
 *  (formats, then quantizations, then architectures). */
export const servesTags = (r: RuntimeItem): string[] => [
  ...new Set([...(r.formats ?? []), ...(r.quantizations ?? []), ...(r.architectures ?? [])]),
];

/** Prefill/decode disaggregated = a decoder component is present. */
export const isDisaggregated = (r: RuntimeItem): boolean =>
  !!r.components?.some((c) => c.role === "decoder");
/** Any component runs a multi-node worker group. */
export const isMultiNode = (r: RuntimeItem): boolean => !!r.components?.some((c) => c.multiNode);

/** One-line node topology across components, e.g. "engine multi-node ×2 ·
 *  decoder ×1"; a multi-node group without a declared size omits the count. */
export const topologySummary = (components?: RuntimeComponent[]): string | null =>
  components?.length
    ? components
        .map((c) =>
          c.multiNode ? `${c.role} multi-node${c.nodes ? ` ×${c.nodes}` : ""}` : `${c.role} ×1`,
        )
        .join(" · ")
    : null;

/** Format a resource map as "cpu 4 · memory 32Gi · nvidia.com/gpu 8", or null. */
export const fmtRes = (m?: Record<string, string>): string | null =>
  m && Object.keys(m).length
    ? Object.entries(m)
        .map(([k, v]) => `${k} ${v}`)
        .join(" · ")
    : null;

/** Lay a command out one flag per line: the executable (and short flags) on the
 *  first line, then each `--long-flag` with its values on its own indented line. */
export const argLines = (command?: string[], args?: string[]): string => {
  const lines: string[] = [];
  for (const t of [...(command ?? []), ...(args ?? [])]) {
    if (t.startsWith("--") || lines.length === 0) lines.push(t);
    else lines[lines.length - 1] += ` ${t}`;
  }
  return lines.map((l, i) => (i === 0 ? l : `  ${l}`)).join("\n");
};
