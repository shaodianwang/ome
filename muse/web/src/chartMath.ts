/** Index of the sample nearest a target timestamp; ties resolve to the earlier sample. */
export function nearestTimestampIndex(timestamps: number[], target: number): number {
  if (!timestamps.length) return -1;
  let low = 0;
  let high = timestamps.length - 1;
  while (low < high) {
    const mid = Math.floor((low + high) / 2);
    if (timestamps[mid] < target) low = mid + 1;
    else high = mid;
  }
  if (low === 0) return 0;
  return target - timestamps[low - 1] <= timestamps[low] - target ? low - 1 : low;
}

/** Keyboard navigation for a timestamp selection. A missing selection enters
 * from the nearest edge; Escape dismisses without moving focus. */
export function timestampSelectionForKey(
  key: string,
  current: number | null,
  length: number,
): number | null {
  if (length <= 0 || key === "Escape") return null;
  if (key === "Home") return 0;
  if (key === "End") return length - 1;
  if (key === "ArrowLeft") return Math.max(0, (current ?? length) - 1);
  if (key === "ArrowRight") return Math.min(length - 1, (current ?? -1) + 1);
  return current;
}

/** Split a sorted range series wherever an expected scrape step is absent.
 * The cadence comes from the plot-wide timeline, so a sparse series cannot
 * draw through a timestamp present in a neighboring series. */
export function splitTimestampGaps<T extends { at: number }>(
  points: T[],
  timeline: number[] = points.map((point) => point.at),
): T[][] {
  if (points.length < 2) return points.length ? [points] : [];
  const steps = timeline
    .slice(1)
    .map((at, index) => at - timeline[index])
    .filter((step) => step > 0);
  const cadence = Math.min(...steps);
  if (!Number.isFinite(cadence)) return [points];

  const segments: T[][] = [[points[0]]];
  for (let index = 1; index < points.length; index += 1) {
    if (points[index].at - points[index - 1].at > cadence) segments.push([]);
    segments[segments.length - 1].push(points[index]);
  }
  return segments;
}

/** Exact local sample time. */
export function formatChartTimestamp(at: number): string {
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(at * 1_000));
}
