import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { nearestTimestampIndex, splitTimestampGaps, timestampSelectionForKey } from "./chartMath";
import { Chart } from "./viz";

describe("nearestTimestampIndex", () => {
  it("selects the closest actual sample and resolves ties to the earlier value", () => {
    const timestamps = [100, 200, 400];
    expect(nearestTimestampIndex(timestamps, 50)).toBe(0);
    expect(nearestTimestampIndex(timestamps, 150)).toBe(0);
    expect(nearestTimestampIndex(timestamps, 260)).toBe(1);
    expect(nearestTimestampIndex(timestamps, 500)).toBe(2);
  });
});

describe("timestampSelectionForKey", () => {
  it("navigates from either edge, clamps, and dismisses with Escape", () => {
    expect(timestampSelectionForKey("ArrowRight", null, 3)).toBe(0);
    expect(timestampSelectionForKey("ArrowLeft", null, 3)).toBe(2);
    expect(timestampSelectionForKey("ArrowLeft", 0, 3)).toBe(0);
    expect(timestampSelectionForKey("ArrowRight", 2, 3)).toBe(2);
    expect(timestampSelectionForKey("Home", 2, 3)).toBe(0);
    expect(timestampSelectionForKey("End", 0, 3)).toBe(2);
    expect(timestampSelectionForKey("Escape", 1, 3)).toBeNull();
  });
});

describe("splitTimestampGaps", () => {
  it("does not connect samples across a missing scrape interval", () => {
    const points = [
      { at: 0, value: 1 },
      { at: 10, value: 2 },
      { at: 30, value: 3 },
      { at: 40, value: 4 },
    ];
    expect(splitTimestampGaps(points)).toEqual([points.slice(0, 2), points.slice(2)]);
    expect(splitTimestampGaps([points[0], points[2]], [0, 10, 20])).toEqual([
      [points[0]],
      [points[2]],
    ]);
  });
});

describe("Chart", () => {
  it("uses timestamp spacing and exposes keyboard inspection semantics", () => {
    const markup = renderToStaticMarkup(
      createElement(Chart, {
        ariaLabel: "Requests",
        series: [
          {
            label: "Requests",
            color: "red",
            formatValue: String,
            points: [
              { at: 0, value: 0 },
              { at: 10, value: 1 },
              { at: 100, value: 2 },
            ],
          },
        ],
      }),
    );

    expect(markup).toContain('role="img"');
    expect(markup).toContain('tabindex="0"');
    expect(markup).toContain("Use arrow keys to inspect values; Escape closes the tooltip.");
    expect(markup).toContain('aria-live="polite"');
    expect(markup).toContain('points="0.0,96.0 30.0,48.0"');
    expect(markup).not.toContain('points="0.0,96.0 30.0,48.0 300.0,0.0"');
  });
});
