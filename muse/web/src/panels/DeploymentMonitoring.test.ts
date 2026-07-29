import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import type { IsvcMetrics } from "../metrics";
import { Monitoring } from "./DeploymentMonitoring";

const metrics: IsvcMetrics = {
  present: true,
  identityScoped: true,
  evaluatedAt: 300,
  rangeSeconds: 200,
  by: {
    request_rate: {
      last: 3,
      series: [
        { at: 100, value: 1 },
        { at: 200, value: 2 },
        { at: 300, value: 3 },
      ],
    },
  },
};

const baseProps = {
  m: metrics,
  replicas: { configured: false, rows: [] },
  metricsOn: true,
  loading: false,
  error: null,
  queryCount: 1,
  failedQueryCount: 0,
  selectedTimeRangeId: "",
  onTimeRangeChange: () => {},
};

describe("Deployment Monitoring time range", () => {
  it("renders only configured choices and selects the configured default", () => {
    const markup = renderToStaticMarkup(
      createElement(Monitoring, {
        ...baseProps,
        timeRanges: {
          default: "full",
          options: [
            { id: "recent", label: "Recent", seconds: 100, stepSeconds: 10 },
            { id: "full", label: "Full history", seconds: 200, stepSeconds: 20 },
          ],
        },
      }),
    );

    expect(markup).toContain('aria-label="Range"');
    expect(markup).toContain('<option value="recent">Recent</option>');
    expect(markup).toContain('<option value="full" selected="">Full history</option>');
    expect(markup).not.toContain("15m");
  });

  it("omits the selector when no range configuration is supplied", () => {
    const markup = renderToStaticMarkup(createElement(Monitoring, baseProps));
    expect(markup).not.toContain('aria-label="Range"');
  });

  it("applies the configured default window", () => {
    const markup = renderToStaticMarkup(
      createElement(Monitoring, {
        ...baseProps,
        m: { ...metrics, evaluatedAt: 1_000 },
        timeRanges: {
          default: "recent",
          options: [
            { id: "recent", label: "Recent", seconds: 100, stepSeconds: 10 },
            { id: "full", label: "Full history", seconds: 1_000, stepSeconds: 20 },
          ],
        },
      }),
    );

    expect(markup).toContain("No serving traffic in the selected window.");
  });
});
