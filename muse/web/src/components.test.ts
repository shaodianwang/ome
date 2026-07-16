import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { DataNotice } from "./components";

describe("DataNotice", () => {
  it("separates title and detail text while retaining alert and retry semantics", () => {
    const markup = renderToStaticMarkup(
      createElement(DataNotice, {
        tone: "danger",
        title: "Some fleet data is stale or unavailable.",
        detail: "Activity stream: reconnecting.",
        action: { label: "Retry", onClick: () => undefined },
      }),
    );

    expect(markup).toContain('role="alert"');
    expect(markup).toContain(
      "<strong>Some fleet data is stale or unavailable.</strong> <span>Activity stream: reconnecting.</span>",
    );
    expect(markup).toContain('<button type="button"');
    expect(markup).toContain(">Retry</button>");
  });
});
