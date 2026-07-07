// A tiny History-API router (no dependency). The URL is the source of truth for
// which panel is shown and which item's detail is open, so browser back/forward,
// refresh, and deep-links all work. Paths are relative to the app's base (Vite
// BASE_URL), so it works behind the gateway sub-path; the server serves
// index.html for these paths (SPA fallback), so refresh/deep-link resolve.
import { useEffect, useState } from "react";

import type { DepFilter, PanelKey } from "./App";

export interface Route {
  panel: PanelKey;
  /** Open item's detail on the current panel (a deployment / model / runtime). */
  sel?: { cluster: string; name: string };
  /** Deployments list tab. */
  filter?: DepFilter;
}

const PANEL_TO_SLUG: Record<PanelKey, string> = {
  fleet: "",
  workloads: "deployments",
  models: "models",
  runtimes: "runtimes",
  capacity: "capacity",
  deploy: "deploy",
};
const SLUG_TO_PANEL: Record<string, PanelKey> = {
  "": "fleet",
  fleet: "fleet",
  deployments: "workloads",
  models: "models",
  runtimes: "runtimes",
  capacity: "capacity",
  deploy: "deploy",
};

const base = import.meta.env.BASE_URL;

/** Parse the current location into a Route. */
function parse(): Route {
  let path = window.location.pathname;
  if (path.startsWith(base)) path = path.slice(base.length);
  const seg = path.split("/").filter(Boolean);
  const panel = SLUG_TO_PANEL[seg[0] ?? ""] ?? "fleet";
  const route: Route = { panel };
  if (seg[1] && seg[2]) {
    route.sel = { cluster: decodeURIComponent(seg[1]), name: decodeURIComponent(seg[2]) };
  }
  if (panel === "workloads") {
    const tab = new URLSearchParams(window.location.search).get("tab");
    if (tab === "attention" || tab === "ready") route.filter = tab;
  }
  return route;
}

/** Build the URL (base-relative) for a Route. */
export function routePath(r: Route): string {
  let path = base + PANEL_TO_SLUG[r.panel];
  if (r.sel) {
    path += `/${encodeURIComponent(r.sel.cluster)}/${encodeURIComponent(r.sel.name)}`;
  }
  const q = r.filter && r.filter !== "all" ? `?tab=${r.filter}` : "";
  return path + q;
}

/** The current route plus a navigate() that pushes it to browser history. */
export function useRoute(): [Route, (r: Route) => void] {
  const [route, setRoute] = useState<Route>(parse);
  useEffect(() => {
    const onPop = () => setRoute(parse());
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);
  const navigate = (r: Route) => {
    if (routePath(r) !== window.location.pathname + window.location.search) {
      window.history.pushState(null, "", routePath(r));
    }
    setRoute(r);
  };
  return [route, navigate];
}
