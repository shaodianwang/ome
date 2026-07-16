// A tiny History-API router (no dependency). The URL is the source of truth for
// which panel is shown and which item's detail is open, so browser back/forward,
// refresh, and deep-links all work. Paths are relative to the app's base (Vite
// BASE_URL), so it works behind the gateway sub-path; the server serves
// index.html for these paths (SPA fallback), so refresh/deep-link resolve.
import { useEffect, useState } from "react";

import type { DepFilter, PanelKey } from "./App";
import { isResourceKind } from "./resource";
import { isLegacySelected } from "./resource";
import type { ResourceKind, RouteSelected } from "./resource";

export interface Route {
  panel: PanelKey;
  /** Open item's detail on the current panel (a deployment / model / runtime). */
  sel?: RouteSelected;
  /** Deployments list tab. */
  filter?: DepFilter;
  /** Deployments list cluster filter (empty/undefined = all clusters). */
  cluster?: string;
  /** Deployments list search query. */
  q?: string;
}

const PANEL_TO_SLUG: Record<PanelKey, string> = {
  fleet: "",
  workloads: "deployments",
  models: "models",
  runtimes: "runtimes",
  capacity: "capacity",
};
const SLUG_TO_PANEL: Record<string, PanelKey> = {
  "": "fleet",
  fleet: "fleet",
  deployments: "workloads",
  models: "models",
  runtimes: "runtimes",
  capacity: "capacity",
};

const base = import.meta.env.BASE_URL;

const DEFAULT_KIND: Partial<Record<PanelKey, ResourceKind>> = {
  workloads: "InferenceService",
};

const PANEL_KINDS: Partial<Record<PanelKey, readonly ResourceKind[]>> = {
  workloads: ["InferenceService"],
  models: ["BaseModel", "ClusterBaseModel"],
  runtimes: ["ServingRuntime", "ClusterServingRuntime"],
};

function resourceKind(panel: PanelKey, requested: string | null): ResourceKind | undefined {
  const allowed = PANEL_KINDS[panel];
  if (requested == null) return DEFAULT_KIND[panel];
  if (isResourceKind(requested) && allowed?.includes(requested)) return requested;
  return undefined;
}

/** Parse a location into a Route. Explicit arguments keep the URL contract testable. */
export function parseRoute(
  pathname = window.location.pathname,
  search = window.location.search,
): Route {
  let path = pathname;
  if (path.startsWith(base)) path = path.slice(base.length);
  const seg = path.split("/").filter(Boolean);
  const panel = SLUG_TO_PANEL[seg[0] ?? ""] ?? "fleet";
  const route: Route = { panel };
  const sp = new URLSearchParams(search);
  const requestedKind = sp.get("kind");
  const kind = resourceKind(panel, requestedKind);
  if (seg[1] && seg[2]) {
    const cluster = decodeURIComponent(seg[1]);
    const name = decodeURIComponent(seg[2]);
    if (kind) {
      route.sel = {
        cluster,
        kind,
        name,
        namespace: sp.get("namespace") || undefined,
      };
    } else if (requestedKind == null && (panel === "models" || panel === "runtimes")) {
      route.sel = { cluster, name, legacy: panel === "models" ? "model" : "runtime" };
    }
  }
  if (panel === "workloads") {
    const tab = sp.get("tab");
    if (tab === "attention" || tab === "ready") route.filter = tab;
    const cluster = sp.get("cluster");
    if (cluster) route.cluster = cluster;
    const q = sp.get("q");
    if (q) route.q = q;
  }
  return route;
}

/** Build the URL (base-relative) for a Route. */
export function routePath(r: Route): string {
  let path = base + PANEL_TO_SLUG[r.panel];
  if (r.sel) {
    path += `/${encodeURIComponent(r.sel.cluster)}/${encodeURIComponent(r.sel.name)}`;
  }
  const sp = new URLSearchParams();
  if (r.sel && !isLegacySelected(r.sel)) {
    sp.set("kind", r.sel.kind);
    if (r.sel.namespace) sp.set("namespace", r.sel.namespace);
  }
  if (r.filter && r.filter !== "all") sp.set("tab", r.filter);
  if (r.cluster) sp.set("cluster", r.cluster);
  if (r.q) sp.set("q", r.q);
  const query = sp.toString();
  return query ? `${path}?${query}` : path;
}

/** The current route plus a navigate(). Navigation (panel / detail) pushes a
 *  history entry; list-filter changes pass `{ replace: true }` so refining a list
 *  updates the URL in place instead of spamming back-history. */
export function useRoute(): [Route, (r: Route, opts?: { replace?: boolean }) => void] {
  const [route, setRoute] = useState<Route>(parseRoute);
  useEffect(() => {
    const onPop = () => setRoute(parseRoute());
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);
  const navigate = (r: Route, opts?: { replace?: boolean }) => {
    const target = routePath(r);
    if (target !== window.location.pathname + window.location.search) {
      if (opts?.replace) window.history.replaceState(null, "", target);
      else window.history.pushState(null, "", target);
    }
    setRoute(r);
  };
  return [route, navigate];
}
