import { useEffect, useRef, useState } from "react";

import { api } from "./api";
import { clusterLagDetail, summarizeClusterCaches } from "./clusterStatus";
import { DataNotice } from "./components";
import {
  BrandMark,
  IconCapacity,
  IconFleet,
  IconModels,
  IconRuntimes,
  IconSystem,
  IconWorkloads,
  Menu,
} from "./icons";
import { useLive } from "./live";
import { Capacity } from "./panels/Capacity";
import { Fleet } from "./panels/Fleet";
import { isLegacySelected } from "./resource";
import type { Selected } from "./resource";
import { Models } from "./panels/Models";
import { Runtimes } from "./panels/Runtimes";
import { System } from "./panels/System";
import { Workloads } from "./panels/Workloads";
import { useRoute } from "./route";
import type { Route } from "./route";

export type PanelKey = "fleet" | "workloads" | "models" | "runtimes" | "capacity" | "system";
export type DepFilter = "all" | "attention" | "rolling" | "ready";

const LABELS: Record<PanelKey, string> = {
  fleet: "Fleet",
  workloads: "Deployments",
  models: "Models",
  runtimes: "Runtimes",
  capacity: "Capacity",
  system: "System",
};

export default function App() {
  // The URL is the source of truth for navigation, so browser back/forward,
  // refresh, and deep-links all work; each nav pushes a history entry.
  const [route, navigate] = useRoute();
  const panel = route.panel;
  const depFilter: DepFilter = route.filter ?? "all";
  const depCluster = route.cluster;
  const depQ = route.q;
  const sel = route.sel ?? null;
  const canonicalSel = sel && !isLegacySelected(sel) ? sel : null;
  const [navOpen, setNavOpen] = useState(false);
  const activeNavRef = useRef<HTMLButtonElement>(null);
  const navToggleRef = useRef<HTMLButtonElement>(null);
  const mainRef = useRef<HTMLDivElement>(null);
  // A workloads route carrying the current list filters (tab / cluster / search),
  // so navigating into a detail and back restores them.
  const wl = (over: Partial<Route>): Route => ({
    panel: "workloads",
    filter: depFilter,
    cluster: depCluster,
    q: depQ,
    ...over,
  });

  const goto = (p: PanelKey) => {
    setNavOpen(false);
    navigate({ panel: p });
  };
  const openDeployments = (f: DepFilter) => navigate({ panel: "workloads", filter: f });
  const openDeployment = (s: Selected) => navigate({ panel: "workloads", sel: s });
  const openModel = (s: Selected) => navigate({ panel: "models", sel: s });
  const openRuntime = (s: Selected) => navigate({ panel: "runtimes", sel: s });
  const openCapacityCluster = (c: string) => navigate({ panel: "capacity", cluster: c });

  // Sidebar connection status, from the same read plane the panels use.
  const clusters = useLive(api.clusters, {
    kinds: ["Cluster", "InferenceService", "Node"],
    intervalMs: 30_000,
  });
  const items = clusters.data?.items ?? [];
  const cacheSummary = summarizeClusterCaches(items);

  useEffect(() => {
    const main = mainRef.current;
    if (!navOpen) {
      main?.removeAttribute("inert");
      return;
    }
    const navToggle = navToggleRef.current;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") setNavOpen(false);
    };
    const onResize = () => setNavOpen(false);
    main?.setAttribute("inert", "");
    activeNavRef.current?.focus();
    window.addEventListener("keydown", onKeyDown);
    window.addEventListener("resize", onResize);
    return () => {
      main?.removeAttribute("inert");
      window.removeEventListener("keydown", onKeyDown);
      window.removeEventListener("resize", onResize);
      navToggle?.focus();
    };
  }, [navOpen]);

  const navItem = (key: PanelKey, label: string, Icon: typeof IconFleet) => (
    <button
      type="button"
      className={`nav-item ${panel === key ? "active" : ""}`}
      onClick={() => goto(key)}
      aria-current={panel === key ? "page" : undefined}
      aria-label={label}
      title={label}
      ref={panel === key ? activeNavRef : undefined}
    >
      <Icon className="ico" aria-hidden />
      <span>{label}</span>
    </button>
  );

  return (
    <div className={`app${navOpen ? " nav-open" : ""}`}>
      <button
        type="button"
        className="nav-scrim"
        aria-label="Close navigation"
        onClick={() => setNavOpen(false)}
      />
      <aside className="sidebar" id="primary-navigation" aria-label="Primary navigation">
        <div className="brand">
          <div className="logo">
            <BrandMark aria-hidden />
          </div>
          <div className="word">
            MUSE<small>Unified Serving</small>
          </div>
        </div>

        <nav className="nav" aria-label="Primary">
          {navItem("fleet", "Fleet", IconFleet)}
          {navItem("workloads", "Deployments", IconWorkloads)}
          {navItem("models", "Models", IconModels)}
          {navItem("runtimes", "Runtimes", IconRuntimes)}
          {navItem("capacity", "Capacity", IconCapacity)}
          {navItem("system", "System", IconSystem)}
        </nav>

        <div className="sidebar-foot">
          <div>
            <span
              className={
                clusters.error || cacheSummary.unhealthy
                  ? "dot error"
                  : cacheSummary.syncing
                    ? "dot syncing"
                    : "dot"
              }
            />
            {clusters.error
              ? "read plane unavailable"
              : items.length
                ? `${items.length} clusters connected`
                : "connecting…"}
          </div>
          {clusters.error ? (
            <div className="sync-line">{clusters.error}</div>
          ) : items.length ? (
            <div className="sync-line mono">{cacheSummary.line || "all synced"}</div>
          ) : null}
        </div>
      </aside>

      <div className="main" ref={mainRef}>
        <header className="topbar">
          <button
            type="button"
            className="icon-btn nav-toggle"
            aria-label="Open navigation"
            aria-controls="primary-navigation"
            aria-expanded={navOpen}
            onClick={() => setNavOpen((open) => !open)}
            ref={navToggleRef}
          >
            <Menu />
          </button>
          <div className="crumbs">
            <b>{LABELS[panel]}</b>
          </div>
        </header>

        <div className="content">
          {items.length > 0 && items.some((c) => !c.ready) ? (
            <DataNotice
              tone={items.some((c) => c.ready) ? "warning" : "danger"}
              title={
                items.some((c) => c.ready)
                  ? `Partial fleet data — ${items.filter((c) => c.ready).length} of ${items.length} clusters ready`
                  : "No cluster data yet"
              }
              detail={clusterLagDetail(items)}
            />
          ) : null}
          {panel === "fleet" && (
            <Fleet
              openDeployments={openDeployments}
              openDeployment={openDeployment}
              openCapacityCluster={openCapacityCluster}
            />
          )}
          {panel === "workloads" && (
            <Workloads
              filter={depFilter}
              setFilter={(f) => navigate(wl({ filter: f }), { replace: true })}
              cluster={depCluster ?? ""}
              setCluster={(c) => navigate(wl({ cluster: c || undefined }), { replace: true })}
              query={depQ ?? ""}
              setQuery={(query) => navigate(wl({ q: query || undefined }), { replace: true })}
              selected={canonicalSel}
              setSelected={(s) => navigate(wl({ sel: s ?? undefined }))}
              openModel={openModel}
              openRuntime={openRuntime}
            />
          )}
          {panel === "models" && (
            <Models
              selected={sel}
              setSelected={(s) => navigate({ panel: "models", sel: s ?? undefined })}
              openDeployment={openDeployment}
            />
          )}
          {panel === "runtimes" && (
            <Runtimes
              selected={sel}
              setSelected={(s) => navigate({ panel: "runtimes", sel: s ?? undefined })}
              openDeployment={openDeployment}
            />
          )}
          {panel === "capacity" && (
            <Capacity openDeployment={openDeployment} initialCluster={route.cluster} />
          )}
          {panel === "system" && <System />}
        </div>
      </div>
    </div>
  );
}
