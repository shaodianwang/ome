import { useState } from "react";

import { api } from "./api";
import { BrandMark, IconCapacity, IconDeploy, IconFleet, IconModels, IconWorkloads } from "./icons";
import { useLive } from "./live";
import { Capacity } from "./panels/Capacity";
import { Deploy } from "./panels/Deploy";
import { Fleet } from "./panels/Fleet";
import type { Selected } from "./panels/DeploymentDetail";
import { Models } from "./panels/Models";
import { Workloads } from "./panels/Workloads";

export type PanelKey = "fleet" | "workloads" | "models" | "deploy" | "capacity";
export type DepFilter = "all" | "attention" | "ready";

const LABELS: Record<PanelKey, string> = {
  fleet: "Fleet",
  workloads: "Deployments",
  models: "Models",
  deploy: "Deploy",
  capacity: "Capacity",
};

export default function App() {
  const [panel, setPanel] = useState<PanelKey>("fleet");
  const [depFilter, setDepFilter] = useState<DepFilter>("all");
  const [selectedDep, setSelectedDep] = useState<Selected | null>(null);
  // Any top-level nav clears an open deployment detail, so returning to
  // Deployments (or leaving and coming back) always lands on the list.
  const goto = (p: PanelKey) => {
    setSelectedDep(null);
    setPanel(p);
  };
  const openDeployments = (f: DepFilter) => {
    setSelectedDep(null);
    setDepFilter(f);
    setPanel("workloads");
  };

  // Sidebar connection status, from the same read plane the panels use.
  const clusters = useLive(api.clusters, { kinds: ["InferenceService"], intervalMs: 30_000 });
  const items = clusters.data?.items ?? [];
  const ready = items.filter((c) => c.ready).length;
  const syncing = items.length - ready;

  const navItem = (key: PanelKey, label: string, Icon: typeof IconFleet) => (
    <div className={`nav-item ${panel === key ? "active" : ""}`} onClick={() => goto(key)}>
      <Icon className="ico" />
      <span>{label}</span>
    </div>
  );

  return (
    <div className="app">
      <aside className="sidebar">
        <div className="brand">
          <div className="logo">
            <BrandMark />
          </div>
          <div className="word">
            MUSE<small>Unified Serving</small>
          </div>
        </div>

        <nav className="nav">
          <div className="nav-group-label">Operate</div>
          {navItem("fleet", "Fleet", IconFleet)}
          {navItem("workloads", "Deployments", IconWorkloads)}
          {navItem("models", "Models", IconModels)}
          <div className="nav-group-label">Provision</div>
          {navItem("deploy", "Deploy", IconDeploy)}
          {navItem("capacity", "Capacity", IconCapacity)}
        </nav>

        <div className="sidebar-foot">
          <div>
            <span
              className="dot"
              style={{ background: syncing ? "var(--amber)" : "var(--green)" }}
            />
            {items.length ? `${items.length} clusters connected` : "connecting…"}
          </div>
          {items.length ? (
            <div style={{ marginTop: 4 }} className="mono">
              {syncing ? `${syncing} syncing · ${ready} ready` : "all synced"}
            </div>
          ) : null}
        </div>
      </aside>

      <div className="main">
        <header className="topbar">
          <div className="crumbs">
            <b>{LABELS[panel]}</b>
          </div>
          <div className="spacer" />
          <div className="avatar" title="Signed in">
            SL
          </div>
        </header>

        <div className="content">
          {panel === "fleet" && <Fleet openDeployments={openDeployments} />}
          {panel === "workloads" && (
            <Workloads
              goto={goto}
              filter={depFilter}
              setFilter={setDepFilter}
              selected={selectedDep}
              setSelected={setSelectedDep}
            />
          )}
          {panel === "models" && <Models goto={goto} />}
          {panel === "deploy" && <Deploy />}
          {panel === "capacity" && <Capacity />}
        </div>
      </div>
    </div>
  );
}
