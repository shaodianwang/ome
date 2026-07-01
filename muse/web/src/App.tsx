import { useState } from "react";

import { CloudDot } from "./components";
import {
  Bell,
  BrandMark,
  ChevronDown,
  IconCapacity,
  IconDeploy,
  IconFleet,
  IconModels,
  IconWorkloads,
  Search,
} from "./icons";
import { Capacity } from "./panels/Capacity";
import { Deploy } from "./panels/Deploy";
import { Fleet } from "./panels/Fleet";
import { Models } from "./panels/Models";
import { Workloads } from "./panels/Workloads";
import type { Cloud } from "./types";

export type PanelKey = "fleet" | "workloads" | "models" | "deploy" | "capacity";

const LABELS: Record<PanelKey, string> = {
  fleet: "Fleet",
  workloads: "Workloads",
  models: "Models",
  deploy: "Deploy",
  capacity: "Capacity",
};

const CLOUD_CHIPS: { key: "all" | Cloud; label: string }[] = [
  { key: "all", label: "All" },
  { key: "meta", label: "Meta" },
  { key: "gcp", label: "GCP" },
  { key: "oci", label: "OCI" },
  { key: "cw", label: "CoreWeave" },
];

export default function App() {
  const [panel, setPanel] = useState<PanelKey>("fleet");
  const [cloud, setCloud] = useState<"all" | Cloud>("all");
  const goto = (p: PanelKey) => setPanel(p);

  const navItem = (key: PanelKey, label: string, Icon: typeof IconFleet, badge?: string) => (
    <div className={`nav-item ${panel === key ? "active" : ""}`} onClick={() => setPanel(key)}>
      <Icon className="ico" />
      <span>{label}</span>
      {badge ? <span className="badge">{badge}</span> : null}
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
          {navItem("workloads", "Workloads", IconWorkloads, "128")}
          {navItem("models", "Models", IconModels, "47")}
          <div className="nav-group-label">Provision</div>
          {navItem("deploy", "Deploy", IconDeploy)}
          {navItem("capacity", "Capacity", IconCapacity)}
        </nav>

        <div className="sidebar-foot">
          <div>
            <span className="dot" />
            Control plane healthy
          </div>
          <div style={{ marginTop: 4 }} className="mono">
            ome-manager · v1beta1 · 4 shards
          </div>
        </div>
      </aside>

      <div className="main">
        <header className="topbar">
          <div className="crumbs">
            <b>{LABELS[panel]}</b>
            <span className="sep">/</span>
            <span>prod</span>
          </div>
          <div className="spacer" />

          <div className="select prod" title="Environment">
            <span className="env-dot" />
            <span>prod</span>
            <ChevronDown className="chev" width={12} height={12} />
          </div>

          <div className="chipbar">
            {CLOUD_CHIPS.map((c) => (
              <div
                key={c.key}
                className={`chip ${cloud === c.key ? "active" : ""}`}
                onClick={() => setCloud(c.key)}
              >
                {c.key !== "all" ? <CloudDot cloud={c.key} /> : null}
                {c.label}
              </div>
            ))}
          </div>

          <button className="icon-btn" title="Search (⌘K)">
            <Search width={15} height={15} />
          </button>
          <button className="icon-btn" title="Notifications">
            <Bell width={15} height={15} />
          </button>
          <div className="avatar" title="s.lin@meta.com">
            SL
          </div>
        </header>

        <div className="content">
          {panel === "fleet" && <Fleet goto={goto} />}
          {panel === "workloads" && <Workloads goto={goto} />}
          {panel === "models" && <Models />}
          {panel === "deploy" && <Deploy />}
          {panel === "capacity" && <Capacity />}
        </div>
      </div>
    </div>
  );
}
