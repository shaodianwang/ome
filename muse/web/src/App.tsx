import { api } from "./api";
import {
  BrandMark,
  IconCapacity,
  IconDeploy,
  IconFleet,
  IconModels,
  IconRuntimes,
  IconWorkloads,
} from "./icons";
import { useLive } from "./live";
import { Capacity } from "./panels/Capacity";
import { Deploy } from "./panels/Deploy";
import { Fleet } from "./panels/Fleet";
import type { Selected } from "./types";
import { Models } from "./panels/Models";
import { Runtimes } from "./panels/Runtimes";
import { Workloads } from "./panels/Workloads";
import { useRoute } from "./route";

export type PanelKey = "fleet" | "workloads" | "models" | "runtimes" | "deploy" | "capacity";
export type DepFilter = "all" | "attention" | "ready";

const LABELS: Record<PanelKey, string> = {
  fleet: "Fleet",
  workloads: "Deployments",
  models: "Models",
  runtimes: "Runtimes",
  deploy: "Deploy",
  capacity: "Capacity",
};

export default function App() {
  // The URL is the source of truth for navigation, so browser back/forward,
  // refresh, and deep-links all work; each nav pushes a history entry.
  const [route, navigate] = useRoute();
  const panel = route.panel;
  const depFilter: DepFilter = route.filter ?? "all";
  const sel = route.sel ?? null;

  const goto = (p: PanelKey) => navigate({ panel: p });
  const openDeployments = (f: DepFilter) => navigate({ panel: "workloads", filter: f });
  const openDeployment = (s: Selected) => navigate({ panel: "workloads", sel: s });
  const openModel = (s: Selected) => navigate({ panel: "models", sel: s });
  const openRuntime = (s: Selected) => navigate({ panel: "runtimes", sel: s });

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
          {navItem("runtimes", "Runtimes", IconRuntimes)}
          {navItem("capacity", "Capacity", IconCapacity)}
          <div className="nav-group-label">Provision</div>
          {navItem("deploy", "Deploy", IconDeploy)}
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
              setFilter={(f) => navigate({ panel: "workloads", filter: f })}
              selected={sel}
              setSelected={(s) =>
                navigate({ panel: "workloads", sel: s ?? undefined, filter: depFilter })
              }
              openModel={openModel}
              openRuntime={openRuntime}
            />
          )}
          {panel === "models" && (
            <Models
              goto={goto}
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
          {panel === "deploy" && <Deploy />}
          {panel === "capacity" && <Capacity />}
        </div>
      </div>
    </div>
  );
}
