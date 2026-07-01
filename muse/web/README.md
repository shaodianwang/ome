# MUSE console (web)

The MUSE console UI (OEP-0021) — a Vite + React + TypeScript single-page app, the
real-code port of `oeps/0021-muse-console/mockup.html`.

This first cut is a **visual port driven by mock data** (`src/mock.ts`), so it
runs standalone with no backend. The view-model types (`src/types.ts`) already
match the muse read-plane API, so wiring to live data is a follow-up that swaps
`mock.ts` for `fetch` calls — the components don't change.

## Develop

```sh
cd muse/web
npm install
npm run dev          # http://localhost:5173
```

`vite.config.ts` proxies `/v1` and `/healthz` to a locally-running muse-server
(`cargo run -p muse-server`, port 8080), so once the panels are wired they fetch
live data with no CORS setup.

```sh
npm run build        # type-check (tsc -b) + production bundle to dist/
npm run preview      # serve the built bundle
```

## Layout

```
src/
├── main.tsx          entry
├── App.tsx           shell: sidebar nav, topbar, panel switching (React state)
├── styles.css        design system, ported verbatim from the mockup
├── icons.tsx         shared SVG icons
├── components.tsx    Pill / Bar / UtilCell / CloudDot
├── types.ts          view-model types (mirror the muse API shapes)
├── mock.ts           demo data (replaced by live API later)
└── panels/
    ├── Fleet.tsx       clusters table · activity feed · rollouts   → /v1/clusters + /v1/capacity
    ├── Workloads.tsx   InferenceService table + expandable PD/canary detail → /v1/inferenceservices
    ├── Models.tsx      registry cards + onboarding pipeline        → /v1/basemodels + /v1/clusterbasemodels
    ├── Deploy.tsx      create-InferenceService form + YAML preview → (write plane, not built)
    └── Capacity.tsx    RDMA islands · TPU slices · GPU pools        → /v1/capacity + /v1/nodes
```

## Wiring to the live read plane (next)

Each panel reads from `mock.ts`. To go live, add `src/api.ts` that calls the muse
endpoints and maps responses into the `types.ts` shapes, then have panels read
from it (e.g. via `useEffect` + `useState`, or a small data hook). The Deploy
panel needs write endpoints that muse-server doesn't expose yet.
