import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// Node's `process` at config-eval time, without pulling in all of @types/node.
declare const process: { env: Record<string, string | undefined> };

// `base` is the sub-path the console is served under. In the packaged image it
// defaults to /muse/ (a gateway routes /muse to muse-server and strips the
// prefix); override with MUSE_WEB_BASE at build time. Dev always uses "/" so the
// proxy below works.
//
// During `npm run dev`, proxy the read-plane API to a locally-running muse-server
// (cargo run -p muse-server) so the UI can fetch live data without CORS.
export default defineConfig(({ command }) => ({
  base: process.env.MUSE_WEB_BASE ?? (command === "build" ? "/muse/" : "/"),
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/v1": "http://localhost:8080",
      "/healthz": "http://localhost:8080",
    },
  },
}));
