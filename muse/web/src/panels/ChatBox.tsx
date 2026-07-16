import { useState } from "react";

import { api } from "../api";
import { Copy } from "../icons";
import { copyText } from "../util";

/** The endpoints a served model may expose. Engines differ: sglang serves only
 *  `/generate` (and ignores `model`); vLLM serves `/v1/chat/completions`
 *  (which requires `model`) and `/generate`. The panel defaults to chat but lets
 *  the user switch — behavior/topology varies per deployment, so it stays open. */
const ENDPOINTS = [
  { path: "/v1/chat/completions", label: "Chat completions · vLLM" },
  { path: "/generate", label: "Generate · sglang" },
] as const;

/** A starter request body for an endpoint. Chat carries `model` + `messages`
 *  (vLLM requires the model); `/generate` uses sglang's `text` + sampling params. */
function defaultBody(path: string, model: string): string {
  if (path === "/generate") {
    return JSON.stringify({ text: "Hello", sampling_params: { max_new_tokens: 128 } }, null, 2);
  }
  return JSON.stringify({ model, messages: [{ role: "user", content: "Hello" }] }, null, 2);
}

/** A ready-to-run curl for the chosen URL/endpoint/body. The served endpoint sits
 *  behind an mTLS gateway the console can't authenticate to as the user, so this
 *  is the reliable path: the caller runs it with their own client cert. Cert paths
 *  are placeholders — the user fills in their CA + x509. Single quotes in the body
 *  are escaped for the shell. */
function buildCurl(base: string, path: string, body: string): string {
  const url = `${base.replace(/\/+$/, "")}${path}`;
  const compact = body.replace(/\s+/g, " ").trim().replace(/'/g, `'\\''`);
  return [
    `curl -sS '${url}' \\`,
    `  --noproxy '*' \\`,
    `  --cacert <ca.pem> --cert <your-client-cert.pem> \\`,
    `  -H 'content-type: application/json' \\`,
    `  -d '${compact}'`,
  ].join("\n");
}

/** In-page inference test. The endpoint, target URL, and request body are all
 *  editable — engines and gateways vary, and `status.url` is often wrong today,
 *  so the panel is deliberately flexible. It always offers a correct curl (run
 *  with your own cert); when the proxy is enabled it also sends the chosen
 *  endpoint live through muse, falling back to the curl on any failure. */
export function ChatBox({
  cluster,
  name,
  endpoint,
  defaultModel,
  enabled,
  timeoutSecs,
  clientGraceSecs,
}: {
  cluster: string;
  name: string;
  endpoint?: string;
  defaultModel: string;
  enabled: boolean;
  timeoutSecs?: number;
  clientGraceSecs?: number;
}) {
  const [path, setPath] = useState<string>(ENDPOINTS[0].path);
  const [baseUrl, setBaseUrl] = useState(endpoint ?? "");
  const [body, setBody] = useState(() => defaultBody(ENDPOINTS[0].path, defaultModel));
  const [reply, setReply] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [copied, setCopied] = useState(false);

  const onEndpoint = (p: string) => {
    setPath(p);
    setBody(defaultBody(p, defaultModel));
    setReply(null);
    setErr(null);
  };

  const curl = buildCurl(baseUrl, path, body);
  // The live proxy forwards the chosen endpoint through muse (using the pod's
  // identity + the server-known URL); the curl is the fallback with your own cert.
  const canLiveSend = enabled;

  const send = () => {
    if (busy) return;
    let parsed: unknown;
    try {
      parsed = JSON.parse(body);
    } catch {
      setErr("Request body is not valid JSON.");
      return;
    }
    setBusy(true);
    setErr(null);
    setReply(null);
    const timeoutMs =
      timeoutSecs != null && clientGraceSecs != null
        ? (timeoutSecs + clientGraceSecs) * 1000
        : undefined;
    api
      .chat(cluster, name, path, baseUrl, parsed, timeoutMs)
      .then((r) => setReply(r.choices?.[0]?.message?.content ?? JSON.stringify(r, null, 2)))
      .catch((e: unknown) => setErr(e instanceof Error ? e.message : String(e)))
      .finally(() => setBusy(false));
  };

  return (
    <div className="tryit">
      <div className="chat-controls">
        <label className="chat-field">
          <span>Endpoint</span>
          <select value={path} onChange={(e) => onEndpoint(e.target.value)}>
            {ENDPOINTS.map((e) => (
              <option key={e.path} value={e.path}>
                {e.label}
              </option>
            ))}
          </select>
        </label>
        <label className="chat-field grow">
          <span>Target URL</span>
          <input
            className="mono"
            value={baseUrl}
            onChange={(e) => setBaseUrl(e.target.value)}
            placeholder="https://…"
          />
        </label>
      </div>
      {!endpoint ? (
        <p className="note">
          No endpoint on status yet — enter the gateway URL to build a request.
        </p>
      ) : null}

      <textarea
        className="chat-input mono"
        value={body}
        onChange={(e) => setBody(e.target.value)}
        spellCheck={false}
        rows={6}
      />

      {canLiveSend ? (
        <button className="btn primary sm" onClick={send} disabled={busy}>
          {busy ? "Sending…" : "Send"}
        </button>
      ) : null}
      {reply ? <div className="chat-reply">{reply}</div> : null}
      {err ? <div className="notice bad">{err}</div> : null}

      <p className="note">Behind an mTLS gateway — run it with your own client cert + CA:</p>
      <pre className="curl">{curl}</pre>
      <button
        className="btn ghost sm"
        onClick={() =>
          void copyText(curl).then((ok) => {
            if (!ok) return;
            setCopied(true);
            setTimeout(() => setCopied(false), 1200);
          })
        }
      >
        <Copy />
        {copied ? "Copied" : "Copy curl"}
      </button>
    </div>
  );
}
