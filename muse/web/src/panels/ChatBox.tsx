import { useState } from "react";

import { api } from "../api";
import { Copy } from "../icons";
import { copyText } from "../util";

/** How much longer than the server's own chat budget the console waits before
 *  giving up — enough for the server's specific timeout error to arrive first. */
const CLIENT_GRACE_MS = 5000;

/** A ready-to-run curl for the model's OpenAI-style chat endpoint, using whatever
 *  model + prompt the user has entered. The prompt is embedded in the single-quoted
 *  `-d` payload, so any single quotes in it are escaped for the shell. This is what
 *  the user copies to run the request from their terminal with their own creds —
 *  the endpoint sits behind an auth gateway the console can't authenticate to. */
function buildCurl(endpoint: string, model: string, prompt: string): string {
  const body = JSON.stringify({
    model,
    messages: [{ role: "user", content: prompt || "Hello" }],
  });
  // bash-safe single quote: close, escaped quote, reopen.
  const shellSafe = body.replace(/'/g, `'\\''`);
  return [
    `curl -s ${endpoint}/v1/chat/completions \\`,
    `  -H 'content-type: application/json' \\`,
    `  -d '${shellSafe}'`,
  ].join("\n");
}

/** A curl command block with an honest copy button (only flags "Copied" when the
 *  copy actually landed). Shared by the proxy-off state and the failure fallback. */
function CurlBlock({ curl, note }: { curl: string; note?: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <div className="tryit">
      {note ? <div className="muted cell-sm">{note}</div> : null}
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

/** In-page inference test: a live chat when the proxy is enabled, else a copyable
 *  curl against the endpoint. When a live attempt fails — commonly because the
 *  endpoint is behind an auth gateway the console can't authenticate to — it falls
 *  back to the same curl (built from the user's own model + prompt) to run locally. */
export function ChatBox({
  cluster,
  name,
  endpoint,
  defaultModel,
  enabled,
  timeoutSecs,
}: {
  cluster: string;
  name: string;
  endpoint?: string;
  defaultModel: string;
  enabled: boolean;
  timeoutSecs?: number;
}) {
  const [model, setModel] = useState(defaultModel);
  const [prompt, setPrompt] = useState("");
  const [reply, setReply] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  if (!endpoint) {
    return <div className="muted">No endpoint yet — this deployment isn't addressable.</div>;
  }

  if (!enabled) {
    return (
      <CurlBlock
        curl={buildCurl(endpoint, defaultModel, "Hello")}
        note="Live testing is off on this console — run it yourself:"
      />
    );
  }

  // Bound the request a little above the server's own budget, so the server's
  // (more specific) error wins in normal cases and this abort is only a backstop.
  const timeoutMs = timeoutSecs != null ? timeoutSecs * 1000 + CLIENT_GRACE_MS : undefined;

  const send = () => {
    if (!prompt.trim() || busy) return;
    setBusy(true);
    setErr(null);
    setReply(null);
    api
      .chat(cluster, name, { model, messages: [{ role: "user", content: prompt }] }, timeoutMs)
      .then((r) => setReply(r.choices?.[0]?.message?.content ?? JSON.stringify(r)))
      .catch((e: unknown) => setErr(e instanceof Error ? e.message : String(e)))
      .finally(() => setBusy(false));
  };

  return (
    <div className="tryit">
      <div className="chat-model">
        <label>model</label>
        <input value={model} onChange={(e) => setModel(e.target.value)} />
      </div>
      <textarea
        className="chat-input"
        placeholder="Send a test prompt…"
        value={prompt}
        onChange={(e) => setPrompt(e.target.value)}
      />
      <button className="btn primary sm" onClick={send} disabled={busy || !prompt.trim()}>
        {busy ? "Sending…" : "Send"}
      </button>
      {reply ? <div className="chat-reply">{reply}</div> : null}
      {err ? (
        <>
          <div className="notice bad">{err}</div>
          <CurlBlock
            curl={buildCurl(endpoint, model, prompt)}
            note="Couldn't reach the endpoint from the console — it may require your credentials. Run it from your terminal instead:"
          />
        </>
      ) : null}
    </div>
  );
}
