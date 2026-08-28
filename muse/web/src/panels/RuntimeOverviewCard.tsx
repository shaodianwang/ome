import { Fragment } from "react";

import { api, isNotFoundError } from "../api";
import type { RuntimeDetail } from "../api";
import { CommandBlock, DataNotice, EnvList, Pill } from "../components";
import { Copyable, Facts } from "../drawer";
import { useLive } from "../live";
import type { Selected } from "../resource";
import { imageShort, isDisaggregated, servesTags, topologySummary } from "../runtime";
import { Card } from "../viz";

/** Inline runtime summary for the deployment overview: identity, image, node
 *  topology, what it serves, and each serving container's command and
 *  environment. Fetch failures degrade to a notice inside the card so the rest
 *  of the overview always renders. */
export function RuntimeOverviewCard({
  sel,
  source,
  openRuntime,
}: {
  sel: Selected;
  source?: "spec" | "pods";
  openRuntime: (sel: Selected) => void;
}) {
  // A definitive not-found resolves to `null`, so the card can tell "this
  // runtime does not exist" apart from a transient fetch failure (which
  // surfaces via `error` and must not assert nonexistence).
  const detail = useLive<RuntimeDetail | null>(
    () =>
      api.runtime(sel).catch((e: unknown) => {
        if (isNotFoundError(e)) return null;
        throw e;
      }),
    { kinds: ["ServingRuntime", "ClusterServingRuntime"], throttleMs: 5_000 },
  );
  const d = detail.data;
  const open = (
    <span className="right">
      <button className="linkbtn" onClick={() => openRuntime(sel)}>
        Open →
      </button>
    </span>
  );
  const sub = [sel.kind, sel.namespace].filter(Boolean).join(" · ");
  if (!d) {
    return (
      <Card title="Runtime" sub={sub} actions={open}>
        {detail.loading ? (
          <div className="muted">Loading runtime…</div>
        ) : detail.error ? (
          <div className="muted" title={detail.error}>
            Runtime details unavailable right now.
          </div>
        ) : (
          <DataNotice
            tone="warning"
            title="Runtime not found"
            detail={`${sel.name} is referenced but does not exist on ${sel.cluster}.`}
          />
        )}
      </Card>
    );
  }
  const tags = servesTags(d);
  const topology = topologySummary(d.components);
  return (
    <Card title="Runtime" sub={sub} actions={open}>
      <Facts
        items={[
          [
            "Name",
            <>
              <span className="mono">{d.name}</span>
              {source === "pods" ? (
                <>
                  {" "}
                  <Pill
                    tone="slate"
                    dot={false}
                    title="spec.runtime was omitted; resolved from the serving-runtime label on running pods"
                    label="auto-selected"
                  />
                </>
              ) : null}
            </>,
          ],
          ["Image", d.image ? <Copyable text={d.image} display={imageShort(d.image)} /> : null],
          [
            "Topology",
            topology ? (
              <>
                {topology}
                {isDisaggregated(d) ? (
                  <>
                    {" "}
                    <Pill tone="violet" dot={false} label="PD" />
                  </>
                ) : null}
              </>
            ) : null,
          ],
          [
            "Serves",
            tags.length ? (
              <span className="rt-serves">
                {tags.map((t) => (
                  <span key={t} className="tag">
                    {t}
                  </span>
                ))}
              </span>
            ) : null,
          ],
          [
            "Protocols",
            d.protocols?.length ? (
              <span className="rt-serves">
                {d.protocols.map((p) => (
                  <span key={p} className="tag">
                    {p}
                  </span>
                ))}
              </span>
            ) : null,
          ],
        ]}
      />
      {(d.containers ?? []).map((c) => (
        <Fragment key={c.role}>
          <div className="rt-role">{c.role}</div>
          {c.command?.length || c.args?.length ? (
            <CommandBlock
              label={`Command & args (${(c.command?.length ?? 0) + (c.args?.length ?? 0)})`}
              command={c.command}
              args={c.args}
            />
          ) : null}
          {c.env?.length ? (
            <details className="collapse">
              <summary>Env ({c.env.length})</summary>
              <EnvList env={c.env} />
            </details>
          ) : null}
        </Fragment>
      ))}
    </Card>
  );
}
