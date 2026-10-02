# ome-bot

The tool server behind the OME Slack bot. It runs read-only `kubectl ome`
commands against every cluster in its registry, reports which clusters run
OME, runs a closed set of read-only tools (CI scripts and a built-in docs
search), reports the release pipeline from Kargo, and, on clusters an operator
marks writable, runs kubectl-ome's guarded verbs for cleared, confirmed
callers. Its callers own Slack, so this Deployment holds no Slack credential.

## API

| Method and path | Body | Reply |
|---|---|---|
| `POST /v1/command` | `{"actor":{"employeeFbid":1,"slackUserId":"U1"},"channelId":"C1","text":"get isvc -A"}` | `{"ok":true,"text":"<slack mrkdwn>","sections":[...]}` |
| `POST /v1/tool` | `{"requestId":"r1","tool":"docs","args":["search","rollout"]}` | `{"ok":true,"text":"<slack mrkdwn>","result":{"exitCode":0,"output":"..."}}` |
| `GET /v1/release` | | Kargo stages, recent Freight and the version each cluster runs; 404 `not_configured` without `config.kargo` |
| `GET /v1/clusters` | | one row per registry cluster with reachability, k8s version, OME presence and version |
| `GET /healthz`, `GET /readyz` | | probes |

With `config.auth.mode: hmac` every request carries `X-OME-Bot-Timestamp` and
`X-OME-Bot-Signature`, the hex HMAC-SHA256 of `timestamp + "\n" + body` under
the shared key in the file `config.auth.secretKey` inside `secrets.volume`.

A refused request answers HTTP 200 with `ok: false` and an `errorClass`
(`not_a_verb`, `forbidden_flag`, `unknown_cluster`, `needs_cluster`,
`malformed`, `unknown_tool`, `subcommand_not_allowed`, `bad_args`,
`timeout`, and for guarded verbs `mutations_off`, `cluster_not_writable`,
`actor_not_allowed`, `needs_confirmation`). More than `config.maxInFlight`
concurrent requests that run a process or probe clusters answer 429 `busy`;
a body over `config.maxRequestBytes` answers 413. The audit line records the
caller, the command or tool arguments, whether it was a mutation and the
outcome, never the output.

Commands accepted in `text`: `help`, `installed clusters`, and the read-only
kubectl-ome verbs (`get`, `status`, `rollout status|explain|history|validate`,
`traffic status|explain`, `placement status|explain|endpoint`, `quota
status|tree|validate`, `instance
list|status|retry-blocks`, `migration status|history`, `runtime
explain|effective|tree|history`, `autoscale status|explain`, `accelerator
explain`, `admin doctor|recommendations`, `cluster status`, `logs`) with
`--cluster <name>`. `get` and `status` without `--cluster` run on every
cluster. Credential-changing flags are refused.

### Guarded verbs

kubectl-ome's eleven actions that change a cluster (`rollout
pause|resume|promote|rollback|repin`, `traffic drain|undrain`, `migration
start`, `instance release-held`, `runtime sync`, `scale`) are parsed too, and
run only when four conditions hold, each refused with its own class:

| Condition | Refusal |
|---|---|
| `config.mutations.enabled` is true | `mutations_off` |
| the request names `--cluster <name>` and that entry has `writable: true` | `needs_cluster`, `cluster_not_writable` |
| the relay sent `actor.canMutate: true` (its own verdict, for example oncall membership) | `actor_not_allowed` |
| the text carries `--yes`, kubectl-ome's own confirmation | `needs_confirmation` |

A guarded verb never fans out, and `--yes` on a read verb is `forbidden_flag`.
The reply of a guarded verb carries `mutation: true` so the relay can post it
where the channel sees it; `--dry-run=server --yes` previews without
persisting. The bot's own answer worker never issues these verbs; they exist
for people typing explicit commands.

## Registry

`clusters` lists the fleet. An entry with `server` and `tokenArgs` is rendered
at startup into an exec-credential kubeconfig that runs `config.execCommand`,
so the file carries no secret and the pod's own identity mints every token.
Entries may instead name a mounted `kubeconfigPath` or set `inCluster: true`.
`writable: true` admits the guarded verbs on that entry once
`config.mutations.enabled` is on; the identity behind the kubeconfig must then
hold a role that can patch OME objects there, which the read-only viewer role
does not.

## Tools and docs

`config.tools` is a closed registry: a call names a tool and passes
arguments, the first of which must be one of the tool's `subcommands`.
Arguments reach the command after `argsPrefix` without a shell, and any
argument equal to or prefixed by a `forbiddenFlags` entry is refused.
`envFromFiles` values are read from the secrets volume on every call.

The built-in `docs` tool answers `search <query> [--root R] [--regexp]
[--max N]` and `read <root>/<path> [--from L] [--lines N]` over the
`config.docs.roots` the image bakes in. Paths cannot leave their root.

`config.kargo` names the Kargo project behind `GET /v1/release`. It reads
Kargo either through a registry entry (`cluster`) or with the pod's own
ServiceAccount (`inCluster: true`) when Kargo runs beside the bot.

## Secrets

`secrets.volume` is any volume source mounted read-only at
`/etc/ome-bot/secrets`. A CSI secrets-store volume works without creating a
Kubernetes Secret:

```yaml
secrets:
  volume:
    csi:
      driver: secrets-store.csi.k8s.io
      readOnly: true
      volumeAttributes:
        secretProviderClass: ome-bot-secrets
```

## Server-to-server access

With `service.api.enabled=true` the chart adds a headless Service annotated
with `service.api.smcTierName`. The platform registers its pod endpoints as
that SMC tier, and `mks-http-fe` routes to it by the `service_endpoint`
header (the HMAC headers still apply):

```bash
curl -H "service_endpoint: <smcTierName>" https://mks-http-fe.fbinfra.net/v1/clusters
```

## Install

```sh
helm install ome-bot charts/ome-bot -n ome-bot --create-namespace \
  --set config.auth.mode=none \
  --set-json 'clusters=[{"name":"local","provider":"mks","inCluster":true}]'
kubectl -n ome-bot port-forward svc/ome-bot 8080:8080
curl -s localhost:8080/v1/command -d '{"text":"installed clusters"}'
```
