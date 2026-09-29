# ome-bot

The deterministic core of the OME Slack bot: an HTTP service that runs
read-only `kubectl ome` commands against every cluster in its registry and
reports which clusters run OME. A relay in front of it owns Slack, so this
Deployment holds no Slack credential.

## API

| Method and path | Body | Reply |
|---|---|---|
| `POST /v1/command` | `{"actor":{"employeeFbid":1,"slackUserId":"U1"},"channelId":"C1","text":"get isvc -A"}` | `{"ok":true,"text":"<slack mrkdwn>","sections":[...]}` |
| `GET /v1/clusters` | | one row per registry cluster with reachability, k8s version, OME presence and version |
| `GET /healthz`, `GET /readyz` | | probes |

With `config.auth.mode: hmac` every request carries `X-OME-Bot-Timestamp` and
`X-OME-Bot-Signature`, the hex HMAC-SHA256 of `timestamp + "\n" + body` under
the shared key in `config.auth.existingSecret`.

Commands accepted in `text`: `help`, `installed clusters`, and the read-only
kubectl-ome verbs (`get`, `status`, `rollout status|explain|history|validate`,
`traffic explain`, `placement endpoint`, `quota tree|validate`, `instance
list|status|retry-blocks`, `migration status|history`, `runtime
explain|effective|tree|history`, `autoscale status|explain`, `accelerator
explain`, `admin doctor|recommendations`, `cluster status`, `logs`) with
`--cluster <name>`. `get` and `status` without `--cluster` run on every
cluster. Guarded verbs and credential-changing flags are refused.

## Registry

`clusters` lists the fleet. An entry with `server` and `tokenArgs` is rendered
at startup into an exec-credential kubeconfig that runs `config.execCommand`,
so the file carries no secret and the pod's own identity mints every token.
Entries may instead name a mounted `kubeconfigPath` or set `inCluster: true`.

## Install

```sh
helm install ome-bot charts/ome-bot -n ome-bot --create-namespace \
  --set config.auth.mode=none \
  --set-json 'clusters=[{"name":"local","provider":"mks","inCluster":true}]'
kubectl -n ome-bot port-forward svc/ome-bot 8080:8080
curl -s localhost:8080/v1/command -d '{"text":"installed clusters"}'
```
