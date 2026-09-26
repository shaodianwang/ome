---
name: slack-post
description: Post one report or status line to the OME working-group Slack channel (Meta workspace) through the Slack API Proxy with the Meta CLI, authored by the msl-ome-bot Crewmate when OME_SLACK_BOT is set and by the operator otherwise. Use when another skill says "post the summary to Slack", or when the user asks to "post this to the channel", "notify the working group", "send the triage to Slack", or "thread this under the earlier post". One message per invocation; never a direct Slack call; never a body with a credential in it.
---

# slack-post — one message to the working-group channel

The channel is `C0C37314X6U` in the Meta workspace (alias `meta`). Everything
goes through `meta slack.message send`, so the post is authorized by the
Slack API Proxy and no token exists on the machine that runs it. The command
is annotated `MODERATE / AUTONOMOUS` by `meta metacli.command annotations`,
so it needs no confirmation; a delete would, and this skill never deletes.

## Identity

| `OME_SLACK_BOT` | Author of the post | Needs |
|---|---|---|
| the Crewmate, `cm-msl-ome-bot` or its FBID `1555509462542873` | the `msl-ome-bot` app, bound to that Crewmate | `act_as` on `CREWMATE_ACCESS:1555509462542873` for the Meta identity running the command (the script runs the CLI as the Crewmate), and the bot invited to the channel |
| an app ID (`A…`) with no owner Crewmate | that app directly | `act_as_app` on `SLACK_APP_AUTH:<APP_ID>` |
| unset | you, through your own connected Slack account | a connection (`meta slack.auth status` prints the link when there is none) |

The portal binds every app to a Crewmate and grants `act_as_app` to that
Crewmate alone, so a person or a job posts by acting as the Crewmate, never by
naming the app. The app is `A0C4EU337J7`.

Acting as the Crewmate mints a delegated credential from an identity already
in the CLI's connection context, and only a certificate-backed identity can
carry the grant: a devserver or OnDemand for a person, a job identity (FaaS,
Sandcastle, a service identity) for automation. A laptop has no such identity,
so the Crewmate lane fails there before anything reaches Slack
("Failed to mint delegated CAT for CREWMATE ..."). Run the bot lane from a
devserver; the person lane works from anywhere.

Automation runs with the app; a person answering a question may post as
themselves. `--require-app` refuses the second lane so a scheduled job can
never post under a person's name.

## Post

```sh
P=.agents/skills/slack-post/scripts/slack_post.py
python3 $P --markdown-file "$OME_CITRIAGE_SCRATCH/b4757.md"          # a write-up (headings, tables, code fences render)
python3 $P --text 'main: b4757 passed (f846db86)'                     # one mrkdwn line
python3 $P --markdown-file r.md --thread-ts 1700000000.000100          # reply under an earlier post
python3 $P --text 'retry fired' --context 'via ci-triage' --button 'Build|https://buildkite.com/msl/ome/builds/4757'
python3 $P --markdown-file r.md --dry-run                              # the CLI previews the post without sending
python3 $P --markdown-file r.md --print                                # the exact command, nothing runs
```

The script refuses an empty body, a body over Slack's rendering limit
(12000 characters of Markdown, 4000 of mrkdwn; share longer reports with
`meta slack.message send --attachment-file`), and any body carrying a
credential-shaped token. It passes the body as a file, so nothing you post
lands in shell history or the CLI audit line. Bare diff, task, paste and SEV
numbers in the body become links; `@handle` mentions notify only people
who already have a Slack account.

## What goes to the channel

- **ci-triage** posts its write-up once per breakage, after it is in the
  reply and on GitHub; later updates for the same breakage thread under it.
- **oncall-loop** posts its iteration reply only when something happened
  (a new red build, an escalation, a follow-up that changed state). Quiet
  iterations stay off the channel.
- Anything a person asks to send, with `--dry-run` first when unsure.

Not for alerts: those flow from detectors through Pegasus to the same channel.

## Setting up the bot (once, by a member of the `ome` oncall)

1. Create the app in the internal Slack Apps Portal (wiki: Slack Developers
   → Onboarding a Slack App): name `msl-ome-bot`, a Crewmate name, oncall
   `ome`. The default manifest (`chat:write`, `channels:read`, org deploy)
   is enough; leave Meta webhooks off until the bot must answer mentions.
2. On the app's Overview page add the Meta (Prod) workspace to its
   distribution, then invite the bot to the channel.
3. Grant `act_as` on `CREWMATE_ACCESS:<crewmate FBID>` to each identity that
   will post (the portal seeds the creator and the oncall; add the job
   identity when a poller exists). `meta slack.app describe --app <APP_ID>`
   shows the registration and the Crewmate; `meta hipster.acl access
   --type=CREWMATE_ACCESS --name=<FBID> --action=act_as` shows who may act.
4. Give the Crewmate Meta CLI access on its Crewmate Hub profile (the
   MetaCLI card: Read / write, ceiling CONFIDENTIAL, since a send is an
   UPDATE and Slack content is CONFIDENTIAL) and request the Slack command
   access, which a human on the `slack_integration` oncall approves:
   ```sh
   meta amp.request create --acl-type=PM_ACTION --acl-name='1702230051323006/Access' \
     --action=access --grantee-type=CREWMATE --grantee-name=<crewmate FBID> \
     --reason='<bot> posts <what> to <channel>'
   ```
   Both are checked after the credential is minted; a refusal names the one
   that is missing. `auth.debug identities` additionally needs the Meta CLI
   login domain (`304977649833214/Login`), which `slack.message send` does
   not; do not treat that denial as a Slack problem.
5. On a devserver export `OME_SLACK_BOT=<crewmate FBID>` and send one
   `--dry-run`, then one real line.

## When the CLI refuses

The refusal names the fix. The common ones: no connected account (connect
through the printed link, or set `OME_SLACK_BOT`); "Acting as Crewmate … is
asked for, but this command is running as you" (the selector named the app;
use the Crewmate handle or FBID); "needs act_as on CREWMATE_ACCESS" (ask a
maintainer of the Crewmate for the grant); an app installed
org-wide needs the workspace (the script always passes `--workspace=meta`;
override with `OME_SLACK_WORKSPACE`); the bot is not in the channel (invite
it); `missing_scope` on `--username` or `--icon-emoji` (the app lacks
`chat:write.customize`; drop the persona flags). A "Retry after N seconds"
answer is Slack's rate limit: wait, do not loop. "The outcome may be unknown"
means check the channel before sending again.
