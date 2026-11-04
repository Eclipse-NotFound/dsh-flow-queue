# dsh-flow-queue

A visual flow editor for DeepSeek Harness, in the right sidebar. Draw **prompts, model switches, commands, condition branches, loops and checkpoints** as one directed graph; the Harness executes it in connection order — and **only at turn boundaries**.

## Why it exists

In DSH a `/model` switch applies immediately. If a turn is still running — or waiting to retry a dropped connection — the retry goes out with the new model, and "this turn" is silently served by a different model than it started with.

This plugin turns model switches, prompt delivery and command execution into **graph nodes**. The engine advances only after `turn/end` is committed and the session's Agent is idle, so:

- an in-flight request and every one of its retries keep the model they started with;
- a switch lands between two turns — which is exactly "before the next input";
- the order on the canvas is the execution order, and progress is visible while it runs.

## Interface

The plugin registers one right-sidebar tab, and the whole canvas lives inside it — no separate window:

- **Top row** — run state, start / pause / resume / stop, and a `⋯` menu (auto layout, packages, reset, clear).
- **Second row** — `+Prompt` `+Model` `+Command` `+Branch` `+Loop` `+Checkpoint`; one click adds a step.
- **Canvas** — drag cards to arrange them; drag from a card's right dot onto another card to connect (branches and loops expose two output dots: yes/no, again/exit); click a connection to remove it; drag empty canvas to pan, and `−` / `+` / `⤢` at the bottom-right control zoom.
- **Select a step** — the canvas area becomes that step's inspector (← goes back), with fields for its kind and "run from here" / "delete" in the header.
- **Packages** — `⋯ → Packages` saves, loads and deletes reusable flows in the same panel.

## Step types

| Type | What it does | Outputs |
|---|---|---|
| Prompt | Delivers one user message (optional image and file attachments; optional model for this step only) | next |
| Model switch | Switches the session's model at the next turn boundary | next |
| Command | Runs a slash command (`/compact`, `/plan`, …) through `commands.execute` | next |
| Branch | Evaluates a condition and picks a path | yes / no |
| Loop | Follows "again" until the exit condition holds or the iteration cap is reached | again / exit |
| Checkpoint | Stops and waits for a human to press continue | next |

### Conditions

A condition is evaluated against the **most recent outcome** (for a prompt step: the assistant text of that turn plus its tool failures; for a command step: the command result):

- `always`
- `regex` against the previous turn (multiline)
- `contains` a literal string
- `marker` — contains a completion marker (default `[[FLOW:DONE]]`; ask the model to print it when finished)
- `tool failed last turn` (a `tool/result` error in that turn)
- `last step failed` (the command failed, or the turn ended in error/abort)
- every condition can be negated

### What a prompt step can carry

- multi-line text;
- image attachments (read as base64, stored through the Host's `attachments.saveImages`, sent as an `image` content part);
- any file attachment (`attachments.admitEncodedFile` → `file` content part);
- a model override for this step (empty = keep the current one).

**Known difference:** `@path` references are expanded by the native composer at submit time. The graph editor does not go through the composer, so `@` is sent as plain text — attach the file instead. Up to 8 attachments per step.

## Run states

`idle / running / waiting (checkpoint) / paused / done / error`. Each card shows its state, duration, the model that actually served it (read from that turn's `request/header`), errors, and command output.

- **No ghost successes** — a turn that ends in error/abort/interrupt marks the step failed.
- **No stuck queues** — a prompt accepted but with no turn started within 30 s fails the step (`no-turn`) and marks the run as errored.
- **Restart safe** — steps still marked running when the process restarts become `interrupted` and the run becomes paused; it never resumes by itself.
- Editing the graph requires a stopped run; pause or stop first.

## Flow packages

`⋯ → Packages` stores the current graph under `$DSH_PROFILE_DIR/flow-queue/packages/*.json` (nodes and edges only, no run state) and loads or deletes them from any session.

## Install

```sh
dsh plugin --profile web add github:Eclipse-NotFound/dsh-flow-queue
```

Then restart the Harness web runtime — bundles are composed at startup — and reload the page. Requires DSH `>=0.2.1-rc.1 <0.3.0`.

The plugin adds same-origin routes under `/flow-queue/api/…`; every one of them goes through `ctx.connection.requestRejection`, so only the authenticated page can call them. It stores only what you create (the graph per session and the packages directory) and sends nothing anywhere else.

## Development

```sh
node --check client.js
node --check host2.js
node test/offline-check.mjs
```

The test drives the real Host module against a mock Cordis context — no DSH process, no network. Eight scenarios: boundary guard, branching, loops (including the iteration cap), checkpoint, command step, attachments, package round-trip, and the validation guards.

### Hot-swapping the Host half

A running Harness caches an imported module by its resolved URL, so editing `hostN.js` does not take effect on its own. To ship a change without restarting the app:

1. save the new implementation as the next file (`host3.js`) and update both the `index.js` re-export and the row name in `cordis.patch.yml` (`dsh-flow-queue/host3`);
2. toggle the bundle off and on in the plugin manager (or restart the app).

The client half needs none of this: the page fetches the client bundle again, so a page reload is enough.

## Not implemented yet

- package variables (`{{var}}` placeholders filled at load time);
- attachment thumbnails, drag reordering, marquee selection, undo/redo;
- an LLM-judged branch (the completion-marker convention stands in for it, to avoid an extra model call per turn);
- canvas and inspector side by side (in a narrow column they take turns; ← returns to the canvas).

## License

MIT — see [LICENSE](LICENSE).
