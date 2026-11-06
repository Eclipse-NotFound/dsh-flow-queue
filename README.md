# dsh-flow-queue

A visual flow editor for DeepSeek Harness, in the right sidebar. Draw **prompts, model switches, commands, condition branches, loops, checkpoints, subagent fan-outs and cross-session sends/waits** as one directed graph; the Harness executes it in connection order — and **only at turn boundaries**. A **Fleet** view layers the workspace's collaborating sessions above the flows.

## Why it exists

In DSH a `/model` switch applies immediately. If a turn is still running — or waiting to retry a dropped connection — the retry goes out with the new model, and "this turn" is silently served by a different model than it started with.

This plugin turns model switches, prompt delivery and command execution into **graph nodes**. The engine advances only after `turn/end` is committed and the session's Agent is idle, so:

- an in-flight request and every one of its retries keep the model they started with;
- a switch lands between two turns — which is exactly "before the next input";
- the order on the canvas is the execution order, and progress is visible while it runs.

## Quickstart (5 minutes)

From zero to a running **Prompt → Model switch → Branch** flow.

### 1. Install

```sh
dsh plugin --profile web add github:Eclipse-NotFound/dsh-flow-queue
```

Then restart the Harness web runtime — bundles are composed at startup — and reload the page. Requires DSH `>=0.2.1-rc.1 <0.3.0`.

**Check**: the plugin's icon appears at the sidebar foot beside Settings; hovering it says "Open the flow queue".

### 2. Open the panel

Click that icon, or the "Flow queue" card on the right sidebar's start page. The right sidebar gains a "Flow queue" tab, and the canvas lives inside it. Next to the title sit two view toggles, "Flow" and "Fleet" — this tour stays on "Flow" ("Fleet" manages the workspace's collaborating sessions).

**Check**: the run chip beside the title reads "Idle"; the Start button on the top bar's right side is greyed out while the canvas is empty (the `⋯` menu sits farthest right); the panel footer shows `v0.7.0`.

### 3. Add three cards

In the second button row click `+Prompt`, then `+Model switch`, then `+Branch`. Each click adds a card to the canvas; the first card you added — the Prompt — is where the run starts.

**Check**: three cards on the canvas with the type chips Prompt / Model switch / Branch.

### 4. Fill the steps in

Clicking a card (a press that does not move) opens its inspector drawer under the canvas, headed "Edit step"; `↓` closes it.

- **Prompt** — write something decidable into the "Prompt text" box, e.g. "Confirm in one sentence that you received this message, then print exactly FLOW-OK on a new line", and save with `Ctrl/⌘ + Enter` (losing focus also saves). Leave "Model" on "Not set (keep current)".
- **Model switch** — pick the target model from the "Model" dropdown; models that declare reasoning levels also let you pick a "Reasoning effort".
- **Branch** — set "Condition" to "Contains text" and "Match value" to `FLOW-OK`.

**Check**: the three card bodies now show the prompt's first line, `provider / model`, and `Contains text · FLOW-OK`.

### 5. Connect

Drag from the Prompt card's right dot onto the Model-switch card (or click the dot, then click the target card), then connect Model switch to Branch. Prompt and Model switch have one output dot (hover: "Next · Click a connection to remove it"); Branch has two, "Yes" and "No". To make the branch's direction visible, add a `+Checkpoint` card and connect "Yes" to it.

**Check**: the three cards are chained by two connections; with the checkpoint added, the "Yes" dot carries a connection too.

### 6. Run

Press "Start". The prompt is delivered into the current session as a real user message; only after that turn ends — right before the next input — does the session switch models; the branch then evaluates the prompt turn's reply. Configuration steps like Model switch never overwrite the outcome conditions read.

**Check**:

- the run chip turns "Running", then "Done" when the whole flow finishes;
- the Prompt card shows "Done", with the model that actually served it in its header;
- with the checkpoint: a reply containing `FLOW-OK` takes "Yes" and stops at the checkpoint — the chip reads "Waiting" and "Continue" finishes the run; without it the flow ends directly, leaving the checkpoint card "Pending".

Two running rules: edit the graph only while it is stopped (edits are rejected mid-run — "Pause" or "Stop" first), and pressing "Start" again resets every step's status and reruns from the top.

To reuse this flow later, save it via `⋯ → Packages`; all nine step types are described under "Step types" below.

## Interface

The plugin registers one right-sidebar tab, and the whole canvas lives inside it — no separate window:

- **Top row** — run state, start / pause / resume / stop, and a `⋯` menu (auto layout, packages, reset, clear).
- **Second row** — `+Prompt` `+Fan-out` `+Send message` `+Wait session` `+Model switch` `+Command` `+Branch` `+Loop` `+Checkpoint`; one click adds a step.
- **Canvas** — the whole card is the drag handle; drag from a card's right dot onto another card to connect (branches and loops expose two output dots: yes/no, again/exit), or click the dot and then click the target card; click a connection to remove it; drag empty canvas to pan, and `−` / `+` / `⤢` at the bottom-right control zoom.
- **Select a step** — a click (a press that does not move) opens that step's inspector as a drawer under the canvas, so the canvas keeps its place and dragging is never interrupted; the drawer header offers "run from here" / "delete", and ↓ closes it.
- **Packages** — `⋯ → Packages` saves, loads and deletes reusable flows in the same panel.

Two entry points open it: the icon beside Settings at the sidebar foot, and the "Flow queue" card on the right sidebar's start page. That start page lists one card per tab type and clips its overflow instead of scrolling, so with enough plugins installed the last cards become unreachable; the plugin's own card walks up to the container that clips and turns on scrolling while it is mounted (the original inline styles come back when it unmounts).

## Step types

| Type | What it does | Outputs |
|---|---|---|
| Prompt | Delivers one user message (optional image and file attachments; optional model for this step only) | next |
| Model switch | Switches the session's model at the next turn boundary, including the reasoning effort when the model declares levels | next |
| Command | Runs a slash command (`/compact`, `/plan`, …) through `commands.execute` | next |
| Branch | Evaluates a condition and picks a path | yes / no |
| Loop | Follows "again" until the exit condition holds or the iteration cap is reached | again / exit |
| Checkpoint | Stops and waits for a human to press continue | next |
| Fan-out | Sends one shared prompt to up to 8 throwaway subagents (`spawn` backend) — each lane may force its own provider/model/effort or inherit the session route — then aggregates every lane's output into the flow outcome. Collection is `all`, `k-of-n` (cancel the stragglers once k finish) or `first`. The parent turn is untouched, so the fan-out waits *beside* the boundary, never across it | next |
| Send message | Delivers one message to another fleet session (persisted first, backoff retries, strictly ordered per target; a dead letter blocks its queue until revived). May attach this session's touched-file list (file-tool intents only, blind spots labelled) | next |
| Wait session | Waits until the target finished a turn after the wait started and is idle, or until its latest turn text contains a marker (`[[FLOW:REPORT` by default) | next |

### Conditions

A condition is evaluated against the **most recent outcome** (for a prompt step: the assistant text of that turn plus its tool failures; for a command step: the command result):

- `always`
- `regex` against the previous turn (multiline)
- `contains` a literal string
- `marker` — contains a completion marker (default `[[FLOW:DONE]]`; ask the model to print it when finished)
- `tool failed last turn` (a `tool/result` error in that turn)
- `last step failed` (the command failed, or the turn ended in error/abort)
- `model judges` (**natural-language condition**, e.g. "latency is below 10ms"): reaching the step starts **one** minimal judge turn — about 40 instruction tokens plus the condition verbatim; the model verifies with the cheapest sufficient check and answers **YES or NO on the first line**, which is the only thing the engine parses. The card shows the verdict and a reason excerpt; a reply without YES/NO fails the step (`no-decision`). No global tool is registered and no session's tool table changes; inside a loop every iteration judges fresh.
- every condition can be negated

### What a prompt step can carry

- multi-line text;
- image attachments (read as base64, stored through the Host's `attachments.saveImages`, sent as an `image` content part);
- any file attachment (`attachments.admitEncodedFile` → `file` content part);
- a model override for this step (empty = keep the current one).

**`/` commands and skills, `@` file references** — the prompt editor keeps the composer's two habits:

- typing `/` at a line start opens the session's **command** catalog (host commands such as `/compact`) and its **skill** catalog; ↑↓ walks, Enter/Tab accepts, Escape closes. A command inserts `/name `, a skill inserts "Use the {name} skill: " for the model to load from its catalog.
- typing `@` opens **workspace file references** (native `@file` semantics: the path is referenced, content is not injected — the model reads it itself); picking a directory keeps the menu open on its trailing slash.
- a prompt step whose **entire text is one host command line** (just `/compact`) runs as that command at the turn boundary instead of being sent to the model — the composer's "a command line is never silently downgraded to a plain prompt" rule, kept for queued steps too. Multi-line text and unknown commands are still delivered as prompts.
- commands that exist only as client popups (pickers without a host command row, such as `/model`) are neither offered in the menu nor executed inline.

**Known difference:** an `@path` is a text reference, not injected content — use "Add attachment" to ship the bytes. Up to 8 attachments per step.

## Run states

`idle / running / waiting (checkpoint) / paused / done / error`. Each card shows its state, duration, the model that actually served it (read from that turn's `request/header`), errors, and command output.

- **No ghost successes** — a turn that ends in error/abort/interrupt marks the step failed.
- **No stuck queues** — a prompt accepted but with no turn started within 30 s fails the step (`no-turn`) and marks the run as errored.
- **Restart safe** — steps still marked running when the process restarts become `interrupted` and the run becomes paused; it never resumes by itself.
- Editing the graph requires a stopped run; pause or stop first.

## Flow packages

`⋯ → Packages` stores the current graph under `$DSH_PROFILE_DIR/flow-queue/packages/*.json` (nodes and edges only, no run state) and loads or deletes them from any session.

A package may declare **variables**: write `{{task}}` in a prompt / fan-out / send text (or a command or note), and loading the package asks for one fill per name (`{{task}}` → your text) before the graph lands. Unfilled placeholders stay visible in the loaded steps — never an empty prompt by surprise. Saving keeps declared vars and auto-derives any `{{placeholder}}` still present in the texts, so "load, retype some parts as placeholders, save" yields a reusable template.

Two packages ship built in (written once, never overwritten — an edited copy stays edited):

- `report-protocol` — the four-state turn-end marker (`[[FLOW:REPORT waiting|ready|blocked|failed]]`) as a worker-prompt template plus the matching marker wait.
- `superagent-review` — the whole superagent lifecycle as one runnable graph: ticket-first worker dispatch → readiness branch on the four-state marker → an independent reviewer → a bounded follow-up loop (≤3 rounds) → a delivery gate that hands the push back to this session. Two vars to fill on load: `{{task}}` and `{{ticket}}`. It runs in any session with no fleet setup: workers and reviewers are throwaway subagents whose context lives in the ticket file, and the opening checkpoint parks the run so you confirm the filled prompts before a single subagent is spent.

## Install

```sh
dsh plugin --profile web add github:Eclipse-NotFound/dsh-flow-queue
```

Then restart the Harness web runtime — bundles are composed at startup — and reload the page. Requires DSH `>=0.2.1-rc.1 <0.3.0`.

The plugin adds same-origin routes under `/flow-queue/api/…`; every one of them goes through `ctx.connection.requestRejection`, so only the authenticated page can call them. It stores only what you create (the graph per session and the packages directory) and sends nothing anywhere else.

## Development

```sh
node --check client.js
node --check index.js
node test/offline-check.mjs      # Host module against a mock Cordis context
node test/client-apply-check.mjs # client activation against a mock loader/context
```

The Host check runs twenty-five scenarios: boundary guard, branching, loops (including the iteration cap), checkpoint, command step, attachments, package round-trip, validation guards, mention catalogs and inline command lines, the natural-language judge, fan-out aggregation with a failing lane, early k-of-n collection, fan-out timeout, stop-cancels-fan-out, loop re-fan-out, outbox delivery with fleet CRUD, backoff/dead-letter/revive ordering, wait-idle, wait-marker, the hop cap with the change-attribution block, fleet.stop idempotence, the shipped report-protocol package, the superagent package with `{{var}}` fills, the superagent loop end to end (ready → blocked review → follow-up → pass), and a marker wait feeding the branch that follows it. The client check emulates the browser module loader and confirms `apply` registers the tab type, the tab body and the sidebar entry.

### Updating a running install

The Host half is loaded once per process, so a Host change needs a bundle toggle in the plugin manager or an app restart. The client half is fetched by the page, so a reload is enough — but keep the bundle row naming the package itself (`name: dsh-flow-queue`): a subpath row gives the client module system a row id the bundle never registers under, and the whole client half silently stays unloaded.

## Not implemented yet

- attachment thumbnails, drag reordering, marquee selection, undo/redo;
- fan-out lanes beyond 8 per step, and cross-step sharing of one fan-out's results;
- canvas and inspector side by side (in a narrow column they take turns; ← returns to the canvas);
- warm reuse of a dispatched worker (a follow-up round spawns a fresh subagent that rebuilds context from the ticket file; sending back into the original child session needs verified delivery).

## License

MIT — see [LICENSE](LICENSE).
