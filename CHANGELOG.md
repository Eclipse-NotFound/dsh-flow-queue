# Changelog

All notable changes to this project are documented here.

## 0.5.0 — 2026-11-06

- New step type `Fan-out`: one shared prompt goes to up to 8 throwaway subagents through the in-process `spawn` backend. Each lane may force its own provider/model/reasoning effort (passed as the child's `agentOptions`); an empty lane inherits the session route. Collection is `all`, `k-of-n` (cancel the stragglers once k finish) or `first`; each lane's output is aggregated into the flow outcome, so the next branch, judge or loop reads one document.
- The parent Session's turn is untouched: completion arrives through each activation's own result promise, so the fan-out waits beside the turn boundary rather than inside a request — the plugin's one invariant holds for subagents exactly as for prompts.
- Lane state shows on the card as chips (pending / running / done / failed / timed out / cancelled / interrupted). A single failing lane does not fail the step; only "no lane finished" does (`dispatch-failed`). The whole step has one timeout (15 s–30 min, default 10 min), and `Stop` cancels every in-flight lane (`dispatch-stopped`).
- Tests: five new Host scenarios — fan-out aggregation with one failing lane, early k-of-n collection with straggler cancellation, timeout, stop-cancels-in-flight, and a loop that re-fans-out every iteration.

## 0.4.0 — 2026-11-06

- Branch and loop conditions gain a `model judges` type: a natural-language condition such as "latency is below 10ms". Reaching the step starts exactly one minimal judge turn — roughly 40 instruction tokens plus the condition verbatim — and the model answers YES or NO on the first reply line; the engine parses that line and nothing else. The verdict (with a reason excerpt) shows on the card, `negate` applies, a reply without YES/NO fails the step as `no-decision`, and a loop re-judges fresh on every iteration. No tool is registered — a link-installed package cannot import the tool DSL, and a global judge tool would sit in every session's schema — so the prompt table of every session stays untouched.

## 0.3.0 — 2026-11-06

- The prompt editor gains the composer's two affordances. Typing `/` at a line start opens the session's command catalog (host commands) and its skill catalog — a command inserts `/name `, a skill inserts a "use this skill" instruction the model loads from its own catalog. Typing `@` opens workspace file references through the Host's `@file` discovery (directories keep the menu open on their trailing slash); a reference is text, not injected content.
- A prompt step whose entire text is exactly one host command line now runs as that command at the turn boundary instead of being sent to the model, matching the composer's rule that a command line is never silently downgraded to a plain prompt. Multi-line text and unknown commands are still delivered as prompts.
- New Host route `GET /flow-queue/api/mention` serves the two catalogs per session (commands and skills cached 30 s; file candidates live).

## 0.2.6 — 2026-11-06

- Fix: connections never worked in any version. The output port called its start handler with only `(port, event)`, so the drag carried the port name as the source node id and the Host rightly rejected the edge (`no such node`). The port now passes the owning node's id with the port.
- The reasoning-effort row is now always visible once a model is chosen: models that declare levels get the dropdown; models that declare none (a hand-written provider entry, for example) get an explicit note instead of a silently missing control.
- The panel footer shows the client build (`v0.2.6`), so a stale page is identifiable at a glance.

## 0.2.5 — 2026-11-06

- Fix: dragging never moved a card and every press opened the inspector. The "did this gesture move" check read `start.x`/`start.y` while the press point is stored as `startX`/`startY`, so the comparison was `NaN > 4` — always false. Every release therefore counted as a click, and connection drags never completed either. Both now read the stored fields.
- Fix: the `⋯` menu items (auto layout, packages, reset, clear) never ran. The outside-press close listener was attached to `document` `pointerdown` without excluding the menu itself, so the menu was removed before the item's `click` landed. The menu region and its trigger are now marked and excluded from closing.
- A committed drag keeps its local position until the server state returns, so a card no longer snaps back to its old spot for a beat; loading a package or clearing the graph drops locally cached positions so a reused node id renders at the loaded position.

## 0.2.4 — 2026-11-05

- Editing no longer takes the canvas away: a click opens the inspector as a drawer under the canvas, so dragging a step is never interrupted by a layout swap. A press that does not move counts as the click; the whole card is the drag handle.
- Connections can be drawn either way: drag from an output dot onto the target step, or click the dot and then click the target step (Esc and a click on empty canvas cancel). A release after a real drag connects; a release without movement only arms the connection.
- Model steps carry the model's reasoning effort when it declares levels; the picker appears under the model selector and keeps the current effort only when the newly chosen model supports it.

## 0.2.3 — 2026-11-05

- The right sidebar's start page clips its card list instead of scrolling, so the last entries (including "more tools") become unreachable once several plugins register tabs. The plugin now renders its own card there and, while that card is mounted, finds the container that actually clips and switches it to scrolling; the original inline styles are restored on unmount.
- `test/client-apply-check.mjs` also asserts the guide card registration.

## 0.2.2 — 2026-11-05

- Fix: the client half did not load at all under 0.2.1. The bundle row named a versioned subpath (`dsh-flow-queue/host2`), which hands the client module system a row id that the client bundle never registers under — so the right-sidebar tab and its sidebar entry never appeared. The row now names the package itself and the Host implementation is back in `index.js`; Host updates apply on a bundle toggle or app restart.

## 0.2.1 — 2026-11-04

- The editor now lives inside the right-sidebar tab instead of a frame-wide overlay: run controls and a `⋯` menu on top, a palette row, the canvas, and a one-line footer.
- Narrow-column layout: node width 196 px, top-down auto layout, empty-canvas drag pans the view.
- Selecting a step swaps the canvas area for its inspector (← returns); packages use the same sheet.

## 0.2.0 — 2026-11-04

- The queue became a flow graph: prompt, model switch, command, branch, loop and checkpoint nodes with port-labelled connections (`next`; `yes`/`no`; `again`/`exit`).
- Condition branches: always, regex on the previous turn, contains, completion marker, tool failed last turn, last step failed — each negatable.
- Loops with an exit condition and a hard iteration cap; checkpoints that wait for a human.
- Prompt steps carry image and file attachments and an optional per-step model override.
- Command steps run through `commands.execute` and show the command's output on the card.
- Flow packages: save, load and delete a graph per profile.
- Host half moved to a versioned implementation file so a running Harness can pick up a Host update without an app restart.

## 0.1.0 — 2026-11-04

- Turn-boundary action queue: prompts and model switches staged in the right sidebar and applied only after `turn/end` with an idle Agent, so an in-flight request and its retries keep their model.
- Flat queue of prompt / model-switch entries with add, remove, reorder, start, pause and per-step retry.
