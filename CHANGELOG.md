# Changelog

All notable changes to this project are documented here.

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
