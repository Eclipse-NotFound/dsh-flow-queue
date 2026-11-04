# Changelog

All notable changes to this project are documented here.

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
