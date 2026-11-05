// Offline check for the dsh-flow-queue Host half (v2 graph engine).
//
// Drives the real module against a mock Cordis context: no DSH process, no
// network, no real Session. It covers the property the plugin exists for (no
// model change or prompt may land inside a live turn) plus the graph engine:
// branching, loops, checkpoints, command steps, attachments and flow packages.
//
//   node test/offline-check.mjs

import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';

import { apply } from '../index.js';

const SESSION = 'offline-session';

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Poll the Host snapshot (async predicates would otherwise resolve immediately). */
async function waitForState(harness, predicate, label, timeoutMs = 5000) {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		const value = await harness.state();
		if (predicate(value)) return value;
		if (Date.now() > deadline) throw new Error(`timed out waiting for ${label}`);
		await sleep(30);
	}
}

/** Poll a predicate: the engine awaits a persisted write, so timing varies. */
async function waitFor(predicate, label, timeoutMs = 5000) {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		if (predicate()) return;
		if (Date.now() > deadline) throw new Error(`timed out waiting for ${label}`);
		await sleep(20);
	}
}

async function makeHarness() {
	const stateDir = await mkdtemp(path.join(tmpdir(), 'flow-queue-'));
	const routes = new Map();
	const listeners = new Map();
	const calls = { selectModel: [], prompt: [], command: [] };
	const world = { agentStatus: 'idle', agentLive: true, routes, listeners, calls, stateDir };

	const ctx = {
		effect(run) {
			const dispose = run();
			return typeof dispose === 'function' ? dispose : () => {};
		},
		on(name, handler) {
			const list = listeners.get(name) ?? [];
			list.push(handler);
			listeners.set(name, list);
			return () => {};
		},
		get(name) {
			if (name === 'agents') {
				return {
					get(id) {
						return world.agentLive ? { id, status: world.agentStatus } : undefined;
					},
				};
			}
			if (name === 'commands') {
				return {
					async execute(agent, line) {
						calls.command.push({ agent, line });
						if (line.startsWith('/unknown')) return undefined;
						return { commandId: 'cmd-1', result: { kind: 'success', text: `ran ${line}` } };
					},
				};
			}
			if (name === 'attachments') {
				return {
					async saveImages(inputs) {
						return inputs.map((input, index) => ({
							attachmentId: `img-${index}-${input.name ?? 'x'}`,
							mediaType: input.mediaType,
							bytes: 12,
							width: 4,
							height: 4,
							name: input.name,
						}));
					},
					async admitEncodedFile(input) {
						return { attachmentId: `file-${input.name ?? 'x'}`, name: input.name ?? 'file', bytes: 12 };
					},
				};
			}
			return undefined;
		},
		webServer: {
			register(route) {
				routes.set(route.path, route.handler);
				return () => routes.delete(route.path);
			},
		},
		connection: { requestRejection: () => undefined },
		sessionController: {
			async selectModel(request) {
				calls.selectModel.push(request);
				return { selected: request };
			},
			async prompt(request) {
				calls.prompt.push(request);
				return { accepted: true };
			},
			async resolveAgent(id) {
				return { agent: { id } };
			},
			async modelCatalog() {
				return { default: { provider: 'p', model: 'm' }, groups: [], failures: [] };
			},
		},
	};

	apply(ctx, { stateDir });

	function emit(name, ...args) {
		for (const handler of listeners.get(name) ?? []) handler(...args);
	}

	async function call(routePath, { method = 'GET', search = '', body } = {}) {
		const handler = routes.get(routePath);
		assert.ok(handler, `route ${routePath} is registered`);
		const payload = body === undefined ? '' : JSON.stringify(body);
		const registered = {};
		const req = {
			method,
			url: `${routePath}${search}`,
			on(name, listener) {
				registered[name] = listener;
				return req;
			},
			destroy() {},
		};
		const result = new Promise((resolve) => {
			const res = {
				writeHead(status) {
					this.status = status;
				},
				end(text) {
					resolve({ status: this.status, json: text ? JSON.parse(text) : undefined });
				},
			};
			handler(req, res);
		});
		await sleep(0);
		if (payload !== '' && registered.data) registered.data(Buffer.from(payload));
		if (registered.end) registered.end();
		return result;
	}

	async function command(body) {
		const response = await call(ROUTES.command, { method: 'POST', body: { sessionId: SESSION, ...body } });
		assert.equal(response.status, 200, `command ${body.op} failed: ${JSON.stringify(response.json && response.json.error)}`);
		return response.json.value;
	}

	async function state() {
		const response = await call(ROUTES.state, { search: `?sessionId=${SESSION}` });
		assert.equal(response.status, 200);
		return response.json.value;
	}

	const session = { id: SESSION, header: {} };
	function turnStart(turn) {
		emit('session/event', session, { type: 'turn/start', seq: 1, time: Date.now(), data: { turn } });
	}
	function assistant(turn, text) {
		emit('session/event', session, {
			type: 'assistant/message',
			seq: 2,
			time: Date.now(),
			data: { turn, step: 1, message: { role: 'assistant', content: [{ type: 'text', text }], source: { kind: 'model', provider: 'p', model: 'm' } }, stream: [] },
		});
	}
	function turnEnd(turn, kind = 'completed') {
		emit('session/event', session, { type: 'turn/end', seq: 3, time: Date.now(), data: { turn, reason: { kind } } });
	}
	function toolError(turn) {
		emit('session/event', session, {
			type: 'tool/result',
			seq: 4,
			time: Date.now(),
			data: { turn, step: 1, message: { role: 'tool', content: [], source: { kind: 'tool' }, toolCallId: 'c1' }, error: { name: 'bash', code: 'failed' } },
		});
	}

	return { world, call, command, state, turnStart, assistant, turnEnd, toolError, emit, session };
}

const ROUTES = {
	state: '/flow-queue/api/state',
	models: '/flow-queue/api/models',
	command: '/flow-queue/api/command',
	packages: '/flow-queue/api/packages',
};

async function cleanup(harness) {
	// A persist may still be landing; Windows refuses to remove a non-empty tree.
	await sleep(150);
	for (let attempt = 0; attempt < 6; attempt += 1) {
		try {
			await rm(harness.world.stateDir, { recursive: true, force: true });
			return;
		} catch {
			await sleep(80);
		}
	}
}

//#region scenario 1: a live turn keeps every queued action parked

{
	const h = await makeHarness();
	h.world.agentStatus = 'running';

	const first = await h.command({ op: 'node.add', kind: 'model', provider: 'acme', model: 'model-b', modelLabel: 'Model B', reasoningEffort: 'high' });
	const prompt = await h.command({ op: 'node.add', kind: 'prompt', text: 'write the patch' });
	await h.command({ op: 'edge.add', from: first.node.id, port: 'next', to: prompt.node.id });

	await h.command({ op: 'run.start' });
	await sleep(300);
	assert.equal(h.world.calls.prompt.length, 0, 'a live turn must keep the prompt parked');
	assert.equal(h.world.calls.selectModel.length, 0, 'a live turn must keep the model switch parked');

	h.turnEnd(1);
	h.world.agentStatus = 'idle';
	await waitFor(() => h.world.calls.selectModel.length === 1, 'the model switch to apply');
	await waitFor(() => h.world.calls.prompt.length === 1, 'the prompt after the switch');

	assert.deepEqual(
		{ provider: h.world.calls.selectModel[0].provider, model: h.world.calls.selectModel[0].model, reasoningEffort: h.world.calls.selectModel[0].reasoningEffort },
		{ provider: 'acme', model: 'model-b', reasoningEffort: 'high' },
	);
	// The switch landed on the boundary, before this prompt was admitted.
	assert.equal(h.world.calls.prompt.length, 1);
	let snapshot = await h.state();
	assert.equal(snapshot.run.state, 'running', 'the prompt step is still waiting for its own turn');
	assert.equal(snapshot.nodes.find((node) => node.kind === 'prompt').status, 'running');

	h.turnEnd(2);
	snapshot = await waitForState(h, (value) => value.run.state === 'done', 'the flow to settle');
	assert.equal(snapshot.run.state, 'done');
	assert.equal(snapshot.nodes.find((node) => node.kind === 'prompt').status, 'completed');

	await cleanup(h);
}

//#endregion

//#region scenario 2: prompt then branch, chosen by the turn's own text

{
	const h = await makeHarness();
	const prompt = await h.command({ op: 'node.add', kind: 'prompt', text: 'run the tests' });
	const branch = await h.command({ op: 'node.add', kind: 'branch', condition: { type: 'regex', value: 'tests? passed' } });
	const ok = await h.command({ op: 'node.add', kind: 'model', provider: 'deploy', model: 'fast' });
	const fail = await h.command({ op: 'node.add', kind: 'prompt', text: 'fix the tests' });
	await h.command({ op: 'edge.add', from: prompt.node.id, port: 'next', to: branch.node.id });
	await h.command({ op: 'edge.add', from: branch.node.id, port: 'yes', to: ok.node.id });
	await h.command({ op: 'edge.add', from: branch.node.id, port: 'no', to: fail.node.id });

	await h.command({ op: 'run.start' });
	await waitFor(() => h.world.calls.prompt.length === 1, 'the prompt to be dispatched');
	h.turnStart(1);
	h.assistant(1, 'all tests passed');
	h.turnEnd(1);
	await waitFor(() => h.world.calls.selectModel.length === 1, 'the yes branch to run');

	const snapshot = await h.state();
	assert.equal(snapshot.run.state, 'done');
	assert.equal(snapshot.nodes.find((node) => node.id === branch.node.id).status, 'completed');
	assert.equal(snapshot.nodes.find((node) => node.id === fail.node.id).status, 'idle', 'the no branch is untouched');
	assert.equal(snapshot.outcome.text, 'all tests passed');

	await cleanup(h);
}

//#endregion

//#region scenario 3: a loop that exits on a model marker, bounded by max iterations

{
	const h = await makeHarness();
	const loop = await h.command({ op: 'node.add', kind: 'loop', maxIterations: 3, condition: { type: 'marker', value: '[[FLOW:DONE]]' } });
	const work = await h.command({ op: 'node.add', kind: 'prompt', text: 'keep improving' });
	const done = await h.command({ op: 'node.add', kind: 'model', provider: 'final', model: 'model' });
	await h.command({ op: 'edge.add', from: loop.node.id, port: 'again', to: work.node.id });
	await h.command({ op: 'edge.add', from: work.node.id, port: 'next', to: loop.node.id });
	await h.command({ op: 'edge.add', from: loop.node.id, port: 'exit', to: done.node.id });

	await h.command({ op: 'run.start' });
	await waitFor(() => h.world.calls.prompt.length === 1, 'the first iteration');
	h.turnStart(1);
	h.assistant(1, 'still working');
	h.turnEnd(1);
	await waitFor(() => h.world.calls.prompt.length === 2, 'the second iteration');
	h.turnStart(2);
	h.assistant(2, 'this looks finished [[FLOW:DONE]]');
	h.turnEnd(2);
	await waitFor(() => h.world.calls.selectModel.length === 1, 'the exit edge');

	const snapshot = await h.state();
	assert.equal(snapshot.counters[loop.node.id], 2, 'two iterations were counted');
	assert.equal(snapshot.run.state, 'done');
	assert.equal(h.world.calls.prompt.length, 2);

	await cleanup(h);
}

//#endregion

//#region scenario 4: a loop stops at max iterations even without the marker

{
	const h = await makeHarness();
	const loop = await h.command({ op: 'node.add', kind: 'loop', maxIterations: 2, condition: { type: 'marker', value: '[[FLOW:DONE]]' } });
	const work = await h.command({ op: 'node.add', kind: 'prompt', text: 'never done' });
	const exit = await h.command({ op: 'node.add', kind: 'command', command: '/compact' });
	await h.command({ op: 'edge.add', from: loop.node.id, port: 'again', to: work.node.id });
	await h.command({ op: 'edge.add', from: work.node.id, port: 'next', to: loop.node.id });
	await h.command({ op: 'edge.add', from: loop.node.id, port: 'exit', to: exit.node.id });

	await h.command({ op: 'run.start' });
	await waitFor(() => h.world.calls.prompt.length === 1, 'first iteration');
	h.turnStart(1);
	h.assistant(1, 'nope');
	h.turnEnd(1);
	await waitFor(() => h.world.calls.prompt.length === 2, 'second iteration');
	h.turnStart(2);
	h.assistant(2, 'still nope');
	h.turnEnd(2);
	await waitFor(() => h.world.calls.command.length === 1, 'the exit edge after the cap');

	const snapshot = await h.state();
	assert.equal(snapshot.counters[loop.node.id], 2);
	assert.equal(snapshot.nodes.find((node) => node.id === exit.node.id).status, 'completed');
	assert.equal(h.world.calls.command[0].line, '/compact');

	await cleanup(h);
}

//#endregion

//#region scenario 5: checkpoint waits for a human, then continues

{
	const h = await makeHarness();
	const prompt = await h.command({ op: 'node.add', kind: 'prompt', text: 'draft the plan' });
	const gate = await h.command({ op: 'node.add', kind: 'checkpoint', note: 'review before deploy' });
	const deploy = await h.command({ op: 'node.add', kind: 'model', provider: 'deploy', model: 'model' });
	await h.command({ op: 'edge.add', from: prompt.node.id, port: 'next', to: gate.node.id });
	await h.command({ op: 'edge.add', from: gate.node.id, port: 'next', to: deploy.node.id });

	await h.command({ op: 'run.start' });
	await waitFor(() => h.world.calls.prompt.length === 1, 'the draft prompt');
	h.turnEnd(1);
	await waitForState(h, (value) => value.run.state === 'waiting', 'the checkpoint to wait');
	const waiting = await h.state();
	assert.equal(waiting.run.state, 'waiting');
	assert.equal(h.world.calls.selectModel.length, 0, 'nothing runs past a waiting checkpoint');

	await h.command({ op: 'run.resume' });
	await waitFor(() => h.world.calls.selectModel.length === 1, 'the step after the checkpoint');
	assert.equal((await h.state()).run.state, 'done');

	await cleanup(h);
}

//#endregion

//#region scenario 6: attachments travel with the prompt; packages round-trip

{
	const h = await makeHarness();
	const prompt = await h.command({ op: 'node.add', kind: 'prompt', text: 'look at this' });
	await h.command({ op: 'node.attach', id: prompt.node.id, kind: 'image', data: Buffer.from('fake-image-bytes').toString('base64'), name: 'shot.png', mediaType: 'image/png' });
	await h.command({ op: 'node.attach', id: prompt.node.id, kind: 'file', data: Buffer.from('fake-file-bytes').toString('base64'), name: 'log.txt' });

	const withAttachments = await h.state();
	const node = withAttachments.nodes.find((candidate) => candidate.id === prompt.node.id);
	assert.equal(node.attachments.length, 2);
	assert.equal(node.attachments[0].type, 'image');

	await h.command({ op: 'run.start' });
	await waitFor(() => h.world.calls.prompt.length === 1, 'the prompt with attachments');
	const content = h.world.calls.prompt[0].content;
	assert.equal(content[0].type, 'text');
	assert.equal(content[1].type, 'image');
	assert.equal(content[2].type, 'file');
	h.turnEnd(1);

	// Package round-trip: save, list, load into a fresh session graph, delete.
	const saved = await (await h.call(ROUTES.packages, { method: 'POST', body: { op: 'save', sessionId: SESSION, name: 'demo', description: 'two steps' } })).json;
	assert.equal(saved.ok, true);
	const listed = await (await h.call(ROUTES.packages)).json;
	assert.ok(listed.value.some((item) => item.id === saved.value.id));

	await h.command({ op: 'graph.clear' });
	assert.equal((await h.state()).nodes.length, 0);
	const loaded = await (await h.call(ROUTES.packages, { method: 'POST', body: { op: 'load', sessionId: SESSION, id: saved.value.id } })).json;
	assert.equal(loaded.ok, true);
	assert.equal(loaded.value.nodes.length, 1);

	const removed = await (await h.call(ROUTES.packages, { method: 'POST', body: { op: 'delete', sessionId: SESSION, id: saved.value.id } })).json;
	assert.equal(removed.ok, true);
	const after = await (await h.call(ROUTES.packages)).json;
	assert.equal(after.value.some((item) => item.id === saved.value.id), false);

	await cleanup(h);
}

//#endregion

//#region scenario 7: guards (ports, editing during a run, bad input)

{
	const h = await makeHarness();
	const prompt = await h.command({ op: 'node.add', kind: 'prompt', text: 'hello' });
	const model = await h.command({ op: 'node.add', kind: 'model', provider: 'p', model: 'm' });

	const badPort = await h.call(ROUTES.command, {
		method: 'POST',
		body: { sessionId: SESSION, op: 'edge.add', from: prompt.node.id, port: 'yes', to: model.node.id },
	});
	assert.equal(badPort.status, 400);
	assert.equal(badPort.json.error.code, 'invalid-port');

	await h.command({ op: 'edge.add', from: prompt.node.id, port: 'next', to: model.node.id });
	await h.command({ op: 'run.start' });
	const locked = await h.call(ROUTES.command, {
		method: 'POST',
		body: { sessionId: SESSION, op: 'node.add', kind: 'prompt', text: 'nope' },
	});
	assert.equal(locked.status, 400);
	assert.equal(locked.json.error.code, 'run-active');

	const missing = await h.call(ROUTES.command, { method: 'POST', body: { op: 'node.add', kind: 'prompt' } });
	assert.equal(missing.status, 400);
	assert.equal(missing.json.error.code, 'invalid-session');

	const unknown = await h.call(ROUTES.command, { method: 'POST', body: { sessionId: SESSION, op: 'nope' } });
	assert.equal(unknown.status, 400);
	assert.equal(unknown.json.error.code, 'invalid-command');

	const catalog = await h.call(ROUTES.models);
	assert.equal(catalog.status, 200);

	await cleanup(h);
}

//#endregion

//#region scenario 8: the graph is persisted and reloads

{
	const h = await makeHarness();
	const prompt = await h.command({ op: 'node.add', kind: 'prompt', text: 'persisted step' });
	const model = await h.command({ op: 'node.add', kind: 'model', provider: 'p', model: 'm2' });
	await h.command({ op: 'edge.add', from: prompt.node.id, port: 'next', to: model.node.id });

	const stored = JSON.parse(await readFile(path.join(h.world.stateDir, 'sessions', `${SESSION}.json`), 'utf8'));
	assert.equal(stored.version, 2);
	assert.equal(stored.nodes.length, 2);
	assert.equal(stored.edges.length, 1);

	await cleanup(h);
}

//#endregion

console.log('dsh-flow-queue offline check: all scenarios passed');
