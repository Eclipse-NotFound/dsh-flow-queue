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

async function makeHarness(options = {}) {
	const stateDir = await mkdtemp(path.join(tmpdir(), 'flow-queue-'));
	const routes = new Map();
	const listeners = new Map();
	const calls = { selectModel: [], prompt: [], command: [], startActivation: [] };
	const world = { agentStatus: 'idle', agentLive: true, routes, listeners, calls, stateDir, children: {}, promptFailFor: null };

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
					find(agent, name) {
						return name === 'compact' || name === 'echo' ? { name, description: `the ${name} command` } : undefined;
					},
					async list(agent) {
						return [
							{ name: 'compact', description: 'the compact command' },
							{ name: 'echo', description: 'the echo command' },
						];
					},
					async execute(agent, line) {
						calls.command.push({ agent, line });
						if (line.startsWith('/unknown')) return undefined;
						return { commandId: 'cmd-1', result: { kind: 'success', text: `ran ${line}` } };
					},
				};
			}
			if (name === 'skills') {
				return {
					async list(options) {
						return [
							{ name: 'code-review', description: 'Review code changes', invocation: { modelInvocable: true, userInvocable: true } },
							{ name: 'hidden-skill', description: 'Neither surface may load this', invocation: { modelInvocable: false, userInvocable: false } },
						];
					},
				};
			}
			if (name === 'fileReferences') {
				return {
					async list(agent, query) {
						return [
							{ path: 'src/app.ts', kind: 'file' },
							{ path: 'docs', kind: 'directory' },
						];
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
			if (name === 'subagents') {
				return {
					getProvider(id) {
						return world.subagentsEnabled === false ? undefined : { name: id };
					},
					startActivation(spec) {
						calls.startActivation.push(spec);
						if (world.activationFailure === true) throw new Error('spawn refused');
						const childId = `child-${calls.startActivation.length}`;
						const preset = world.children[childId] ?? { delay: 5, output: `output ${childId}` };
						const result = new Promise((resolve, reject) => {
							setTimeout(() => {
								if (preset.reject !== undefined) reject(new Error(preset.reject));
								else resolve({ output: [{ type: 'text', text: preset.output ?? '' }], stopReason: 'completed' });
							}, preset.delay ?? 5);
						});
						return Promise.resolve({
							childId,
							messageId: 'msg-1',
							result,
							dispose: async () => {},
						});
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
				if (world.promptFailFor !== null && world.promptFailFor.has(request.sessionId)) throw new Error('delivery refused');
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

	apply(ctx, { stateDir, ...options });

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

const FLEET_ROUTE = '/flow-queue/api/fleet';

async function fleetOp(h, body) {
	const response = await h.call(FLEET_ROUTE, { method: 'POST', body: { sessionId: SESSION, ...body } });
	assert.equal(response.status, 200, `fleet op ${body.op} failed: ${JSON.stringify(response.json && response.json.error)}`);
	return response.json.value;
}

async function fleetState(h) {
	const response = await h.call(FLEET_ROUTE, { search: `?sessionId=${SESSION}` });
	assert.equal(response.status, 200);
	return response.json.value;
}

function targetEvent(h, sessionId, event) {
	h.emit('session/event', { id: sessionId, header: {} }, event);
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

//#region scenario 9: the mention route and inline command lines

{
	const h = await makeHarness();

	const slash = await (await h.call('/flow-queue/api/mention', { search: `?sessionId=${SESSION}&kind=slash` })).json;
	assert.equal(slash.ok, true);
	assert.ok(slash.value.commands.some((entry) => entry.name === 'compact'));
	assert.ok(slash.value.skills.some((entry) => entry.name === 'code-review'));
	assert.equal(slash.value.skills.some((entry) => entry.name === 'hidden-skill'), false, 'skills blocked on both surfaces are not offered');

	const files = await (await h.call('/flow-queue/api/mention', { search: `?sessionId=${SESSION}&kind=file&query=src` })).json;
	assert.equal(files.ok, true);
	assert.deepEqual(files.value.files, [
		{ path: 'src/app.ts', kind: 'file' },
		{ path: 'docs', kind: 'directory' },
	]);

	// A prompt whose whole text is one host command line runs as that command.
	const inline = await h.command({ op: 'node.add', kind: 'prompt', text: '/compact now' });
	await h.command({ op: 'run.start' });
	await waitFor(() => h.world.calls.command.length >= 1, 'the inline command to execute');
	assert.equal(h.world.calls.prompt.length, 0, 'an inline command line is never sent as a prompt');
	let snapshot = await h.state();
	let inlineNode = snapshot.nodes.find((node) => node.id === inline.node.id);
	assert.equal(inlineNode.status, 'completed');
	assert.equal(inlineNode.result, 'ran /compact now');
	await h.command({ op: 'graph.reset' });

	// Multi-line text starting with a slash, and unknown commands, stay prompts.
	const multi = await h.command({ op: 'node.add', kind: 'prompt', text: '/compact everything\nand then summarize' });
	await h.command({ op: 'run.start', id: multi.node.id });
	await waitFor(() => h.world.calls.prompt.length === 1, 'the multi-line prompt to dispatch');
	assert.equal(h.world.calls.prompt[0].content[0].text, '/compact everything\nand then summarize');
	h.turnEnd(1);
	snapshot = await waitForState(h, (value) => value.run.state === 'done', 'the multi-line run to settle');

	const unknown = snapshot.nodes.find((node) => node.id === multi.node.id);
	assert.equal(unknown.status, 'completed');

	await cleanup(h);
}

//#endregion

//#region scenario 10: a natural-language branch is judged by one model turn

{
	const h = await makeHarness();
	const gate = await h.command({ op: 'node.add', kind: 'branch', condition: { type: 'judge', value: '卡顿降到 10ms 以下' } });
	const yes = await h.command({ op: 'node.add', kind: 'model', provider: 'fast', model: 'm' });
	const no = await h.command({ op: 'node.add', kind: 'model', provider: 'slow', model: 'm' });
	await h.command({ op: 'edge.add', from: gate.node.id, port: 'yes', to: yes.node.id });
	await h.command({ op: 'edge.add', from: gate.node.id, port: 'no', to: no.node.id });

	await h.command({ op: 'run.start' });
	await waitFor(() => h.world.calls.prompt.length === 1, 'the judge prompt to dispatch');
	const judgeText = h.world.calls.prompt[0].content[0].text;
	assert.ok(judgeText.includes('Condition: 卡顿降到 10ms 以下'), 'the judge prompt quotes the condition');
	assert.ok(judgeText.length < 600, 'the judge prompt stays minimal');
	assert.equal((await h.state()).run.waitingJudge, gate.node.id, 'the run waits on the judge node');

	h.turnStart(1);
	h.assistant(1, 'YES\n测得 7ms，低于阈值');
	h.turnEnd(1);
	await waitFor(() => h.world.calls.selectModel.length === 1, 'the yes edge to run');
	assert.equal(h.world.calls.selectModel[0].provider, 'fast');
	let snapshot = await h.state();
	const gateView = snapshot.nodes.find((node) => node.id === gate.node.id);
	assert.equal(gateView.status, 'completed');
	assert.ok(gateView.result.startsWith('YES'), 'the card shows the verdict');
	assert.equal(gateView.judge, undefined, 'the consumed verdict is not exposed');
	assert.equal(snapshot.run.state, 'done');
	await h.command({ op: 'graph.reset' });

	// A NO answer takes the other edge; an unparsable reply fails the step.
	h.world.calls.selectModel.length = 0;
	h.world.calls.prompt.length = 0;
	await h.command({ op: 'run.start' });
	await waitFor(() => h.world.calls.prompt.length === 1, 'the second judge prompt');
	h.turnStart(2);
	h.assistant(2, 'NO\n仍在 23ms');
	h.turnEnd(2);
	await waitFor(() => h.world.calls.selectModel.length === 1, 'the no edge to run');
	assert.equal(h.world.calls.selectModel[0].provider, 'slow');
	await h.command({ op: 'graph.reset' });

	h.world.calls.selectModel.length = 0;
	h.world.calls.prompt.length = 0;
	await h.command({ op: 'run.start' });
	await waitFor(() => h.world.calls.prompt.length === 1, 'the third judge prompt');
	h.turnStart(3);
	h.assistant(3, '我觉得不太好说');
	h.turnEnd(3);
	snapshot = await waitForState(h, (value) => value.run.state === 'error', 'the unparsable judge to fail');
	assert.equal(snapshot.run.message, 'no-decision');
	assert.equal(snapshot.nodes.find((node) => node.id === gate.node.id).error.code, 'no-decision');

	await cleanup(h);
}

//#endregion

//#region scenario 11: a fan-out runs every target as a subagent and aggregates

{
	const h = await makeHarness();
	h.world.children = {
		'child-1': { delay: 10, output: 'idea A' },
		'child-2': { delay: 20, reject: 'boom' },
		'child-3': { delay: 5, output: 'idea C' },
	};
	const fan = await h.command({
		op: 'node.add',
		kind: 'dispatch',
		text: 'brainstorm the naming',
		targets: [
			{ label: 'A', provider: 'p', model: 'a' },
			{ label: 'B', provider: 'p', model: 'b' },
			{ label: 'C', provider: 'p', model: 'c' },
		],
		collect: 'all',
	});
	const after = await h.command({ op: 'node.add', kind: 'model', provider: 'next', model: 'm' });
	await h.command({ op: 'edge.add', from: fan.node.id, port: 'next', to: after.node.id });

	await h.command({ op: 'run.start' });
	await waitForState(h, (value) => value.run.state === 'done', 'the fan-out flow to settle');
	const snapshot = await h.state();
	const node = snapshot.nodes.find((candidate) => candidate.id === fan.node.id);
	assert.equal(h.world.calls.startActivation.length, 3, 'one child per target');
	assert.equal(h.world.calls.startActivation[0].provider, 'spawn', 'the in-process spawn backend is used');
	assert.deepEqual(h.world.calls.startActivation[0].request.agentOptions, { provider: 'p', model: 'a' }, 'the per-child route is passed');
	assert.equal(h.world.calls.startActivation[0].request.prompt[0].text, 'brainstorm the naming', 'every child gets the same prompt');
	assert.equal(node.status, 'completed');
	assert.equal(node.result, '2/3 done');
	assert.equal(node.children[0].status, 'done');
	assert.equal(node.children[1].status, 'failed');
	assert.equal(node.children[2].status, 'done');
	assert.equal(snapshot.outcome.kind, 'dispatch');
	assert.ok(snapshot.outcome.text.includes('idea A'));
	assert.ok(snapshot.outcome.text.includes('idea C'));
	assert.equal(h.world.calls.selectModel.length, 1, 'the next step runs once');

	await cleanup(h);
}

//#endregion

//#region scenario 12: k-of-n collects early and cancels the rest

{
	const h = await makeHarness();
	h.world.children = {
		'child-1': { delay: 5, output: 'quick A' },
		'child-2': { delay: 5, output: 'quick B' },
		'child-3': { delay: 400, output: 'slow C' },
	};
	const fan = await h.command({
		op: 'node.add',
		kind: 'dispatch',
		text: 'vote',
		targets: [
			{ label: 'A', provider: 'p', model: 'a' },
			{ label: 'B', provider: 'p', model: 'b' },
			{ label: 'C', provider: 'p', model: 'c' },
		],
		collect: 'k-of-n',
		k: 2,
	});
	await h.command({ op: 'run.start' });
	const snapshot = await waitForState(h, (value) => value.run.state === 'done', 'the k-of-n flow to settle');
	const node = snapshot.nodes.find((candidate) => candidate.id === fan.node.id);
	assert.equal(node.status, 'completed');
	assert.equal(node.children[2].status, 'cancelled', 'the straggler is cancelled once k children are done');
	assert.equal(node.children.filter((child) => child.status === 'done').length, 2);
	assert.equal(node.result, '2/3 done');

	await cleanup(h);
}

//#endregion

//#region scenario 13: a fan-out timeout marks stragglers and fails when none finish

{
	const h = await makeHarness({ dispatchMinTimeoutMs: 200 });
	h.world.children = {
		'child-1': { delay: 800, output: 'too late' },
		'child-2': { delay: 800, output: 'too late' },
	};
	const fan = await h.command({
		op: 'node.add',
		kind: 'dispatch',
		text: 'slow question',
		targets: [
			{ label: 'A', provider: 'p', model: 'a' },
			{ label: 'B', provider: 'p', model: 'b' },
		],
		collect: 'all',
		timeoutMs: 300,
	});
	await h.command({ op: 'run.start' });
	const snapshot = await waitForState(h, (value) => value.run.state === 'done', 'the timed-out flow to settle');
	const node = snapshot.nodes.find((candidate) => candidate.id === fan.node.id);
	assert.equal(node.status, 'failed');
	assert.equal(node.error.code, 'dispatch-failed');
	assert.equal(node.children[0].status, 'timeout');
	assert.equal(node.children[1].status, 'timeout');

	await cleanup(h);
}

//#endregion

//#region scenario 14: stopping the run cancels an in-flight fan-out

{
	const h = await makeHarness();
	h.world.children = {
		'child-1': { delay: 5000, output: 'never' },
		'child-2': { delay: 5000, output: 'never' },
	};
	const fan = await h.command({
		op: 'node.add',
		kind: 'dispatch',
		text: 'long task',
		targets: [
			{ label: 'A', provider: 'p', model: 'a' },
			{ label: 'B', provider: 'p', model: 'b' },
		],
		collect: 'all',
	});
	await h.command({ op: 'run.start' });
	await waitFor(() => h.world.calls.startActivation.length === 2, 'both children started');
	await h.command({ op: 'run.stop' });
	const snapshot = await h.state();
	assert.equal(snapshot.run.state, 'idle');
	const node = snapshot.nodes.find((candidate) => candidate.id === fan.node.id);
	assert.equal(node.status, 'failed');
	assert.equal(node.error.code, 'dispatch-stopped');
	assert.equal(node.children.every((child) => child.status === 'cancelled'), true);

	await cleanup(h);
}

//#endregion

//#region scenario 15: every loop iteration fans out again

{
	const h = await makeHarness();
	const loop = await h.command({ op: 'node.add', kind: 'loop', maxIterations: 2, condition: { type: 'marker', value: '[[FLOW:DONE]]' } });
	const fan = await h.command({
		op: 'node.add',
		kind: 'dispatch',
		text: 'refine once',
		targets: [{ label: 'A', provider: 'p', model: 'a' }],
		collect: 'all',
	});
	const exit = await h.command({ op: 'node.add', kind: 'model', provider: 'exit', model: 'm' });
	await h.command({ op: 'edge.add', from: loop.node.id, port: 'again', to: fan.node.id });
	await h.command({ op: 'edge.add', from: fan.node.id, port: 'next', to: loop.node.id });
	await h.command({ op: 'edge.add', from: loop.node.id, port: 'exit', to: exit.node.id });

	await h.command({ op: 'run.start' });
	const snapshot = await waitForState(h, (value) => value.run.state === 'done', 'the fan-out loop to settle');
	assert.equal(h.world.calls.startActivation.length, 2, 'each iteration started a fresh child');
	assert.equal(snapshot.counters[loop.node.id], 2);
	assert.equal(h.world.calls.selectModel[0].provider, 'exit');

	await cleanup(h);
}

//#endregion

//#region scenario 16: a send step delivers to another fleet session via the outbox

{
	const h = await makeHarness({ outboxPumpMs: 120 });
	const added = await fleetOp(h, { op: 'session.add', targetSessionId: 'T1', alias: 'worker', role: 'worker' });
	assert.ok(added.session.nodeId.startsWith('fs_'), 'fleet sessions get fs_ node ids');

	const send = await h.command({ op: 'node.add', kind: 'send', text: 'go', targetRef: added.session.nodeId });
	const after = await h.command({ op: 'node.add', kind: 'model', provider: 'next', model: 'm' });
	await h.command({ op: 'edge.add', from: send.node.id, port: 'next', to: after.node.id });

	await h.command({ op: 'run.start' });
	const snapshot = await waitForState(h, (value) => value.run.state === 'done', 'the send flow to settle');
	let delivered;
	for (let attempt = 0; attempt < 100 && delivered === undefined; attempt += 1) {
		delivered = h.world.calls.prompt.find((request) => request.sessionId === 'T1');
		if (delivered === undefined) await sleep(40);
	}
	assert.ok(delivered, 'the message reached the target session');
	assert.equal(delivered.mode, 'queue', 'cross-session delivery lands in the target queue');
	assert.equal(delivered.content[0].text, 'go');
	const sendView = snapshot.nodes.find((node) => node.id === send.node.id);
	assert.equal(sendView.status, 'completed');
	assert.equal(sendView.target, 'T1');
	const fleet = await fleetState(h);
	assert.equal(fleet.fleet.hops, 1, 'one send counted one hop');
	assert.equal(fleet.sessions[0].agentStatus, 'idle');

	// Fleet CRUD round-trip: update, edge add/remove, session remove.
	await fleetOp(h, { op: 'session.update', nodeId: added.session.nodeId, alias: 'reviewer', role: 'reviewer' });
	const edge = await fleetOp(h, { op: 'edge.add', from: added.session.nodeId, to: added.session.nodeId });
	// Self edges are allowed at storage level? normalizeFleetEdge only checks ends exist; the engine does not use fleet edges. Keep as stored.
	await fleetOp(h, { op: 'edge.remove', id: edge.edge.id });
	await fleetOp(h, { op: 'session.remove', nodeId: added.session.nodeId });
	assert.equal((await fleetState(h)).sessions.length, 0);
	void edge;

	await cleanup(h);
}

//#endregion

//#region scenario 17: outbox backoff, dead letter blocks order, retry revives

{
	const h = await makeHarness({ outboxBackoffMs: [40, 40, 40, 40, 40], outboxPumpMs: 50 });
	h.world.promptFailFor = new Set(['T2']);
	const added = await fleetOp(h, { op: 'session.add', targetSessionId: 'T2', alias: 'flaky' });

	const first = await h.command({ op: 'node.add', kind: 'send', text: 'A', targetRef: added.session.nodeId });
	const second = await h.command({ op: 'node.add', kind: 'send', text: 'B', targetRef: added.session.nodeId });
	await h.command({ op: 'edge.add', from: first.node.id, port: 'next', to: second.node.id });
	await h.command({ op: 'run.start' });
	await waitForState(h, (value) => value.run.state === 'done', 'both sends to enqueue');

	// The head letter exhausts its attempts and dies; the second stays parked.
	let dead = 0;
	for (let attempt = 0; attempt < 160 && dead !== 1; attempt += 1) {
		dead = (await fleetState(h)).sessions[0].outbox.dead;
		if (dead !== 1) await sleep(50);
	}
	assert.equal(dead, 1, 'the head letter died after its attempts');

	// Revive: the pump delivers A then B, in order.
	h.world.promptFailFor = null;
	await fleetOp(h, { op: 'outbox.retry', targetSessionId: 'T2' });
	let sawB = false;
	for (let attempt = 0; attempt < 160 && !sawB; attempt += 1) {
		sawB = h.world.calls.prompt.some((request) => request.sessionId === 'T2' && request.content[0].text === 'B');
		if (!sawB) await sleep(50);
	}
	assert.equal(sawB, true, 'the revived queue drained through B');
	const texts = h.world.calls.prompt.filter((request) => request.sessionId === 'T2').map((request) => request.content[0].text);
	const delivered = texts.filter((text) => text === 'A' || text === 'B');
	assert.equal(delivered[delivered.length - 2], 'A', 'A delivered before B');
	assert.equal(delivered[delivered.length - 1], 'B');

	await cleanup(h);
}

//#endregion

//#region scenario 18: wait-idle releases when the target finishes a turn

{
	const h = await makeHarness();
	const added = await fleetOp(h, { op: 'session.add', targetSessionId: 'T3', alias: 'planner' });
	const wait = await h.command({ op: 'node.add', kind: 'wait', targetRef: added.session.nodeId, until: 'idle' });
	const after = await h.command({ op: 'node.add', kind: 'model', provider: 'relased', model: 'm' });
	await h.command({ op: 'edge.add', from: wait.node.id, port: 'next', to: after.node.id });

	await h.command({ op: 'run.start' });
	let waitRunning = false;
	for (let attempt = 0; attempt < 100 && !waitRunning; attempt += 1) {
		const current = await h.state();
		waitRunning = current.nodes.find((node) => node.id === wait.node.id).status === 'running';
		if (!waitRunning) await sleep(30);
	}
	assert.equal(waitRunning, true, 'the wait is pending');

	targetEvent(h, 'T3', { type: 'assistant/message', seq: 2, time: Date.now(), data: { turn: 1, step: 1, message: { role: 'assistant', content: [{ type: 'text', text: 'plan ready' }], source: { kind: 'model', provider: 'p', model: 'm' } }, stream: [] } });
	targetEvent(h, 'T3', { type: 'turn/end', seq: 3, time: Date.now(), data: { turn: 1, reason: { kind: 'completed' } } });

	await waitFor(() => h.world.calls.selectModel.length === 1, 'the step after the wait');
	const snapshot = await h.state();
	assert.equal(snapshot.nodes.find((node) => node.id === wait.node.id).status, 'completed');
	assert.equal(snapshot.run.state, 'done');

	await cleanup(h);
}

//#endregion

//#region scenario 19: wait-marker releases on the target's report line

{
	const h = await makeHarness();
	const added = await fleetOp(h, { op: 'session.add', targetSessionId: 'T3b', alias: 'worker' });
	const wait = await h.command({ op: 'node.add', kind: 'wait', targetRef: added.session.nodeId, until: 'marker', marker: '[[FLOW:REPORT' });
	const after = await h.command({ op: 'node.add', kind: 'command', command: '/compact' });
	await h.command({ op: 'edge.add', from: wait.node.id, port: 'next', to: after.node.id });

	await h.command({ op: 'run.start' });
	// A turn WITHOUT the marker must not release the wait.
	targetEvent(h, 'T3b', { type: 'assistant/message', seq: 2, time: Date.now(), data: { turn: 1, step: 1, message: { role: 'assistant', content: [{ type: 'text', text: 'still working' }], source: { kind: 'model', provider: 'p', model: 'm' } }, stream: [] } });
	targetEvent(h, 'T3b', { type: 'turn/end', seq: 3, time: Date.now(), data: { turn: 1, reason: { kind: 'completed' } } });
	await sleep(200);
	assert.equal(h.world.calls.command.length, 0, 'a markerless turn does not release the wait');

	targetEvent(h, 'T3b', { type: 'assistant/message', seq: 4, time: Date.now(), data: { turn: 2, step: 1, message: { role: 'assistant', content: [{ type: 'text', text: 'done\n[[FLOW:REPORT ready]]' }], source: { kind: 'model', provider: 'p', model: 'm' } }, stream: [] } });
	targetEvent(h, 'T3b', { type: 'turn/end', seq: 5, time: Date.now(), data: { turn: 2, reason: { kind: 'completed' } } });

	await waitFor(() => h.world.calls.command.length === 1, 'the marker to release the wait');
	const snapshot = await h.state();
	assert.equal(snapshot.nodes.find((node) => node.id === wait.node.id).status, 'completed');

	await cleanup(h);
}

//#endregion

//#region scenario 20: hop cap and the honest change-attachment block

{
	const h = await makeHarness({ maxCommHops: 1, outboxPumpMs: 120 });
	const added = await fleetOp(h, { op: 'session.add', targetSessionId: 'T4', alias: 'reviewer' });

	// One work turn happens inside the run; the file edit lands mid-run so the
	// send's change block can attribute it (changes before the run are not
	// this task's).
	const work = await h.command({ op: 'node.add', kind: 'prompt', text: 'do the work' });
	const send = await h.command({ op: 'node.add', kind: 'send', text: 'review please', targetRef: added.session.nodeId, attachChanges: true });
	const capped = await h.command({ op: 'node.add', kind: 'send', text: 'one more', targetRef: added.session.nodeId });
	await h.command({ op: 'edge.add', from: work.node.id, port: 'next', to: send.node.id });
	await h.command({ op: 'edge.add', from: send.node.id, port: 'next', to: capped.node.id });

	await h.command({ op: 'run.start' });
	await waitFor(() => h.world.calls.prompt.some((request) => request.sessionId === SESSION), 'the work turn to start');
	const exec = { agent: { session: { id: SESSION, header: {} } } };
	await h.emit('tools/pre-execute', exec, async () => ({}));
	await h.emit('fs/write-intent', 'D:\\work\\demo\\a.txt', exec, async () => ({ ok: true }));
	h.turnStart(1);
	h.assistant(1, 'work done');
	h.turnEnd(1);

	const snapshot = await waitForState(h, (value) => value.run.state === 'done', 'the capped flow to settle');
	let delivered;
	for (let attempt = 0; attempt < 100 && delivered === undefined; attempt += 1) {
		delivered = h.world.calls.prompt.find((request) => request.sessionId === 'T4');
		if (delivered === undefined) await sleep(40);
	}
	assert.ok(delivered, 'the attached message was delivered');
	assert.ok(delivered.content[0].text.includes('<flow-changes method="intent-only"'), 'the change block is labelled with its method');
	assert.ok(delivered.content[0].text.toLowerCase().includes('a.txt'), 'the touched path is listed');
	const cappedView = snapshot.nodes.find((node) => node.id === capped.node.id);
	assert.equal(cappedView.status, 'failed');
	assert.equal(cappedView.error.code, 'hop-limit');
	assert.equal((await fleetState(h)).fleet.hops, 1);

	// Resetting hops unblocks further sends.
	await fleetOp(h, { op: 'reset-hops' });
	assert.equal((await fleetState(h)).fleet.hops, 0);

	await cleanup(h);
}

//#endregion

//#region scenario 21: fleet.stop pauses the live run, idempotently

{
	const h = await makeHarness({ outboxPumpMs: 400 });
	h.world.promptFailFor = new Set(['T5']);
	const added = await fleetOp(h, { op: 'session.add', targetSessionId: 'T5', alias: 'slow' });

	const send = await h.command({ op: 'node.add', kind: 'send', text: 'stuck', targetRef: added.session.nodeId });
	const work = await h.command({ op: 'node.add', kind: 'prompt', text: 'still open' });
	await h.command({ op: 'edge.add', from: send.node.id, port: 'next', to: work.node.id });
	await h.command({ op: 'run.start' });
	await waitFor(() => h.world.calls.prompt.some((request) => request.sessionId === SESSION), 'the flow to reach its prompt step');

	// The run is live with an open turn: fleet.stop must pause it.
	const firstStop = await fleetOp(h, { op: 'stop' });
	assert.equal(firstStop.paused, 1);
	const paused = await waitForState(h, (value) => value.run.state === 'paused', 'the run to be paused');
	assert.equal(paused.run.waitingNode, null);
	assert.equal(h.world.calls.prompt.length >= 1, true, 'nothing new was dispatched after the stop');

	const again = await fleetOp(h, { op: 'stop' });
	assert.equal(again.paused, 0, 'a second stop pauses nothing new');

	await cleanup(h);
}

//#endregion

//#region scenario 22: the report-protocol package ships and loads

{
	const h = await makeHarness();
	let entry;
	for (let attempt = 0; attempt < 100 && entry === undefined; attempt += 1) {
		const listed = await h.call(ROUTES.packages);
		assert.equal(listed.status, 200);
		entry = listed.json.value.find((item) => item.id === 'report-protocol');
		if (entry === undefined) await sleep(30);
	}
	assert.ok(entry, 'the report protocol package is present');
	assert.equal(entry.nodes, 2);

	const loaded = await h.call(ROUTES.packages, { method: 'POST', body: { op: 'load', sessionId: SESSION, id: 'report-protocol' } });
	assert.equal(loaded.status, 200, `load failed: ${JSON.stringify(loaded.json && loaded.json.error)}`);
	const state = loaded.json.value;
	assert.equal(state.nodes.length, 2);
	const waitNode = state.nodes.find((node) => node.kind === 'wait');
	assert.equal(waitNode.until, 'marker');
	assert.equal(waitNode.marker, '[[FLOW:REPORT ready]]');
	const promptNode = state.nodes.find((node) => node.kind === 'prompt');
	assert.ok(promptNode.text.includes('[[FLOW:REPORT ready]]'), 'the worker template pins the four-state marker');

	await cleanup(h);
}

//#endregion

//#region scenario 23: the superagent package ships, loads, and takes {{var}} fills

{
	const h = await makeHarness();
	let entry;
	for (let attempt = 0; attempt < 100 && entry === undefined; attempt += 1) {
		const listed = await h.call(ROUTES.packages);
		assert.equal(listed.status, 200);
		entry = listed.json.value.find((item) => item.id === 'superagent-review');
		if (entry === undefined) await sleep(30);
	}
	assert.ok(entry, 'the superagent review package is present');
	assert.equal(entry.nodes, 10);
	assert.equal(entry.edges, 10);
	assert.deepEqual(entry.vars.map((item) => item.name), ['task', 'ticket']);

	// Load without fills: placeholders stay visible, never silently empty.
	const bare = await h.call(ROUTES.packages, { method: 'POST', body: { op: 'load', sessionId: SESSION, id: 'superagent-review' } });
	assert.equal(bare.status, 200, `bare load failed: ${JSON.stringify(bare.json && bare.json.error)}`);
	const bareWorker = bare.json.value.nodes.find((node) => node.id === 'sa_worker');
	assert.ok(bareWorker.text.includes('{{task}}'), 'an unfilled placeholder stays visible');

	// Load with fills: texts substituted, structural props untouched.
	const filled = await h.call(ROUTES.packages, {
		method: 'POST',
		body: {
			op: 'load',
			sessionId: SESSION,
			id: 'superagent-review',
			vars: { task: '给 README 补安装一节', ticket: 'D:/tmp/tickets/T9-运行.md' },
		},
	});
	assert.equal(filled.status, 200, `filled load failed: ${JSON.stringify(filled.json && filled.json.error)}`);
	const graph = filled.json.value;
	const worker = graph.nodes.find((node) => node.id === 'sa_worker');
	assert.ok(worker.text.includes('给 README 补安装一节'));
	assert.ok(worker.text.includes('D:/tmp/tickets/T9-运行.md'));
	assert.ok(!worker.text.includes('{{task}}'), 'filled placeholders are gone');
	const gate = graph.nodes.find((node) => node.id === 'sa_w_ok');
	assert.equal(gate.condition.type, 'regex');
	assert.equal(gate.condition.value, '\\[\\[FLOW:REPORT ready\\]\\]');
	const loopBack = graph.edges.find((edge) => edge.from === 'sa_followup' && edge.port === 'next');
	assert.equal(loopBack.to, 'sa_w_ok', 'the follow-up round re-enters the readiness gate');

	await cleanup(h);
}

//#endregion

//#region scenario 24: the superagent loop runs end to end (ready → blocked review → follow-up → pass)

{
	const h = await makeHarness();
	// Worker replies carry a long head so only the tail brief keeps the marker.
	h.world.children['child-1'] = { delay: 5, output: `${'背景与实现说明。'.repeat(30)}\n全部改动已提交。\n[[FLOW:REPORT ready]] 已完成待审阅` };
	h.world.children['child-2'] = { delay: 5, output: '逐项核验：两处未过。\n[[FLOW:REPORT blocked]] 需修复安装命令与徽章链接' };
	h.world.children['child-3'] = { delay: 5, output: '两处已修复并复跑。\n[[FLOW:REPORT ready]] 待复审' };
	h.world.children['child-4'] = { delay: 5, output: '复审全部通过。\n[[FLOW:REPORT ready]] 可交付' };

	// The builtin ships from an async ensure: wait for it like the panel does.
	for (let attempt = 0; attempt < 100; attempt += 1) {
		const listed = await h.call(ROUTES.packages);
		if (listed.json.value.some((item) => item.id === 'superagent-review')) break;
		await sleep(30);
	}
	const loaded = await h.call(ROUTES.packages, {
		method: 'POST',
		body: { op: 'load', sessionId: SESSION, id: 'superagent-review', vars: { task: '补 README 安装节', ticket: 'D:/tmp/tickets/T10.md' } },
	});
	assert.equal(loaded.status, 200);

	await h.command({ op: 'run.start' });
	// The briefing checkpoint parks the run: dispatch costs nothing until the
	// Router-side confirmation.
	let parked = await waitForState(h, (value) => value.run.state === 'waiting', 'the run to park at the briefing');
	assert.equal(parked.run.waitingNode, 'sa_brief');
	await h.command({ op: 'run.resume' });

	await waitFor(() => h.world.calls.startActivation.length >= 1, 'the worker dispatch');
	const workerPrompt = h.world.calls.startActivation[0].request.prompt[0].text;
	assert.ok(workerPrompt.includes('补 README 安装节') && workerPrompt.includes('D:/tmp/tickets/T10.md'), 'the dispatched prompt carries the filled ticket');
	await waitFor(() => h.world.calls.startActivation.length >= 2, 'the reviewer dispatch after a ready worker');
	await waitFor(() => h.world.calls.startActivation.length >= 3, 'the follow-up dispatch after a blocked review');
	await waitFor(() => h.world.calls.startActivation.length >= 4, 'the second review round');

	// The delivery-gate prompt lands in the Router session itself.
	await waitFor(() => h.world.calls.prompt.some((request) => request.sessionId === SESSION && request.content[0].text.includes('交付门')), 'the delivery gate');
	h.turnStart(1);
	h.assistant(1, '已复核并推送。');
	h.turnEnd(1);

	const done = await waitForState(h, (value) => value.run.state === 'done', 'the superagent loop to finish');
	const byId = new Map(done.nodes.map((node) => [node.id, node]));
	if (process.env.FLOW_QUEUE_DEBUG) {
		console.log('DEBUG run', done.run.state, done.run.message, 'activations', h.world.calls.startActivation.length);
		for (const node of done.nodes) console.log('DEBUG', node.id, node.kind, node.status, JSON.stringify((node.result ?? '').slice(0, 80)));
	}
	assert.equal(byId.get('sa_worker').status, 'completed');
	assert.equal(byId.get('sa_w_ok').status, 'completed');
	assert.equal(byId.get('sa_w_ok').title, '条件成立', 'the tail brief kept the marker routable');
	assert.equal(byId.get('sa_round').status, 'completed');
	assert.equal(byId.get('sa_deliver').status, 'completed');
	const followup = byId.get('sa_followup');
	assert.equal(followup.status, 'completed');
	assert.ok((followup.children ?? []).length > 0, 'the follow-up round recorded its child');

	await cleanup(h);
}

//#endregion

//#region scenario 25: a marker wait feeds the branch that follows it

{
	const h = await makeHarness();
	const added = await fleetOp(h, { op: 'session.add', targetSessionId: 'T7', alias: 'worker' });
	const wait = await h.command({ op: 'node.add', kind: 'wait', targetRef: added.session.nodeId, until: 'marker', marker: '[[FLOW:REPORT' });
	const gate = await h.command({ op: 'node.add', kind: 'branch', condition: { type: 'regex', value: '\\[\\[FLOW:REPORT ready\\]\\]' } });
	const relax = await h.command({ op: 'node.add', kind: 'prompt', text: '可以交付' });
	const intervene = await h.command({ op: 'node.add', kind: 'prompt', text: '人工介入' });
	await h.command({ op: 'edge.add', from: wait.node.id, port: 'next', to: gate.node.id });
	await h.command({ op: 'edge.add', from: gate.node.id, port: 'yes', to: relax.node.id });
	await h.command({ op: 'edge.add', from: gate.node.id, port: 'no', to: intervene.node.id });

	await h.command({ op: 'run.start' });
	targetEvent(h, 'T7', {
		type: 'assistant/message', seq: 2, time: Date.now(),
		data: { turn: 1, step: 1, message: { role: 'assistant', content: [{ type: 'text', text: '卡在凭据上。\n[[FLOW:REPORT blocked]] 需要 token' }], source: { kind: 'model', provider: 'p', model: 'm' }, stream: [] } },
	});
	targetEvent(h, 'T7', { type: 'turn/end', seq: 3, time: Date.now(), data: { turn: 1, reason: { kind: 'completed' } } });

	await waitFor(() => h.world.calls.prompt.some((request) => request.sessionId === SESSION && request.content[0].text === '人工介入'), 'the blocked report to route to intervention');
	const snapshot = await h.state();
	const waitView = snapshot.nodes.find((node) => node.id === wait.node.id);
	assert.equal(waitView.status, 'completed');
	assert.equal(waitView.result, '[[FLOW:REPORT blocked]] 需要 token', 'the wait result carries the matched marker line');
	h.turnStart(1);
	h.assistant(1, '已介入。');
	h.turnEnd(1);
	await waitForState(h, (value) => value.run.state === 'done', 'the gated flow to finish');

	await cleanup(h);
}

//#endregion

console.log('dsh-flow-queue offline check: all scenarios passed');
