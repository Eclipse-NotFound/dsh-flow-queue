// dsh-flow-queue — host half (v2: flow graph).
//
// One flow graph per Session, executed only at turn boundaries. The rule that
// the plugin exists for is unchanged: nothing that changes the Session's model
// or injects a prompt may land while a turn is live, so an in-flight request
// and all of its automatic retries keep the model they started with.
//
// Wire surface (same-origin, Host-trust fenced like every plugin route):
//   GET  /flow-queue/api/state?sessionId=…   graph + run snapshot
//   GET  /flow-queue/api/models              routable model catalog
//   GET  /flow-queue/api/packages            saved flow packages
//   POST /flow-queue/api/command             graph / run / attachment mutations
//   POST /flow-queue/api/packages            save / load / delete a package

import { randomUUID } from 'node:crypto';
import { mkdir, readdir, readFile, rename, rm, writeFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import path from 'node:path';

const PREFIX = '/flow-queue/api';
const STATE_PATH = `${PREFIX}/state`;
const MODELS_PATH = `${PREFIX}/models`;
const COMMAND_PATH = `${PREFIX}/command`;
const PACKAGES_PATH = `${PREFIX}/packages`;

const STORE_VERSION = 2;
const MAX_BODY_BYTES = 24 * 1024 * 1024;
const MAX_PROMPT_CHARS = 20000;
const MAX_NODES = 200;
const MAX_EDGES = 400;
const MAX_IDENT_CHARS = 200;
const MAX_ATTACHMENTS_PER_NODE = 8;
const DISPATCH_WATCHDOG_MS = 30000;
const SETTLE_MS = 80;
const MAX_LOOP_EXECUTIONS = 500;

const NODE_STATUSES = new Set(['idle', 'running', 'completed', 'failed', 'skipped', 'waiting']);
const NODE_KINDS = new Set(['prompt', 'model', 'command', 'checkpoint', 'branch', 'loop']);
const PORTS = {
	prompt: ['next'],
	model: ['next'],
	command: ['next'],
	checkpoint: ['next'],
	branch: ['yes', 'no'],
	loop: ['again', 'exit'],
};
const CONDITION_TYPES = new Set(['always', 'regex', 'contains', 'marker', 'tool-failed', 'last-failed']);

export const inject = ['webServer', 'connection', 'sessionController'];

function resolveBaseDir(config) {
	if (typeof config.stateDir === 'string' && config.stateDir !== '') return path.resolve(config.stateDir);
	const profileDir = process.env.DSH_PROFILE_DIR;
	if (typeof profileDir === 'string' && profileDir !== '') return path.join(profileDir, 'flow-queue');
	return path.join(homedir(), '.dsh', 'flow-queue');
}

function writeJson(response, status, body) {
	const payload = JSON.stringify(body);
	response.writeHead(status, {
		'content-type': 'application/json; charset=utf-8',
		'cache-control': 'no-store',
	});
	response.end(payload);
}

function fail(code, message) {
	return { code, message: typeof message === 'string' ? message.slice(0, 400) : '' };
}

function singleLine(value, max = MAX_IDENT_CHARS) {
	if (typeof value !== 'string') return '';
	const trimmed = value.trim();
	if (trimmed === '' || trimmed.length > max) return '';
	return trimmed;
}

function newId(prefix = 'n') {
	return `${prefix}_${randomUUID().replace(/-/g, '').slice(0, 18)}`;
}

function readBody(request) {
	return new Promise((resolve, reject) => {
		const chunks = [];
		let size = 0;
		request.on('data', (chunk) => {
			size += chunk.length;
			if (size > MAX_BODY_BYTES) {
				reject(new Error('request body too large'));
				request.destroy();
				return;
			}
			chunks.push(chunk);
		});
		request.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
		request.on('error', reject);
	});
}

function textOfBlocks(content) {
	if (!Array.isArray(content)) return '';
	const parts = [];
	for (const block of content) {
		if (block !== null && typeof block === 'object' && block.type === 'text' && typeof block.text === 'string') {
			parts.push(block.text);
		}
	}
	return parts.join('\n').trim();
}

function conditionOf(input) {
	const source = input !== null && typeof input === 'object' ? input : {};
	const type = CONDITION_TYPES.has(source.type) ? source.type : 'always';
	const value = typeof source.value === 'string' ? source.value.slice(0, 500) : '';
	return { type, value, negate: source.negate === true };
}

function emptyRun() {
	return {
		state: 'idle',
		cursor: null,
		waitingNode: null,
		steps: 0,
		message: '',
		startedAt: 0,
		endedAt: 0,
	};
}

function emptyGraph(sessionId) {
	return {
		sessionId,
		entry: null,
		nodes: [],
		edges: [],
		run: emptyRun(),
		counters: {},
		outcome: { kind: 'none', text: '', ok: true, toolErrors: [] },
		pumping: false,
		lastTurn: undefined,
	};
}

function normalizeNode(input) {
	if (input === null || typeof input !== 'object' || !NODE_KINDS.has(input.kind)) return undefined;
	const kind = input.kind;
	const node = {
		id: typeof input.id === 'string' && /^[A-Za-z0-9_-]{1,64}$/.test(input.id) ? input.id : newId(),
		kind,
		x: Number.isFinite(input.x) ? Math.round(input.x) : 40,
		y: Number.isFinite(input.y) ? Math.round(input.y) : 40,
		status: NODE_STATUSES.has(input.status) ? input.status : 'idle',
		createdAt: Number.isFinite(input.createdAt) ? input.createdAt : Date.now(),
	};
	if (Number.isFinite(input.startedAt)) node.startedAt = input.startedAt;
	if (Number.isFinite(input.endedAt)) node.endedAt = input.endedAt;
	if (Number.isFinite(input.turn)) node.turn = input.turn;
	if (typeof input.usedModel === 'string') node.usedModel = input.usedModel.slice(0, MAX_IDENT_CHARS);
	if (typeof input.title === 'string') node.title = input.title.slice(0, 200);
	if (input.error !== null && typeof input.error === 'object') {
		node.error = fail(typeof input.error.code === 'string' ? input.error.code : 'unknown', typeof input.error.message === 'string' ? input.error.message : '');
	}

	switch (kind) {
		case 'prompt': {
			node.text = typeof input.text === 'string' ? input.text.slice(0, MAX_PROMPT_CHARS) : '';
			node.attachments = Array.isArray(input.attachments)
				? input.attachments
					.slice(0, MAX_ATTACHMENTS_PER_NODE)
					.map((item) => normalizeAttachment(item))
					.filter((item) => item !== undefined)
				: [];
			if (typeof input.provider === 'string' && input.provider !== '') node.provider = input.provider.slice(0, MAX_IDENT_CHARS);
			if (typeof input.model === 'string' && input.model !== '') node.model = input.model.slice(0, MAX_IDENT_CHARS);
			if (typeof input.modelLabel === 'string' && input.modelLabel !== '') node.modelLabel = input.modelLabel.slice(0, MAX_IDENT_CHARS);
			if (typeof input.reasoningEffort === 'string' && input.reasoningEffort !== '') node.reasoningEffort = input.reasoningEffort.slice(0, 64);
			break;
		}
		case 'model': {
			node.provider = typeof input.provider === 'string' ? input.provider.slice(0, MAX_IDENT_CHARS) : '';
			node.model = typeof input.model === 'string' ? input.model.slice(0, MAX_IDENT_CHARS) : '';
			node.modelLabel = typeof input.modelLabel === 'string' ? input.modelLabel.slice(0, MAX_IDENT_CHARS) : '';
			if (typeof input.reasoningEffort === 'string' && input.reasoningEffort !== '') node.reasoningEffort = input.reasoningEffort.slice(0, 64);
			break;
		}
		case 'command': {
			node.command = typeof input.command === 'string' ? input.command.slice(0, 2000) : '';
			node.result = typeof input.result === 'string' ? input.result.slice(0, 4000) : '';
			break;
		}
		case 'checkpoint': {
			node.note = typeof input.note === 'string' ? input.note.slice(0, 500) : '';
			break;
		}
		case 'branch': {
			node.condition = conditionOf(input.condition);
			break;
		}
		case 'loop': {
			node.condition = conditionOf(input.condition);
			const max = Number.isFinite(input.maxIterations) ? Math.round(input.maxIterations) : 5;
			node.maxIterations = Math.min(Math.max(max, 1), 100);
			break;
		}
		default:
			break;
	}
	return node;
}

function normalizeAttachment(input) {
	if (input === null || typeof input !== 'object') return undefined;
	const type = input.type === 'file' ? 'file' : input.type === 'image' ? 'image' : undefined;
	if (type === undefined) return undefined;
	const ref = input.attachment;
	if (ref === null || typeof ref !== 'object' || typeof ref.attachmentId !== 'string') return undefined;
	const attachment = { attachmentId: ref.attachmentId, name: typeof ref.name === 'string' ? ref.name.slice(0, 200) : '', bytes: Number.isFinite(ref.bytes) ? ref.bytes : 0 };
	if (type === 'image') {
		attachment.mediaType = typeof ref.mediaType === 'string' ? ref.mediaType : 'image/png';
		attachment.width = Number.isFinite(ref.width) ? ref.width : 0;
		attachment.height = Number.isFinite(ref.height) ? ref.height : 0;
	}
	return { type, attachment };
}

function normalizeEdge(input) {
	if (input === null || typeof input !== 'object') return undefined;
	if (typeof input.from !== 'string' || typeof input.to !== 'string') return undefined;
	return {
		id: typeof input.id === 'string' && /^[A-Za-z0-9_-]{1,64}$/.test(input.id) ? input.id : newId('e'),
		from: input.from,
		port: typeof input.port === 'string' ? input.port : 'next',
		to: input.to,
	};
}

export function apply(ctx, config = {}) {
	const baseDir = resolveBaseDir(config);
	const graphDir = path.join(baseDir, 'sessions');
	const packageDir = path.join(baseDir, 'packages');

	const graphs = new Map();
	const watchdogs = new Map();
	const timers = new Set();
	const writeChains = new Map();

	function track(handle) {
		timers.add(handle);
		return handle;
	}

	function later(operation, delayMs) {
		const handle = setTimeout(() => {
			timers.delete(handle);
			try {
				Promise.resolve(operation()).catch(() => {});
			} catch {
				/* scheduled work must never throw into the timer loop */
			}
		}, delayMs);
		return track(handle);
	}

	function clearWatchdog(nodeId) {
		const handle = watchdogs.get(nodeId);
		if (handle === undefined) return;
		clearTimeout(handle);
		timers.delete(handle);
		watchdogs.delete(nodeId);
	}

	ctx.effect(
		() => () => {
			for (const handle of timers) clearTimeout(handle);
			timers.clear();
			watchdogs.clear();
		},
		'dsh-flow-queue: owned timers',
	);

	//#region storage

	function graphFile(sessionId) {
		const safe = String(sessionId).replace(/[^A-Za-z0-9_-]/g, '_').slice(0, 128);
		return path.join(graphDir, `${safe}.json`);
	}

	function serializeGraph(graph) {
		return JSON.stringify({
			version: STORE_VERSION,
			sessionId: graph.sessionId,
			entry: graph.entry,
			nodes: graph.nodes,
			edges: graph.edges,
			run: graph.run,
			counters: graph.counters,
			outcome: graph.outcome,
			updatedAt: Date.now(),
		});
	}

	/** v1 stored a flat prompt/model queue; convert it to a linear graph. */
	function migrateLegacy(parsed) {
		const legacyNodes = Array.isArray(parsed.nodes) ? parsed.nodes : [];
		const nodes = [];
		const edges = [];
		let previous;
		for (const item of legacyNodes) {
			if (item === null || typeof item !== 'object') continue;
			const kind = item.type === 'model_switch' ? 'model' : 'prompt';
			const node = normalizeNode({
				...item,
				kind,
				x: 60,
				y: 60 + nodes.length * 130,
				status: item.status === 'completed' ? 'completed' : item.status === 'failed' ? 'failed' : 'idle',
			});
			if (node === undefined) continue;
			nodes.push(node);
			if (previous !== undefined) edges.push({ id: newId('e'), from: previous.id, port: 'next', to: node.id });
			previous = node;
		}
		return { entry: nodes.length > 0 ? nodes[0].id : null, nodes, edges };
	}

	function normalizeStored(parsed, sessionId) {
		const graph = emptyGraph(sessionId);
		if (parsed === null || typeof parsed !== 'object') return graph;
		const source = parsed.version === 2 ? parsed : migrateLegacy(parsed);
		if (Array.isArray(source.nodes)) {
			graph.nodes = source.nodes.slice(0, MAX_NODES).map(normalizeNode).filter((node) => node !== undefined);
		}
		const ids = new Set(graph.nodes.map((node) => node.id));
		if (Array.isArray(source.edges)) {
			graph.edges = source.edges
				.slice(0, MAX_EDGES)
				.map(normalizeEdge)
				.filter((edge) => edge !== undefined && ids.has(edge.from) && ids.has(edge.to));
		}
		if (typeof source.entry === 'string' && ids.has(source.entry)) graph.entry = source.entry;
		if (source.run !== null && typeof source.run === 'object') {
			const run = source.run;
			graph.run = {
				...emptyRun(),
				state: typeof run.state === 'string' ? run.state : 'idle',
				cursor: typeof run.cursor === 'string' && ids.has(run.cursor) ? run.cursor : null,
				waitingNode: typeof run.waitingNode === 'string' && ids.has(run.waitingNode) ? run.waitingNode : null,
				steps: Number.isFinite(run.steps) ? run.steps : 0,
				message: typeof run.message === 'string' ? run.message.slice(0, 400) : '',
				startedAt: Number.isFinite(run.startedAt) ? run.startedAt : 0,
				endedAt: Number.isFinite(run.endedAt) ? run.endedAt : 0,
			};
			// A run that was live when the process died must not silently resume.
			if (graph.run.state === 'running') {
				graph.run.state = 'paused';
				graph.run.message = 'interrupted';
			}
		}
		if (source.counters !== null && typeof source.counters === 'object') {
			for (const key of Object.keys(source.counters)) {
				const value = source.counters[key];
				if (Number.isFinite(value)) graph.counters[key] = Math.max(0, Math.round(value));
			}
		}
		if (source.outcome !== null && typeof source.outcome === 'object') {
			const outcome = source.outcome;
			graph.outcome = {
				kind: typeof outcome.kind === 'string' ? outcome.kind : 'none',
				text: typeof outcome.text === 'string' ? outcome.text.slice(0, 20000) : '',
				ok: outcome.ok !== false,
				toolErrors: Array.isArray(outcome.toolErrors) ? outcome.toolErrors.slice(0, 20).map(String) : [],
			};
		}
		// Nodes that claim to be running have no live turn behind them any more.
		for (const node of graph.nodes) {
			if (node.status === 'running' || node.status === 'waiting') {
				node.status = 'failed';
				node.error = fail('interrupted', 'the Harness process restarted while this step was running');
				if (node.endedAt === undefined) node.endedAt = Date.now();
			}
		}
		return graph;
	}

	async function ensureGraph(sessionId) {
		const existing = graphs.get(sessionId);
		if (existing !== undefined) return existing;
		let graph = emptyGraph(sessionId);
		try {
			const raw = await readFile(graphFile(sessionId), 'utf8');
			graph = normalizeStored(JSON.parse(raw), sessionId);
		} catch {
			/* no stored graph yet, or an unreadable one: start clean */
		}
		graphs.set(sessionId, graph);
		return graph;
	}

	function persist(sessionId, graph) {
		const file = graphFile(sessionId);
		const payload = serializeGraph(graph);
		const previous = writeChains.get(sessionId) ?? Promise.resolve();
		const next = previous
			.then(async () => {
				await mkdir(path.dirname(file), { recursive: true });
				const temporary = `${file}.tmp`;
				await writeFile(temporary, payload, 'utf8');
				await rename(temporary, file);
			})
			.catch(() => {});
		writeChains.set(sessionId, next);
		return next;
	}

	//#endregion

	//#region snapshot

	function publicAttachment(attachment) {
		const view = {
			type: attachment.type,
			attachment: {
				attachmentId: attachment.attachment.attachmentId,
				name: attachment.attachment.name,
				bytes: attachment.attachment.bytes,
			},
		};
		if (attachment.type === 'image') {
			view.attachment.mediaType = attachment.attachment.mediaType;
			view.attachment.width = attachment.attachment.width;
			view.attachment.height = attachment.attachment.height;
		}
		return view;
	}

	function publicNode(node) {
		const view = { ...node };
		if (Array.isArray(node.attachments)) view.attachments = node.attachments.map(publicAttachment);
		if (node.startedAt !== undefined && node.endedAt !== undefined) view.durationMs = node.endedAt - node.startedAt;
		return view;
	}

	function publicState(graph) {
		return {
			version: STORE_VERSION,
			sessionId: graph.sessionId,
			entry: graph.entry,
			nodes: graph.nodes.map(publicNode),
			edges: graph.edges,
			run: graph.run,
			counters: graph.counters,
			outcome: graph.outcome,
		};
	}

	//#endregion

	//#region turn boundary

	function isIdle(sessionId) {
		const agents = ctx.get('agents');
		const agent = agents === undefined ? undefined : agents.get(sessionId);
		if (agent === undefined || agent === null) return true;
		return agent.status !== 'running';
	}

	function findNode(graph, id) {
		return graph.nodes.find((node) => node.id === id);
	}

	function followEdge(graph, fromId, port) {
		const edge = graph.edges.find((candidate) => candidate.from === fromId && candidate.port === port);
		return edge === undefined ? undefined : edge.to;
	}

	function entryNode(graph) {
		if (graph.entry !== null) {
			const explicit = findNode(graph, graph.entry);
			if (explicit !== undefined) return explicit;
		}
		const targets = new Set(graph.edges.map((edge) => edge.to));
		const roots = graph.nodes.filter((node) => !targets.has(node.id));
		if (roots.length > 0) return roots[0];
		return graph.nodes[0];
	}

	function runningPromptNode(graph) {
		if (graph.run.waitingNode === null) return undefined;
		const node = findNode(graph, graph.run.waitingNode);
		if (node === undefined || node.kind !== 'prompt') return undefined;
		return node.status === 'running' ? node : undefined;
	}

	function touchNode(node, status) {
		node.status = status;
		if (status === 'running') {
			node.startedAt = Date.now();
			delete node.endedAt;
			delete node.error;
		}
		if (status === 'completed' || status === 'failed') {
			node.endedAt = Date.now();
		}
	}

	function evaluate(condition, outcome) {
		if (condition === undefined) return true;
		const text = outcome === undefined || typeof outcome.text !== 'string' ? '' : outcome.text;
		let value;
		switch (condition.type) {
			case 'regex': {
				if (condition.value === '') return false;
				try {
					value = new RegExp(condition.value, 'm').test(text);
				} catch {
					value = false;
				}
				break;
			}
			case 'contains':
				value = condition.value !== '' && text.includes(condition.value);
				break;
			case 'marker':
				value = text.includes(condition.value === '' ? '[[FLOW:DONE]]' : condition.value);
				break;
			case 'tool-failed':
				value = outcome !== undefined && Array.isArray(outcome.toolErrors) && outcome.toolErrors.length > 0;
				break;
			case 'last-failed':
				value = outcome !== undefined && outcome.ok === false;
				break;
			case 'always':
			default:
				value = true;
				break;
		}
		return condition.negate === true ? !value : value;
	}

	function armWatchdog(sessionId, graph, node) {
		clearWatchdog(node.id);
		const handle = later(async () => {
			watchdogs.delete(node.id);
			if (node.status !== 'running') return;
			const current = graphs.get(sessionId);
			if (current === undefined) return;
			touchNode(node, 'failed');
			node.error = fail('no-turn', 'the prompt was accepted but no turn started');
			graph.run.waitingNode = null;
			graph.run.state = 'error';
			graph.run.message = 'no-turn';
			await persist(sessionId, current);
		}, DISPATCH_WATCHDOG_MS);
		watchdogs.set(node.id, handle);
	}

	function finishRun(graph, state, message) {
		graph.run.state = state;
		graph.run.message = message ?? '';
		graph.run.waitingNode = null;
		graph.run.cursor = null;
		graph.run.endedAt = Date.now();
	}

	function modelPartsOf(node) {
		if (node.provider === undefined || node.model === undefined) return undefined;
		if (node.provider === '' || node.model === '') return undefined;
		const parts = { provider: node.provider, model: node.model };
		if (typeof node.reasoningEffort === 'string' && node.reasoningEffort !== '') parts.reasoningEffort = node.reasoningEffort;
		return parts;
	}

	async function attachmentsOf(node) {
		const content = [];
		if (typeof node.text === 'string' && node.text.trim() !== '') content.push({ type: 'text', text: node.text });
		for (const attachment of node.attachments ?? []) {
			if (attachment.type === 'image') {
				content.push({
					type: 'image',
					attachment: {
						attachmentId: attachment.attachment.attachmentId,
						mediaType: attachment.attachment.mediaType,
						bytes: attachment.attachment.bytes,
						width: attachment.attachment.width,
						height: attachment.attachment.height,
						name: attachment.attachment.name,
					},
				});
			} else {
				content.push({ type: 'file', attachment: attachment.attachment });
			}
		}
		return content;
	}

	/**
	 * Execute one node. Returns one of:
	 *   { port }            follow that output port
	 *   { wait: true }      a prompt turn is running; stop and wait
	 *   { stop: true }      the run is now waiting or ended
	 */
	async function executeNode(sessionId, graph, node) {
		switch (node.kind) {
			case 'model': {
				touchNode(node, 'running');
				await persist(sessionId, graph);
				const selection = { sessionId, provider: node.provider, model: node.model };
				if (typeof node.reasoningEffort === 'string' && node.reasoningEffort !== '') selection.reasoningEffort = node.reasoningEffort;
				await ctx.sessionController.selectModel(selection);
				touchNode(node, 'completed');
				// Configuration changes do not overwrite the work outcome that
				// conditions are evaluated against.
				return { port: 'next' };
			}
			case 'command': {
				if (typeof node.command !== 'string' || node.command.trim() === '') {
					touchNode(node, 'failed');
					node.error = fail('invalid-command', 'this step has no command line');
					return { port: 'next' };
				}
				touchNode(node, 'running');
				await persist(sessionId, graph);
				const commands = ctx.get('commands');
				if (commands === undefined) {
					touchNode(node, 'failed');
					node.error = fail('no-commands', 'the command service is not available in this profile');
					return { port: 'next' };
				}
				const resolved = await ctx.sessionController.resolveAgent(sessionId);
				if (resolved === undefined || resolved.agent === undefined) {
					touchNode(node, 'failed');
					node.error = fail('no-agent', 'the Session could not be resumed');
					return { port: 'next' };
				}
				const line = node.command.startsWith('/') ? node.command : `/${node.command}`;
				const execution = await commands.execute(resolved.agent, line, [], new AbortController().signal);
				if (execution === undefined) {
					touchNode(node, 'failed');
					node.error = fail('unknown-command', `no command named ${line}`);
					return { port: 'next' };
				}
				const result = execution.result ?? {};
				node.result = typeof result.text === 'string' ? result.text.slice(0, 4000) : '';
				if (result.kind === 'success') {
					touchNode(node, 'completed');
					graph.outcome = { kind: 'command', text: node.result, ok: true, toolErrors: [] };
				} else {
					touchNode(node, 'failed');
					node.error = fail('command-failed', node.result);
					graph.outcome = { kind: 'command', text: node.result, ok: false, toolErrors: [] };
				}
				return { port: 'next' };
			}
			case 'checkpoint': {
				touchNode(node, 'waiting');
				graph.run.state = 'waiting';
				graph.run.waitingNode = node.id;
				graph.run.cursor = node.id;
				return { stop: true };
			}
			case 'branch': {
				touchNode(node, 'running');
				const ok = evaluate(node.condition, graph.outcome);
				touchNode(node, 'completed');
				node.title = ok ? '条件成立' : '条件不成立';
				return { port: ok ? 'yes' : 'no' };
			}
			case 'loop': {
				const used = graph.counters[node.id] ?? 0;
				const done = evaluate(node.condition, graph.outcome);
				if (done || used >= node.maxIterations) {
					touchNode(node, 'completed');
					node.result = used >= node.maxIterations && !done ? '达到最大轮数' : '满足退出条件';
					return { port: 'exit' };
				}
				graph.counters[node.id] = used + 1;
				touchNode(node, 'running');
				node.result = `第 ${used + 1} 轮`;
				return { port: 'again' };
			}
			case 'prompt':
			default: {
				const override = modelPartsOf(node);
				if (override !== undefined) {
					await ctx.sessionController.selectModel({ sessionId, ...override });
				}
				// A prompt whose entire text is one host command line runs as that
				// command — the composer never silently downgrades a command line to
				// a plain prompt, and neither does a queued step.
				const inline = await inlineCommandOf(sessionId, node);
				if (inline !== undefined) {
					touchNode(node, 'running');
					await persist(sessionId, graph);
					const execution = await inline.commands.execute(inline.agent, inline.line, [], new AbortController().signal);
					if (execution === undefined) {
						touchNode(node, 'failed');
						node.error = fail('unknown-command', `no command resolved for ${inline.line}`);
					} else {
						node.result = typeof execution.result?.text === 'string' ? execution.result.text.slice(0, 4000) : '';
						if (execution.result?.kind === 'success') {
							touchNode(node, 'completed');
							graph.outcome = { kind: 'command', text: node.result, ok: true, toolErrors: [] };
						} else {
							touchNode(node, 'failed');
							node.error = fail('command-failed', node.result);
							graph.outcome = { kind: 'command', text: node.result, ok: false, toolErrors: [] };
						}
					}
					return { port: 'next' };
				}
				touchNode(node, 'running');
				graph.run.waitingNode = node.id;
				await persist(sessionId, graph);
				const content = await attachmentsOf(node);
				if (content.length === 0) {
					touchNode(node, 'failed');
					node.error = fail('empty-prompt', 'this step has no text or attachment');
					graph.run.waitingNode = null;
					return { port: 'next' };
				}
				await ctx.sessionController.prompt(
					{
						requestId: `flow-queue-${randomUUID()}`,
						sessionId,
						mode: 'queue',
						content,
						clientTimeZone: Intl.DateTimeFormat().resolvedOptions().timeZone,
					},
					new AbortController().signal,
				);
				armWatchdog(sessionId, graph, node);
				return { wait: true };
			}
		}
	}

	/**
	 * Resolve the inline command a prompt step carries, when its whole text is
	 * exactly one host command line (`/name args…`). Client-owned popup commands
	 * (`popupSelect` surfaces like the model picker) have no host row and stay
	 * plain prompt text.
	 */
	async function inlineCommandOf(sessionId, node) {
		const commands = ctx.get('commands');
		if (commands === undefined || typeof commands.find !== 'function') return undefined;
		const text = typeof node.text === 'string' ? node.text.trim() : '';
		if (!text.startsWith('/') || text.includes('\n')) return undefined;
		const name = text.slice(1).split(/\s+/)[0] ?? '';
		if (name === '' || !/^[A-Za-z0-9_-]+$/.test(name)) return undefined;
		const resolved = await ctx.sessionController.resolveAgent(sessionId);
		if (resolved === undefined || resolved.agent === undefined) return undefined;
		if (commands.find(resolved.agent, name) === undefined) return undefined;
		return { commands, agent: resolved.agent, line: text };
	}

	async function advance(sessionId) {
		const graph = await ensureGraph(sessionId);
		if (graph.run.state !== 'running' || graph.pumping) return;
		if (!isIdle(sessionId)) return;
		graph.pumping = true;
		try {
			for (let guard = 0; guard < MAX_LOOP_EXECUTIONS; guard += 1) {
				if (graph.run.state !== 'running') break;
				if (!isIdle(sessionId)) break;
				const current = graph.run.cursor === null ? entryNode(graph) : findNode(graph, graph.run.cursor);
				if (current === undefined) {
					finishRun(graph, 'done', 'the graph has no entry step');
					break;
				}
				graph.run.cursor = current.id;
				const result = await executeNode(sessionId, graph, current);
				graph.run.steps += 1;
				await persist(sessionId, graph);
				if (result.wait === true) break;
				if (result.stop === true) break;
				const next = followEdge(graph, current.id, result.port);
				if (next === undefined) {
					finishRun(graph, 'done', 'no outgoing connection');
					break;
				}
				graph.run.cursor = next;
			}
		} catch (error) {
			graph.run.state = 'error';
			graph.run.message = error instanceof Error ? error.message.slice(0, 300) : String(error);
			const node = graph.run.cursor === null ? undefined : findNode(graph, graph.run.cursor);
			if (node !== undefined && node.status === 'running') {
				touchNode(node, 'failed');
				node.error = fail('dispatch-failed', graph.run.message);
			}
			clearWatchdog(node === undefined ? '' : node.id);
		} finally {
			graph.pumping = false;
			await persist(sessionId, graph);
		}
	}

	ctx.on('session/event', (session, event) => {
		const sessionId = session === undefined ? undefined : session.id;
		if (typeof sessionId !== 'string' || event === undefined || event === null) return;
		const graph = graphs.get(sessionId);

		if (event.type === 'assistant/message') {
			if (graph === undefined) return;
			const data = event.data ?? {};
			const text = textOfBlocks(data.message === undefined ? [] : data.message.content);
			if (typeof data.turn === 'number') {
				if (graph.lastTurn === undefined || graph.lastTurn.turn !== data.turn) {
					graph.lastTurn = { turn: data.turn, text: '', toolErrors: [] };
				}
				if (text !== '') graph.lastTurn.text = text;
			} else if (graph.lastTurn !== undefined) {
				graph.lastTurn.text = text;
			}
			return;
		}

		if (event.type === 'tool/result') {
			if (graph === undefined) return;
			const data = event.data ?? {};
			if (data.error === undefined || data.error === null) return;
			if (typeof data.turn === 'number') {
				if (graph.lastTurn === undefined || graph.lastTurn.turn !== data.turn) {
					graph.lastTurn = { turn: data.turn, text: '', toolErrors: [] };
				}
				const name = data.error.name === undefined ? 'tool' : String(data.error.name);
				graph.lastTurn.toolErrors.push(name);
			}
			return;
		}

		if (event.type === 'request/header') {
			if (graph === undefined) return;
			const node = runningPromptNode(graph);
			if (node === undefined) return;
			const config = event.data === undefined || event.data.header === undefined ? undefined : event.data.header.config;
			if (config === undefined || typeof config.provider !== 'string' || typeof config.model !== 'string') return;
			node.usedModel = `${config.provider}/${config.model}`;
			return;
		}

		if (event.type === 'turn/start') {
			if (graph === undefined) return;
			const node = runningPromptNode(graph);
			if (node === undefined) return;
			const turn = event.data === undefined ? undefined : event.data.turn;
			if (typeof turn === 'number') node.turn = turn;
			clearWatchdog(node.id);
			return;
		}

		if (event.type !== 'turn/end') return;
		if (graph === undefined) return;
		const data = event.data ?? {};
		const node = runningPromptNode(graph);
		if (node !== undefined) {
			const reason = data.reason ?? { kind: 'completed' };
			const kind = typeof reason.kind === 'string' ? reason.kind : 'completed';
			if (kind === 'completed' || kind === 'max-tokens') {
				touchNode(node, 'completed');
			} else {
				touchNode(node, 'failed');
				node.error = fail(`turn-${kind}`, kind === 'error' && reason.error !== undefined ? reason.error.message : '');
			}
			const lastTurn = graph.lastTurn !== undefined && graph.lastTurn.turn === data.turn ? graph.lastTurn : undefined;
			graph.outcome = {
				kind: 'prompt',
				text: lastTurn === undefined ? '' : lastTurn.text,
				ok: kind === 'completed' || kind === 'max-tokens',
				toolErrors: lastTurn === undefined ? [] : lastTurn.toolErrors.slice(0, 20),
			};
			graph.run.waitingNode = null;
			clearWatchdog(node.id);
			// The prompt step is done: move the cursor along its edge now, otherwise
			// the next advance would dispatch the very same prompt again.
			const nextAfterPrompt = followEdge(graph, node.id, 'next');
			if (nextAfterPrompt === undefined) finishRun(graph, 'done', 'no outgoing connection');
			else graph.run.cursor = nextAfterPrompt;
			void persist(sessionId, graph);
		}
		if (graph.run.state === 'running') later(() => advance(sessionId), SETTLE_MS);
	});

	ctx.on('agent/status', (payload) => {
		const agent = payload === undefined ? undefined : payload.agent;
		const status = payload === undefined ? undefined : payload.status;
		if (agent === undefined || status !== 'idle') return;
		const sessionId = agent.id;
		if (typeof sessionId !== 'string') return;
		const graph = graphs.get(sessionId);
		if (graph === undefined || graph.run.state !== 'running') return;
		later(() => advance(sessionId), SETTLE_MS * 2);
	});

	//#endregion

	//#region packages

	async function listPackages() {
		let entries = [];
		try {
			entries = await readdir(packageDir);
		} catch {
			return [];
		}
		const list = [];
		for (const name of entries) {
			if (name.startsWith('.') || !name.endsWith('.json')) continue;
			try {
				const parsed = JSON.parse(await readFile(path.join(packageDir, name), 'utf8'));
				if (parsed === null || typeof parsed !== 'object') continue;
				list.push({
					id: name.slice(0, -'.json'.length),
					name: typeof parsed.name === 'string' ? parsed.name : name,
					description: typeof parsed.description === 'string' ? parsed.description : '',
					createdAt: Number.isFinite(parsed.createdAt) ? parsed.createdAt : 0,
					nodes: Array.isArray(parsed.nodes) ? parsed.nodes.length : 0,
					edges: Array.isArray(parsed.edges) ? parsed.edges.length : 0,
				});
			} catch {
				/* skip an unreadable package */
			}
		}
		return list.sort((left, right) => right.createdAt - left.createdAt);
	}

	async function savePackage(sessionId, payload) {
		const graph = await ensureGraph(sessionId);
		const name = singleLine(payload.name) || '未命名流程';
		const description = typeof payload.description === 'string' ? payload.description.slice(0, 500) : '';
		const id = `pkg_${randomUUID().replace(/-/g, '').slice(0, 16)}`;
		const body = {
			version: STORE_VERSION,
			id,
			name,
			description,
			createdAt: Date.now(),
			nodes: graph.nodes.map((node) => {
				const copy = { ...node };
				delete copy.status;
				delete copy.startedAt;
				delete copy.endedAt;
				delete copy.turn;
				delete copy.usedModel;
				delete copy.error;
				delete copy.durationMs;
				if (copy.result !== undefined) delete copy.result;
				return copy;
			}),
			edges: graph.edges.map((edge) => ({ ...edge })),
			entry: graph.entry,
		};
		await mkdir(packageDir, { recursive: true });
		await writeFile(path.join(packageDir, `${id}.json`), JSON.stringify(body), 'utf8');
		return { id, name };
	}

	async function loadPackage(sessionId, id) {
		const safe = typeof id === 'string' ? id.replace(/[^A-Za-z0-9_-]/g, '') : '';
		if (safe === '') throw fail('invalid-package', 'a package id is required');
		let parsed;
		try {
			parsed = JSON.parse(await readFile(path.join(packageDir, `${safe}.json`), 'utf8'));
		} catch {
			throw fail('invalid-package', 'no such package');
		}
		const graph = await ensureGraph(sessionId);
		if (graph.run.state === 'running' || graph.run.state === 'waiting') {
			throw fail('run-active', 'pause or reset the run before loading a package');
		}
		const loaded = normalizeStored({ version: STORE_VERSION, nodes: parsed.nodes, edges: parsed.edges, entry: parsed.entry }, sessionId);
		graph.nodes = loaded.nodes;
		graph.edges = loaded.edges;
		graph.entry = loaded.entry;
		graph.run = emptyRun();
		graph.counters = {};
		graph.outcome = { kind: 'none', text: '', ok: true, toolErrors: [] };
		await persist(sessionId, graph);
		return publicState(graph);
	}

	async function deletePackage(id) {
		const safe = typeof id === 'string' ? id.replace(/[^A-Za-z0-9_-]/g, '') : '';
		if (safe === '') throw fail('invalid-package', 'a package id is required');
		await rm(path.join(packageDir, `${safe}.json`), { force: true });
		return { id: safe };
	}

	//#endregion

	//#region commands

	function requireSessionId(value) {
		if (typeof value !== 'string' || !/^[A-Za-z0-9_-]{1,128}$/.test(value)) {
			throw fail('invalid-session', 'a valid sessionId is required');
		}
		return value;
	}

	function requireNode(graph, value) {
		if (typeof value !== 'string') throw fail('invalid-node', 'a node id is required');
		const node = findNode(graph, value);
		if (node === undefined) throw fail('invalid-node', 'no such node');
		return node;
	}

	function requireMutable(graph) {
		if (graph.run.state === 'running' || graph.run.state === 'waiting') {
			throw fail('run-active', 'pause or reset the run before editing the graph');
		}
	}

	function applyPatch(node, patch) {
		if (patch === null || typeof patch !== 'object') return;
		if (Number.isFinite(patch.x)) node.x = Math.round(patch.x);
		if (Number.isFinite(patch.y)) node.y = Math.round(patch.y);
		if (typeof patch.title === 'string') node.title = patch.title.slice(0, 200);
		if (typeof patch.text === 'string') node.text = patch.text.slice(0, MAX_PROMPT_CHARS);
		if (typeof patch.command === 'string') node.command = patch.command.slice(0, 2000);
		if (typeof patch.note === 'string') node.note = patch.note.slice(0, 500);
		if (patch.condition !== undefined) node.condition = conditionOf(patch.condition);
		if (Number.isFinite(patch.maxIterations)) node.maxIterations = Math.min(Math.max(Math.round(patch.maxIterations), 1), 100);
		if (patch.provider === null) delete node.provider;
		else if (typeof patch.provider === 'string') node.provider = patch.provider.slice(0, MAX_IDENT_CHARS);
		if (patch.model === null) delete node.model;
		else if (typeof patch.model === 'string') node.model = patch.model.slice(0, MAX_IDENT_CHARS);
		if (patch.modelLabel === null) delete node.modelLabel;
		else if (typeof patch.modelLabel === 'string') node.modelLabel = patch.modelLabel.slice(0, MAX_IDENT_CHARS);
		if (patch.reasoningEffort === null) delete node.reasoningEffort;
		else if (typeof patch.reasoningEffort === 'string') {
			const effort = patch.reasoningEffort.trim().slice(0, 64);
			if (effort === '') delete node.reasoningEffort;
			else node.reasoningEffort = effort;
		}
	}

	async function addAttachment(graph, node, payload) {
		const attachments = ctx.get('attachments');
		if (attachments === undefined) throw fail('no-attachments', 'the attachment service is not available');
		if (!Array.isArray(node.attachments)) node.attachments = [];
		if (node.attachments.length >= MAX_ATTACHMENTS_PER_NODE) throw fail('too-many-attachments', 'this step already has the maximum number of attachments');
		const data = typeof payload.data === 'string' ? payload.data : '';
		if (data === '') throw fail('invalid-attachment', 'attachment data is required');
		const bytes = Buffer.from(data, 'base64');
		const name = singleLine(payload.name, 200) || undefined;
		if (payload.kind === 'image') {
			const mediaType = typeof payload.mediaType === 'string' ? payload.mediaType : 'image/png';
			const [ref] = await attachments.saveImages([{ data: bytes, mediaType, name }]);
			node.attachments.push({
				type: 'image',
				attachment: {
					attachmentId: ref.attachmentId,
					mediaType: ref.mediaType,
					bytes: ref.bytes,
					width: ref.width,
					height: ref.height,
					name: ref.name ?? '',
				},
			});
		} else {
			const ref = await attachments.admitEncodedFile({ data, name });
			node.attachments.push({ type: 'file', attachment: { attachmentId: ref.attachmentId, name: ref.name, bytes: ref.bytes } });
		}
		return publicNode(node);
	}

	async function runCommand(payload) {
		if (payload === null || typeof payload !== 'object') throw fail('invalid-command', 'a JSON object is required');
		const sessionId = requireSessionId(payload.sessionId);
		const graph = await ensureGraph(sessionId);
		const op = payload.op;

		switch (op) {
			case 'node.add': {
				requireMutable(graph);
				if (graph.nodes.length >= MAX_NODES) throw fail('graph-full', 'this flow already has the maximum number of steps');
				const node = normalizeNode({
					kind: payload.kind,
					x: Number.isFinite(payload.x) ? payload.x : 60,
					y: Number.isFinite(payload.y) ? payload.y : 60,
					text: payload.text,
					command: payload.command,
					provider: payload.provider,
					model: payload.model,
					modelLabel: payload.modelLabel,
					reasoningEffort: payload.reasoningEffort,
					condition: payload.condition,
					maxIterations: payload.maxIterations,
					note: payload.note,
				});
				if (node === undefined) throw fail('invalid-node', 'unknown step kind');
				graph.nodes.push(node);
				if (graph.entry === null) graph.entry = node.id;
				await persist(sessionId, graph);
				return { state: publicState(graph), node: publicNode(node) };
			}
			case 'node.update': {
				requireMutable(graph);
				const node = requireNode(graph, payload.id);
				applyPatch(node, payload.patch);
				await persist(sessionId, graph);
				return { state: publicState(graph) };
			}
			case 'node.remove': {
				requireMutable(graph);
				const node = requireNode(graph, payload.id);
				graph.nodes = graph.nodes.filter((candidate) => candidate.id !== node.id);
				graph.edges = graph.edges.filter((edge) => edge.from !== node.id && edge.to !== node.id);
				if (graph.entry === node.id) graph.entry = graph.nodes.length > 0 ? graph.nodes[0].id : null;
				delete graph.counters[node.id];
				await persist(sessionId, graph);
				return { state: publicState(graph) };
			}
			case 'edge.add': {
				requireMutable(graph);
				const from = requireNode(graph, payload.from);
				const to = requireNode(graph, payload.to);
				const port = typeof payload.port === 'string' ? payload.port : 'next';
				const allowed = PORTS[from.kind] ?? ['next'];
				if (!allowed.includes(port)) throw fail('invalid-port', `a ${from.kind} step has no "${port}" output`);
				if (from.id === to.id) throw fail('invalid-edge', 'a step cannot connect to itself');
				const existing = graph.edges.find((edge) => edge.from === from.id && edge.port === port);
				if (existing !== undefined) graph.edges = graph.edges.filter((edge) => edge !== existing);
				if (graph.edges.length >= MAX_EDGES) throw fail('graph-full', 'this flow already has the maximum number of connections');
				graph.edges.push({ id: newId('e'), from: from.id, port, to: to.id });
				await persist(sessionId, graph);
				return { state: publicState(graph) };
			}
			case 'edge.remove': {
				requireMutable(graph);
				const before = graph.edges.length;
				graph.edges = graph.edges.filter((edge) => edge.id !== payload.id);
				if (graph.edges.length === before) throw fail('invalid-edge', 'no such connection');
				await persist(sessionId, graph);
				return { state: publicState(graph) };
			}
			case 'node.attach': {
				requireMutable(graph);
				const node = requireNode(graph, payload.id);
				const view = await addAttachment(graph, node, payload);
				await persist(sessionId, graph);
				return { state: publicState(graph), node: view };
			}
			case 'node.detach': {
				requireMutable(graph);
				const node = requireNode(graph, payload.id);
				const attachmentId = typeof payload.attachmentId === 'string' ? payload.attachmentId : '';
				node.attachments = (node.attachments ?? []).filter((item) => item.attachment.attachmentId !== attachmentId);
				await persist(sessionId, graph);
				return { state: publicState(graph) };
			}
			case 'entry.set': {
				requireMutable(graph);
				const node = requireNode(graph, payload.id);
				graph.entry = node.id;
				await persist(sessionId, graph);
				return { state: publicState(graph) };
			}
			case 'graph.clear': {
				requireMutable(graph);
				graph.nodes = [];
				graph.edges = [];
				graph.entry = null;
				graph.counters = {};
				graph.run = emptyRun();
				graph.outcome = { kind: 'none', text: '', ok: true, toolErrors: [] };
				await persist(sessionId, graph);
				return { state: publicState(graph) };
			}
			case 'graph.reset': {
				graph.run = emptyRun();
				graph.counters = {};
				graph.outcome = { kind: 'none', text: '', ok: true, toolErrors: [] };
				for (const node of graph.nodes) {
					node.status = 'idle';
					delete node.startedAt;
					delete node.endedAt;
					delete node.error;
					delete node.durationMs;
					delete node.usedModel;
					delete node.turn;
					delete node.result;
				}
				await persist(sessionId, graph);
				return { state: publicState(graph) };
			}
			case 'run.start': {
				if (graph.nodes.length === 0) throw fail('empty-graph', 'add a step before running');
				graph.run = {
					...emptyRun(),
					state: 'running',
					cursor: typeof payload.id === 'string' && findNode(graph, payload.id) !== undefined
						? payload.id
						: entryNode(graph) === undefined ? null : entryNode(graph).id,
					startedAt: Date.now(),
				};
				for (const node of graph.nodes) {
					if (node.status === 'completed' || node.status === 'failed' || node.status === 'waiting') {
						node.status = 'idle';
						delete node.error;
					}
				}
				graph.counters = {};
				await persist(sessionId, graph);
				later(() => advance(sessionId), 0);
				return { state: publicState(graph) };
			}
			case 'run.pause': {
				if (graph.run.state === 'running') {
					graph.run.state = 'paused';
					graph.run.message = 'paused by user';
				}
				await persist(sessionId, graph);
				return { state: publicState(graph) };
			}
			case 'run.resume': {
				const waiting = graph.run.waitingNode === null ? undefined : findNode(graph, graph.run.waitingNode);
				if (waiting !== undefined && waiting.kind === 'checkpoint') {
					touchNode(waiting, 'completed');
					const next = followEdge(graph, waiting.id, 'next');
					graph.run.waitingNode = null;
					graph.run.cursor = next === undefined ? null : next;
					graph.run.state = next === undefined ? 'done' : 'running';
				} else {
					graph.run.state = 'running';
				}
				graph.run.message = '';
				await persist(sessionId, graph);
				later(() => advance(sessionId), 0);
				return { state: publicState(graph) };
			}
			case 'run.stop': {
				graph.run.state = 'idle';
				graph.run.cursor = null;
				graph.run.waitingNode = null;
				graph.run.message = 'stopped';
				graph.run.endedAt = Date.now();
				await persist(sessionId, graph);
				return { state: publicState(graph) };
			}
			case 'run.from': {
				requireMutable(graph);
				const node = requireNode(graph, payload.id);
				graph.run = { ...emptyRun(), state: 'running', cursor: node.id, startedAt: Date.now() };
				graph.counters = {};
				for (const candidate of graph.nodes) {
					if (candidate.id !== node.id) {
						candidate.status = 'idle';
						delete candidate.error;
					}
				}
				await persist(sessionId, graph);
				later(() => advance(sessionId), 0);
				return { state: publicState(graph) };
			}
			default:
				throw fail('invalid-command', 'unknown op');
		}
	}

	//#endregion

	//#region routes

	function admitted(request) {
		return ctx.connection.requestRejection(request) === undefined;
	}

	/** Session-scoped "/" catalog (host commands + skills), refreshed at most this often. */
	const MENTION_CACHE_MS = 30000;
	const mentionCache = new Map();

	async function slashCatalog(sessionId) {
		const cached = mentionCache.get(sessionId);
		if (cached !== undefined && Date.now() - cached.at < MENTION_CACHE_MS) return cached.value;
		const commands = [];
		const skills = [];
		const resolved = await ctx.sessionController.resolveAgent(sessionId);
		const agent = resolved !== undefined ? resolved.agent : undefined;
		if (agent !== undefined) {
			const registry = ctx.get('commands');
			if (registry !== undefined && typeof registry.list === 'function') {
				for (const descriptor of await registry.list(agent)) {
					if (descriptor === null || typeof descriptor !== 'object' || typeof descriptor.name !== 'string') continue;
					if (!/^[A-Za-z0-9_-]+$/.test(descriptor.name)) continue;
					commands.push({
						name: descriptor.name,
						description: typeof descriptor.description === 'string' ? descriptor.description.slice(0, 200) : '',
					});
				}
			}
			const skillRegistry = ctx.get('skills');
			const workingDirectory = ctx.get('workingDirectory');
			if (skillRegistry !== undefined && typeof skillRegistry.list === 'function') {
				const cwd = workingDirectory !== undefined && agent.session !== undefined ? workingDirectory.get(agent.session) : undefined;
				const listed = await skillRegistry.list({ cwd });
				for (const skill of Array.isArray(listed) ? listed : []) {
					if (skill === null || typeof skill !== 'object' || typeof skill.name !== 'string') continue;
					if (skill.invocation !== undefined && skill.invocation.userInvocable !== true && skill.invocation.modelInvocable !== true) continue;
					skills.push({
						name: skill.name,
						description: typeof skill.description === 'string' ? skill.description.slice(0, 200) : '',
						userInvocable: skill.invocation?.userInvocable === true,
					});
				}
			}
		}
		commands.sort((left, right) => left.name.localeCompare(right.name));
		skills.sort((left, right) => left.name.localeCompare(right.name));
		const value = { commands, skills };
		mentionCache.set(sessionId, { at: Date.now(), value });
		return value;
	}

	async function fileCandidates(sessionId, query, signal) {
		const references = ctx.get('fileReferences');
		if (references === undefined || typeof references.list !== 'function') return [];
		const resolved = await ctx.sessionController.resolveAgent(sessionId);
		const agent = resolved !== undefined ? resolved.agent : undefined;
		if (agent === undefined) return [];
		const listed = await references.list(agent, typeof query === 'string' ? query : '', signal);
		const files = [];
		for (const candidate of Array.isArray(listed) ? listed : []) {
			if (candidate === null || typeof candidate !== 'object' || typeof candidate.path !== 'string') continue;
			files.push({ path: candidate.path, kind: candidate.kind === 'directory' ? 'directory' : 'file' });
			if (files.length >= 20) break;
		}
		return files;
	}

	ctx.on('session/disposed', (session) => {
		mentionCache.delete(session.id);
	});

	ctx.effect(
		() =>
			ctx.webServer.register({
				kind: 'exact',
				path: `${PREFIX}/mention`,
				handler: async (req, res) => {
					if (!admitted(req)) return writeJson(res, 403, { ok: false, error: fail('forbidden', 'forbidden') });
					if (req.method !== 'GET' && req.method !== 'HEAD') return writeJson(res, 405, { ok: false, error: fail('method-error', 'method not allowed') });
					let sessionId = '';
					let kind = '';
					let query = '';
					try {
						const url = new URL(req.url ?? '/', 'http://dsh.internal');
						sessionId = url.searchParams.get('sessionId') ?? '';
						kind = url.searchParams.get('kind') ?? '';
						query = url.searchParams.get('query') ?? '';
					} catch {
						/* fall through with the empty values */
					}
					if (!/^[A-Za-z0-9_-]{1,128}$/.test(sessionId)) {
						return writeJson(res, 400, { ok: false, error: fail('invalid-session', 'missing or invalid sessionId') });
					}
					try {
						const value = kind === 'file'
							? { files: await fileCandidates(sessionId, query, new AbortController().signal) }
							: await slashCatalog(sessionId);
						writeJson(res, 200, { ok: true, value });
					} catch (error) {
						writeJson(res, 500, { ok: false, error: fail('mention-failed', error instanceof Error ? error.message : String(error)) });
					}
				},
			}),
		'dsh-flow-queue: mention route',
	);

	ctx.effect(
		() =>
			ctx.webServer.register({
				kind: 'exact',
				path: STATE_PATH,
				handler: async (req, res) => {
					if (!admitted(req)) return writeJson(res, 403, { ok: false, error: fail('forbidden', 'forbidden') });
					if (req.method !== 'GET' && req.method !== 'HEAD') return writeJson(res, 405, { ok: false, error: fail('method-error', 'method not allowed') });
					let sessionId = '';
					try {
						sessionId = new URL(req.url ?? '/', 'http://dsh.internal').searchParams.get('sessionId') ?? '';
					} catch {
						sessionId = '';
					}
					if (!/^[A-Za-z0-9_-]{1,128}$/.test(sessionId)) {
						return writeJson(res, 400, { ok: false, error: fail('invalid-session', 'missing or invalid sessionId') });
					}
					try {
						const graph = await ensureGraph(sessionId);
						writeJson(res, 200, { ok: true, value: publicState(graph) });
					} catch (error) {
						writeJson(res, 500, { ok: false, error: fail('state-failed', error instanceof Error ? error.message : String(error)) });
					}
				},
			}),
		'dsh-flow-queue: state route',
	);

	ctx.effect(
		() =>
			ctx.webServer.register({
				kind: 'exact',
				path: MODELS_PATH,
				handler: async (req, res) => {
					if (!admitted(req)) return writeJson(res, 403, { ok: false, error: fail('forbidden', 'forbidden') });
					if (req.method !== 'GET' && req.method !== 'HEAD') return writeJson(res, 405, { ok: false, error: fail('method-error', 'method not allowed') });
					try {
						const catalog = await ctx.sessionController.modelCatalog();
						writeJson(res, 200, {
							ok: true,
							value: {
								default: catalog.default ?? null,
								groups: Array.isArray(catalog.groups) ? catalog.groups : [],
								failures: Array.isArray(catalog.failures) ? catalog.failures : [],
							},
						});
					} catch (error) {
						writeJson(res, 500, { ok: false, error: fail('catalog-failed', error instanceof Error ? error.message : String(error)) });
					}
				},
			}),
		'dsh-flow-queue: models route',
	);

	ctx.effect(
		() =>
			ctx.webServer.register({
				kind: 'exact',
				path: PACKAGES_PATH,
				handler: async (req, res) => {
					if (!admitted(req)) return writeJson(res, 403, { ok: false, error: fail('forbidden', 'forbidden') });
					if (req.method === 'GET' || req.method === 'HEAD') {
						try {
							writeJson(res, 200, { ok: true, value: await listPackages() });
						} catch (error) {
							writeJson(res, 500, { ok: false, error: fail('package-failed', error instanceof Error ? error.message : String(error)) });
						}
						return;
					}
					if (req.method !== 'POST') return writeJson(res, 405, { ok: false, error: fail('method-error', 'method not allowed') });
					let payload;
					try {
						payload = JSON.parse(await readBody(req));
					} catch (error) {
						return writeJson(res, 400, { ok: false, error: fail('invalid-json', error instanceof Error ? error.message : String(error)) });
					}
					try {
						const sessionId = requireSessionId(payload === null || payload === undefined ? undefined : payload.sessionId);
						if (payload.op === 'save') return writeJson(res, 200, { ok: true, value: await savePackage(sessionId, payload) });
						if (payload.op === 'load') return writeJson(res, 200, { ok: true, value: await loadPackage(sessionId, payload.id) });
						if (payload.op === 'delete') return writeJson(res, 200, { ok: true, value: await deletePackage(payload.id) });
						return writeJson(res, 400, { ok: false, error: fail('invalid-command', 'unknown op') });
					} catch (error) {
						if (error !== null && typeof error === 'object' && typeof error.code === 'string') {
							return writeJson(res, 400, { ok: false, error });
						}
						return writeJson(res, 500, { ok: false, error: fail('package-failed', error instanceof Error ? error.message : String(error)) });
					}
				},
			}),
		'dsh-flow-queue: packages route',
	);

	ctx.effect(
		() =>
			ctx.webServer.register({
				kind: 'exact',
				path: COMMAND_PATH,
				handler: async (req, res) => {
					if (!admitted(req)) return writeJson(res, 403, { ok: false, error: fail('forbidden', 'forbidden') });
					if (req.method !== 'POST') return writeJson(res, 405, { ok: false, error: fail('method-error', 'method not allowed') });
					let payload;
					try {
						payload = JSON.parse(await readBody(req));
					} catch (error) {
						return writeJson(res, 400, { ok: false, error: fail('invalid-json', error instanceof Error ? error.message : String(error)) });
					}
					try {
						const value = await runCommand(payload);
						writeJson(res, 200, { ok: true, value });
					} catch (error) {
						if (error !== null && typeof error === 'object' && typeof error.code === 'string') {
							writeJson(res, 400, { ok: false, error });
						} else {
							writeJson(res, 500, { ok: false, error: fail('command-failed', error instanceof Error ? error.message : String(error)) });
						}
					}
				},
			}),
		'dsh-flow-queue: command route',
	);

	//#endregion
}
