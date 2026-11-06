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
const MAX_DISPATCH_TARGETS = 8;
const DISPATCH_MIN_TIMEOUT_MS = 15000;
const DISPATCH_MAX_TIMEOUT_MS = 1800000;
const DISPATCH_DEFAULT_TIMEOUT_MS = 600000;
// Tests drive the timeout path with a much lower floor; production keeps 15 s.
let dispatchMinTimeoutMs = DISPATCH_MIN_TIMEOUT_MS;
const DISPATCH_CHILD_STATUSES = new Set(['pending', 'running', 'done', 'failed', 'timeout', 'cancelled', 'interrupted']);
const DISPATCH_COLLECT_MODES = new Set(['all', 'k-of-n', 'first']);

const FLEET_VERSION = 1;
const MAX_FLEET_SESSIONS = 16;
const MAX_FLEET_EDGES = 48;
const MAX_COMM_HOPS = 50;
// Fleet-tunable at apply time (tests cap it at 1; the M4 pilot may tighten it).
let maxCommHops = MAX_COMM_HOPS;
const OUTBOX_MAX_ATTEMPTS = 5;
const OUTBOX_BACKOFF_MS = [2000, 4000, 8000, 16000, 32000, 60000];
const OUTBOX_PUMP_MS = 2000;
// Tests shrink both, exactly like the dispatch timeout floor.
let outboxBackoffMs = OUTBOX_BACKOFF_MS;
let outboxPumpMs = OUTBOX_PUMP_MS;
const WAIT_MIN_TIMEOUT_MS = 5000;
const WAIT_MAX_TIMEOUT_MS = 3600000;
const WAIT_DEFAULT_TIMEOUT_MS = 900000;
let waitMinTimeoutMs = WAIT_MIN_TIMEOUT_MS;
const WAIT_MODES = new Set(['idle', 'marker']);
const DEFAULT_REPORT_MARKER = '[[FLOW:REPORT';
const MAX_FLEET_IDENT = 120;

const NODE_STATUSES = new Set(['idle', 'running', 'completed', 'failed', 'skipped', 'waiting']);
const NODE_KINDS = new Set(['prompt', 'model', 'command', 'checkpoint', 'branch', 'loop', 'dispatch', 'send', 'wait']);
const PORTS = {
	prompt: ['next'],
	model: ['next'],
	command: ['next'],
	checkpoint: ['next'],
	branch: ['yes', 'no'],
	loop: ['again', 'exit'],
	dispatch: ['next'],
	send: ['next'],
	wait: ['next'],
};
const CONDITION_TYPES = new Set(['always', 'regex', 'contains', 'marker', 'tool-failed', 'last-failed', 'judge']);

/**
 * The whole judge protocol: one minimal turn in the same session. The model
 * verifies with whatever cheapest check suffices and answers on the first
 * line; the engine parses that line and nothing else. No tool registration
 * (a link-installed package cannot import the tool DSL, and a global judge
 * tool would sit in every session's schema), no free-text parsing beyond the
 * first token.
 */
const JUDGE_INSTRUCTION = 'Judge the flow condition below, right now. Answer on the FIRST line with exactly YES or NO (met / not met); you may add one brief reason on the second line. Use the cheapest sufficient check, then answer.';

/** Parse the judge verdict from the first non-empty line of the reply. */
function parseJudgeAnswer(text) {
	const firstLine = typeof text === 'string'
		? text.split('\n').map((line) => line.trim()).find((line) => line !== '')
		: undefined;
	if (firstLine === undefined) return undefined;
	const match = /^(yes|no|true|false|是|否)(?![a-z0-9])/i.exec(firstLine);
	if (match === null) return undefined;
	return /^(yes|true|是)/i.test(match[1]);
}

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
		waitingJudge: null,
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

function normalizeDispatchTargets(input) {
	if (!Array.isArray(input) || input.length === 0) {
		return [{ label: 'A', provider: '', model: '', reasoningEffort: '' }];
	}
	const targets = input
		.slice(0, MAX_DISPATCH_TARGETS)
		.map((item) => {
			if (item === null || typeof item !== 'object') return undefined;
			return {
				label: typeof item.label === 'string' && item.label.trim() !== '' ? item.label.trim().slice(0, 80) : 'model',
				provider: typeof item.provider === 'string' ? item.provider.slice(0, MAX_IDENT_CHARS) : '',
				model: typeof item.model === 'string' ? item.model.slice(0, MAX_IDENT_CHARS) : '',
				reasoningEffort: typeof item.reasoningEffort === 'string' ? item.reasoningEffort.slice(0, 64) : '',
			};
		})
		.filter((item) => item !== undefined);
	return targets.length > 0 ? targets : [{ label: 'A', provider: '', model: '', reasoningEffort: '' }];
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
		case 'dispatch': {
			node.text = typeof input.text === 'string' ? input.text.slice(0, MAX_PROMPT_CHARS) : '';
			node.targets = normalizeDispatchTargets(input.targets);
			node.collect = DISPATCH_COLLECT_MODES.has(input.collect) ? input.collect : 'all';
			const total = Math.max(node.targets.length, 1);
			node.k = Number.isFinite(input.k) ? Math.min(Math.max(Math.round(input.k), 1), total) : 1;
			if (node.collect === 'first') node.k = 1;
			const timeout = Number.isFinite(input.timeoutMs) ? Math.round(input.timeoutMs) : DISPATCH_DEFAULT_TIMEOUT_MS;
			node.timeoutMs = Math.min(Math.max(timeout, dispatchMinTimeoutMs), DISPATCH_MAX_TIMEOUT_MS);
			if (Array.isArray(input.children)) {
				node.children = input.children
					.slice(0, MAX_DISPATCH_TARGETS)
					.map((child) => {
						if (child === null || typeof child !== 'object') return undefined;
						return {
							label: typeof child.label === 'string' ? child.label.slice(0, 80) : '',
							provider: typeof child.provider === 'string' ? child.provider.slice(0, MAX_IDENT_CHARS) : '',
							model: typeof child.model === 'string' ? child.model.slice(0, MAX_IDENT_CHARS) : '',
							status: DISPATCH_CHILD_STATUSES.has(child.status) ? child.status : 'pending',
							summary: typeof child.summary === 'string' ? child.summary.slice(0, 600) : '',
						};
					})
					.filter((child) => child !== undefined);
			}
			break;
		}
		case 'send': {
			node.text = typeof input.text === 'string' ? input.text.slice(0, MAX_PROMPT_CHARS) : '';
			node.targetRef = typeof input.targetRef === 'string' ? input.targetRef.slice(0, MAX_IDENT_CHARS) : '';
			node.attachChanges = input.attachChanges === true;
			break;
		}
		case 'wait': {
			node.targetRef = typeof input.targetRef === 'string' ? input.targetRef.slice(0, MAX_IDENT_CHARS) : '';
			node.until = WAIT_MODES.has(input.until) ? input.until : 'idle';
			node.marker = typeof input.marker === 'string' && input.marker.trim() !== '' ? input.marker.trim().slice(0, 200) : DEFAULT_REPORT_MARKER;
			const timeout = Number.isFinite(input.timeoutMs) ? Math.round(input.timeoutMs) : WAIT_DEFAULT_TIMEOUT_MS;
			node.timeoutMs = Math.min(Math.max(timeout, waitMinTimeoutMs), WAIT_MAX_TIMEOUT_MS);
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
	if (Number.isFinite(config.dispatchMinTimeoutMs)) {
		dispatchMinTimeoutMs = Math.max(50, Math.round(config.dispatchMinTimeoutMs));
	}
	if (Number.isFinite(config.waitMinTimeoutMs)) {
		waitMinTimeoutMs = Math.max(50, Math.round(config.waitMinTimeoutMs));
	}
	if (Array.isArray(config.outboxBackoffMs) && config.outboxBackoffMs.length > 0) {
		outboxBackoffMs = config.outboxBackoffMs.filter((value) => Number.isFinite(value) && value >= 10).map((value) => Math.round(value));
		if (outboxBackoffMs.length === 0) outboxBackoffMs = OUTBOX_BACKOFF_MS;
	}
	if (Number.isFinite(config.outboxPumpMs)) {
		outboxPumpMs = Math.max(50, Math.round(config.outboxPumpMs));
	}
	if (Number.isFinite(config.maxCommHops)) {
		maxCommHops = Math.max(1, Math.round(config.maxCommHops));
	}
	const graphDir = path.join(baseDir, 'sessions');
	const packageDir = path.join(baseDir, 'packages');
	const fleetDir = path.join(baseDir, 'fleet');
	const outboxDir = path.join(baseDir, 'outbox');

	const graphs = new Map();
	const watchdogs = new Map();
	const dispatchRuns = new Map();
	const timers = new Set();
	const writeChains = new Map();
	const fleets = new Map();
	const fleetChains = new Map();
	const waitRegistry = new Map(); // wait node id -> { sessionId, graph, targetSessionId, since, mode, marker }
	// Per-session activity for every session this process sees (not just flow
	// sessions): the L0 fleet view and wait steps observe other sessions.
	const activity = new Map(); // sessionId -> { lastTurnEndAt, lastText }
	// Change attribution (report 2026-11-06, L1 bookkeeping): which paths a
	// session's file tools touched, mirroring the recorder's own events so a
	// send step can attach an honest "files this task touched" block.
	const attribution = {
		execSession: new WeakMap(), // tool exec -> root sessionId
		latestTurn: new Map(), // sessionId -> turn
		cwds: new Map(), // sessionId -> cwd
		parents: new Map(), // subagent sessionId -> parent sessionId
		touched: new Map(), // sessionId -> Map(path -> lastAt)
	};

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
			for (const entry of dispatchRuns.values()) entry.abort.abort();
			dispatchRuns.clear();
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
				waitingJudge: typeof run.waitingJudge === 'string' && ids.has(run.waitingJudge) ? run.waitingJudge : null,
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
			if (Array.isArray(node.children)) {
				node.children = node.children.map((child) =>
					child.status === 'pending' || child.status === 'running' ? { ...child, status: 'interrupted' } : child,
				);
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

	//#region fleet & outbox

	function emptyFleet(key) {
		return { version: FLEET_VERSION, key, sessions: [], edges: [], hops: 0, createdAt: Date.now() };
	}

	function fleetFile(key) {
		const safe = String(key).replace(/[^A-Za-z0-9_-]/g, '_').slice(0, 128);
		return path.join(fleetDir, `${safe}.json`);
	}

	function normalizeFleetSession(input) {
		if (input === null || typeof input !== 'object') return undefined;
		const sessionId = typeof input.sessionId === 'string' && /^[A-Za-z0-9_-]{1,128}$/.test(input.sessionId) ? input.sessionId : undefined;
		if (sessionId === undefined) return undefined;
		return {
			nodeId: typeof input.nodeId === 'string' && /^[A-Za-z0-9_-]{1,64}$/.test(input.nodeId) ? input.nodeId : newId('fs'),
			sessionId,
			alias: typeof input.alias === 'string' ? input.alias.slice(0, 60) : '',
			role: typeof input.role === 'string' ? input.role.slice(0, 40) : '',
			note: typeof input.note === 'string' ? input.note.slice(0, 300) : '',
		};
	}

	function normalizeFleetEdge(input, sessions) {
		if (input === null || typeof input !== 'object') return undefined;
		const kinds = new Set(['send', 'wait', 'note']);
		const kind = kinds.has(input.kind) ? input.kind : 'send';
		if (typeof input.from !== 'string' || typeof input.to !== 'string') return undefined;
		const ids = new Set(sessions.map((session) => session.nodeId));
		if (!ids.has(input.from) || !ids.has(input.to)) return undefined;
		return { id: newId('fe'), from: input.from, to: input.to, kind };
	}

	async function ensureFleet(key) {
		const existing = fleets.get(key);
		if (existing !== undefined) return existing;
		let fleet = emptyFleet(key);
		try {
			const parsed = JSON.parse(await readFile(fleetFile(key), 'utf8'));
			if (parsed !== null && typeof parsed === 'object' && Array.isArray(parsed.sessions)) {
				const sessions = parsed.sessions.map(normalizeFleetSession).filter((session) => session !== undefined).slice(0, MAX_FLEET_SESSIONS);
				const edges = Array.isArray(parsed.edges)
					? parsed.edges.map((edge) => normalizeFleetEdge(edge, sessions)).filter((edge) => edge !== undefined).slice(0, MAX_FLEET_EDGES)
					: [];
				// Keep node ids stable across restarts.
				const seen = new Set();
				for (const session of sessions) {
					if (seen.has(session.nodeId)) session.nodeId = newId('fs');
					seen.add(session.nodeId);
				}
				fleet = {
					version: FLEET_VERSION,
					key,
					sessions,
					edges,
					hops: Number.isFinite(parsed.hops) ? Math.max(0, Math.round(parsed.hops)) : 0,
					createdAt: Number.isFinite(parsed.createdAt) ? parsed.createdAt : Date.now(),
				};
			}
		} catch {
			/* no stored fleet yet: start clean */
		}
		fleets.set(key, fleet);
		return fleet;
	}

	function persistFleet(fleet) {
		const file = fleetFile(fleet.key);
		const payload = JSON.stringify({ ...fleet, updatedAt: Date.now() });
		const previous = fleetChains.get(fleet.key) ?? Promise.resolve();
		const next = previous
			.then(async () => {
				await mkdir(fleetDir, { recursive: true });
				const temporary = `${file}.tmp`;
				await writeFile(temporary, payload, 'utf8');
				await rename(temporary, file);
			})
			.catch(() => {});
		fleetChains.set(fleet.key, next);
		return next;
	}

	/** The fleet a session belongs to is keyed by its workspace directory. */
	async function fleetKeyOf(sessionId) {
		try {
			const resolved = await ctx.sessionController.resolveAgent(sessionId);
			const agent = resolved === undefined ? undefined : resolved.agent;
			const session = agent === undefined ? undefined : agent.session;
			let cwd;
			if (session !== undefined) {
				cwd = typeof session.header?.cwd === 'string' && session.header.cwd !== '' ? session.header.cwd : undefined;
				if (cwd === undefined) {
					const workingDirectory = ctx.get('workingDirectory');
					if (workingDirectory !== undefined) cwd = workingDirectory.get(session);
				}
			}
			if (cwd === undefined || cwd === '') return 'default';
			return cwd.replace(/[^A-Za-z0-9_-]/g, '_').slice(0, 120);
		} catch {
			return 'default';
		}
	}

	/** Resolve a send/wait target reference: a fleet node id, or a raw session id. */
	async function resolveTargetRef(sessionId, ref) {
		if (ref === '') return undefined;
		const fleet = await ensureFleet(await fleetKeyOf(sessionId));
		const entry = fleet.sessions.find((session) => session.nodeId === ref || session.sessionId === ref);
		if (entry !== undefined) return { sessionId: entry.sessionId, alias: entry.alias, fleet };
		if (/^[A-Za-z0-9_-]{1,128}$/.test(ref)) return { sessionId: ref, alias: '', fleet };
		return undefined;
	}

	function outboxFile(sessionId) {
		const safe = String(sessionId).replace(/[^A-Za-z0-9_-]/g, '_').slice(0, 128);
		return path.join(outboxDir, `${safe}.jsonl`);
	}

	async function readOutbox(targetSessionId) {
		try {
			const raw = await readFile(outboxFile(targetSessionId), 'utf8');
			const entries = [];
			for (const line of raw.split('\n')) {
				const trimmed = line.trim();
				if (trimmed === '') continue;
				try {
					const parsed = JSON.parse(trimmed);
					if (parsed !== null && typeof parsed === 'object') entries.push(parsed);
				} catch {
					/* skip a corrupt line */
				}
			}
			return entries;
		} catch {
			return [];
		}
	}

	async function writeOutbox(targetSessionId, entries) {
		await mkdir(outboxDir, { recursive: true });
		if (entries.length === 0) {
			await rm(outboxFile(targetSessionId), { force: true });
			return;
		}
		await writeFile(outboxFile(targetSessionId), entries.map((entry) => JSON.stringify(entry)).join('\n') + '\n', 'utf8');
	}

	/**
	 * Deliver one message to another session. Persisted before delivery,
	 * retried with backoff, strictly ordered per target: the head entry of a
	 * target's outbox is the only one in flight (the superagent delivery
	 * discipline, kept for cross-session sends).
	 */
	async function enqueueSend(fromSessionId, targetSessionId, text) {
		const entries = await readOutbox(targetSessionId);
		entries.push({
			id: `ob_${randomUUID().replace(/-/g, '').slice(0, 16)}`,
			from: fromSessionId,
			to: targetSessionId,
			text: text.slice(0, MAX_PROMPT_CHARS),
			attempts: 0,
			nextAt: Date.now(),
			status: 'pending',
			createdAt: Date.now(),
		});
		await writeOutbox(targetSessionId, entries);
		scheduleOutboxPump();
	}

	const outboxPumping = new Set();
	let outboxTimer = undefined;

	async function pumpTarget(targetSessionId) {
		if (outboxPumping.has(targetSessionId)) return;
		outboxPumping.add(targetSessionId);
		try {
			const entries = await readOutbox(targetSessionId);
			// Ordering per target: a dead letter blocks everything behind it
			// until a person revives it (the superagent delivery discipline).
			const head = entries.find((entry) => entry.status === 'pending' || entry.status === 'dead');
			if (head === undefined) return;
			if (head.status === 'dead') return;
			if (Number.isFinite(head.nextAt) && head.nextAt > Date.now()) return;
			try {
				await ctx.sessionController.prompt(
					{
						requestId: `flow-queue-send-${head.id}`,
						sessionId: targetSessionId,
						mode: 'queue',
						content: [{ type: 'text', text: head.text }],
						clientTimeZone: Intl.DateTimeFormat().resolvedOptions().timeZone,
					},
					new AbortController().signal,
				);
				head.status = 'delivered';
				head.deliveredAt = Date.now();
				await writeOutbox(targetSessionId, entries.filter((entry) => entry.status !== 'delivered'));
			} catch (error) {
				head.attempts = (head.attempts ?? 0) + 1;
				const backoff = outboxBackoffMs[Math.min(head.attempts, outboxBackoffMs.length) - 1] ?? 60000;
				if (head.attempts >= OUTBOX_MAX_ATTEMPTS) {
					head.status = 'dead';
					head.error = error instanceof Error ? error.message.slice(0, 200) : String(error).slice(0, 200);
				} else {
					head.nextAt = Date.now() + backoff;
				}
				await writeOutbox(targetSessionId, entries);
			}
		} finally {
			outboxPumping.delete(targetSessionId);
		}
	}

	function scheduleOutboxPump() {
		if (outboxTimer !== undefined) return;
		outboxTimer = track(setTimeout(() => {
			outboxTimer = undefined;
			void pumpOutbox();
		}, Math.min(250, outboxPumpMs)));
	}

	async function pumpOutbox() {
		let entries = [];
		try {
			entries = await readdir(outboxDir);
		} catch {
			return;
		}
		const targets = entries.filter((name) => name.endsWith('.jsonl')).map((name) => name.slice(0, -'.jsonl'.length));
		await Promise.all(targets.map((target) => pumpTarget(target).catch(() => {})));
		// Keep pumping while any outbox still has pending work.
		let pending = false;
		for (const target of targets) {
			const list = await readOutbox(target);
			if (list.some((entry) => entry.status === 'pending')) {
				pending = true;
				break;
			}
		}
		if (pending) scheduleOutboxPump();
	}

	//#endregion

	//#region change attribution

	/** Root session of a lineage: a subagent's file edits count for its parent. */
	function rootSessionOf(sessionId) {
		let current = sessionId;
		const visited = new Set();
		for (let depth = 0; depth < 8; depth += 1) {
			const parent = attribution.parents.get(current);
			if (parent === undefined || visited.has(parent)) return current;
			visited.add(parent);
			current = parent;
		}
		return current;
	}

	function canonicalPath(target, cwd) {
		let absolute = target;
		const fsService = ctx.get('fs');
		if (fsService !== undefined && typeof fsService.processPath === 'function') {
			try {
				absolute = fsService.processPath(target);
			} catch {
				/* fall back to plain resolution */
			}
		}
		if (typeof absolute !== 'string') absolute = String(target);
		if (!path.isAbsolute(absolute)) absolute = path.resolve(cwd ?? process.cwd(), absolute);
		absolute = path.normalize(absolute);
		return process.platform === 'win32' ? absolute.toLowerCase() : absolute;
	}

	// The attribution bookkeeping mirrors the recorder's own events (proven by
	// dsh-usage-replay's sessionScopedEdits): map each tool execution to its
	// root session, then record fs write/edit intents under that session.
	ctx.on('tools/pre-execute', (exec, next) => {
		try {
			const session = exec === undefined ? undefined : exec.agent?.session;
			if (session !== undefined) attribution.execSession.set(exec, rootSessionOf(session.id));
		} catch {
			/* attribution must never break a tool call */
		}
		return next();
	});
	const intentListener = async (target, actor, next) => {
		const intent = await next();
		try {
			const sessionId = actor === undefined || actor === null ? undefined : attribution.execSession.get(actor);
			if (sessionId === undefined) return intent;
			const absolute = canonicalPath(target, attribution.cwds.get(sessionId));
			let touched = attribution.touched.get(sessionId);
			if (touched === undefined) {
				touched = new Map();
				attribution.touched.set(sessionId, touched);
			}
			touched.set(absolute, Date.now());
		} catch {
			/* attribution must never break the write */
		}
		return intent;
	};
	ctx.on('fs/write-intent', intentListener);
	ctx.on('fs/edit-intent', intentListener);

	/**
	 * Honest v1 change block for a send step: only paths this session's file
	 * tools touched since the run started, method-labelled, with the blind
	 * spots named. No window diff join here — that needs live
	 * workspaceChanges and is deliberately deferred; the method field says
	 * exactly what this block is (report invariants 1 and 3).
	 */
	function changeBlockOf(sessionId, since) {
		const touched = attribution.touched.get(sessionId);
		if (touched === undefined || touched.size === 0) {
			return { method: 'handoff-only', text: '<flow-changes method="handoff-only">未观察到本会话经文件工具的改动；shell-only 改动与并发他话改动不在统计内。</flow-changes>' };
		}
		const paths = [...touched.entries()]
			.filter(([, at]) => at >= since)
			.sort((left, right) => left[1] - right[1])
			.map(([file]) => file)
			.slice(0, 200);
		if (paths.length === 0) {
			return { method: 'handoff-only', text: '<flow-changes method="handoff-only">本次运行开始后未观察到本会话经文件工具的改动。</flow-changes>' };
		}
		return {
			method: 'intent-only',
			text: `<flow-changes method="intent-only" session="${sessionId}" count="${paths.length}">\n${paths.map((file) => `- ${file}`).join('\n')}\n说明：仅统计本会话（含其子代理）经 DSH 文件工具写入的路径；shell 命令产生的改动与并发其他会话的改动不在此列。</flow-changes>`,
		};
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
		// The judge verdict is consumed on read; never expose the stale copy.
		delete view.judge;
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
			graph.run.waitingJudge = null;
			graph.run.state = 'error';
			graph.run.message = 'no-turn';
			await persist(sessionId, current);
		}, DISPATCH_WATCHDOG_MS);
		watchdogs.set(node.id, handle);
	}

	/**
	 * Dispatch the one-turn judge prompt for a branch or loop whose condition is
	 * natural language. The verdict lands on `node.judge` when that turn ends;
	 * the next advance consumes it and picks the port. Each loop iteration asks
	 * again — a judge answer is never reused across rounds.
	 */
	async function dispatchJudge(sessionId, graph, node) {
		touchNode(node, 'running');
		delete node.judge;
		node.result = undefined;
		graph.run.waitingJudge = node.id;
		await persist(sessionId, graph);
		await ctx.sessionController.prompt(
			{
				requestId: `flow-queue-judge-${randomUUID()}`,
				sessionId,
				mode: 'queue',
				content: [{ type: 'text', text: `${JUDGE_INSTRUCTION}\nCondition: ${node.condition.value}` }],
				clientTimeZone: Intl.DateTimeFormat().resolvedOptions().timeZone,
			},
			new AbortController().signal,
		);
		armWatchdog(sessionId, graph, node);
		return { wait: true };
	}

	/** Resolve a judge condition against the consumed verdict; negate applies. */
	function judgeVerdict(node) {
		const answer = node.judge.answer;
		// Consume on read: a later visit of the same node asks again.
		delete node.judge;
		return node.condition.negate === true ? !answer : answer;
	}

	//#region dispatch fan-out

	/**
	 * Fan one prompt out to N throwaway subagents through the in-process
	 * `spawn` backend and collect their outputs. Completion arrives through
	 * each activation's own result promise, so the parent Session's turn is
	 * never involved — the fan-out step waits beside the turn boundary, it
	 * never steps across it. A target with provider+model passes them as the
	 * child's AgentOptions; an empty target inherits the session route.
	 */
	async function startDispatch(sessionId, graph, node) {
		const subagents = ctx.get('subagents');
		if (subagents === undefined || typeof subagents.startActivation !== 'function') {
			touchNode(node, 'failed');
			node.error = fail('no-subagents', 'the subagents service is not available in this profile');
			return { port: 'next' };
		}
		if (typeof subagents.getProvider === 'function' && subagents.getProvider('spawn') === undefined) {
			touchNode(node, 'failed');
			node.error = fail('no-subagents', 'the in-process spawn backend is not registered');
			return { port: 'next' };
		}
		const resolved = await ctx.sessionController.resolveAgent(sessionId);
		if (resolved === undefined || resolved.agent === undefined) {
			touchNode(node, 'failed');
			node.error = fail('no-agent', 'the Session could not be resumed');
			return { port: 'next' };
		}
		const text = typeof node.text === 'string' ? node.text.trim() : '';
		if (text === '') {
			touchNode(node, 'failed');
			node.error = fail('empty-prompt', 'this step has no prompt text for the subagents');
			return { port: 'next' };
		}
		touchNode(node, 'running');
		node.children = node.targets.map((target) => ({
			label: target.label,
			provider: target.provider,
			model: target.model,
			status: 'pending',
			summary: '',
		}));
		graph.run.waitingNode = node.id;
		await persist(sessionId, graph);

		const total = node.targets.length;
		const needed = node.collect === 'first' ? 1 : node.collect === 'k-of-n' ? Math.min(Math.max(node.k ?? 1, 1), total) : total;
		const abort = new AbortController();
		const run = { abort, activations: [] };
		dispatchRuns.set(node.id, run);
		let settledDone = 0;
		let settledAll = 0;

		const finish = () => {
			// dispatchRuns membership is the once-latch: stop, timeout and the
			// last settle all funnel here, and only the first one runs.
			if (dispatchRuns.get(node.id) !== run) return;
			if (run.timeout !== undefined) {
				clearTimeout(run.timeout);
				timers.delete(run.timeout);
			}
			dispatchRuns.delete(node.id);
			const children = Array.isArray(node.children) ? node.children : [];
			const done = children.filter((child) => child.status === 'done');
			const sections = children.map((child) => {
				const route = child.provider !== '' && child.model !== '' ? `${child.provider}/${child.model}` : 'inherit';
				return `## ${child.label} (${route}) ${child.status}\n${child.summary}`;
			});
			const ok = done.length > 0;
			node.result = `${done.length}/${children.length} done`;
			graph.outcome = { kind: 'dispatch', text: sections.join('\n\n').slice(0, 20000), ok, toolErrors: [] };
			touchNode(node, ok ? 'completed' : 'failed');
			if (!ok) node.error = fail('dispatch-failed', 'every subagent failed or timed out');
			graph.run.waitingNode = null;
			const next = followEdge(graph, node.id, 'next');
			if (next === undefined) finishRun(graph, 'done', 'no outgoing connection');
			else graph.run.cursor = next;
			void persist(sessionId, graph);
			if (graph.run.state === 'running') later(() => advance(sessionId), SETTLE_MS);
		};

		run.timeout = later(() => {
			if (dispatchRuns.get(node.id) !== run) return;
			for (const child of node.children) {
				if (child.status === 'pending' || child.status === 'running') child.status = 'timeout';
			}
			void persist(sessionId, graph);
			finish();
		}, node.timeoutMs);

		const settle = (index, status, summary) => {
			if (dispatchRuns.get(node.id) !== run) return;
			const child = node.children[index];
			if (child === undefined || (child.status !== 'pending' && child.status !== 'running')) return;
			child.status = status;
			child.summary = summary;
			settledAll += 1;
			if (status === 'done') settledDone += 1;
			const satisfied = settledDone >= needed;
			if (satisfied) {
				// k-of-n / first: whatever is still in flight is no longer needed.
				abort.abort();
				for (const other of node.children) {
					if (other.status === 'pending' || other.status === 'running') {
						other.status = 'cancelled';
						settledAll += 1;
					}
				}
			}
			void persist(sessionId, graph);
			if (satisfied || settledAll >= total) finish();
		};

		for (let index = 0; index < node.targets.length; index += 1) {
			const target = node.targets[index];
			const child = node.children[index];
			const request = { prompt: [{ type: 'text', text }], parent: resolved.agent };
			if (target.provider !== '' && target.model !== '') {
				const agentOptions = { provider: target.provider, model: target.model };
				if (typeof target.reasoningEffort === 'string' && target.reasoningEffort !== '') agentOptions.reasoningEffort = target.reasoningEffort;
				request.agentOptions = agentOptions;
			}
			try {
				const activation = await subagents.startActivation({
					provider: 'spawn',
					label: `${node.title !== undefined ? node.title : 'fan-out'} · ${target.label}`,
					delivery: 'caller',
					request,
					signal: abort.signal,
				});
				run.activations.push(activation);
				child.childId = activation.childId;
				child.status = 'running';
				void persist(sessionId, graph);
				activation.result
					.then((result) => {
						const output = result === undefined || result === null ? '' : textOfBlocks(result.output);
						settle(index, 'done', output.slice(0, 600));
					})
					.catch((error) => {
						settle(index, 'failed', error instanceof Error ? error.message.slice(0, 300) : String(error).slice(0, 300));
					});
			} catch (error) {
				settle(index, 'failed', error instanceof Error ? error.message.slice(0, 300) : String(error).slice(0, 300));
			}
		}
		return { wait: true };
	}

	/** Cancel every in-flight fan-out of one graph (run.stop and unload paths). */
	function cancelDispatches(graph) {
		for (const node of graph.nodes) {
			if (node.kind !== 'dispatch') continue;
			const run = dispatchRuns.get(node.id);
			if (run === undefined) continue;
			run.abort.abort();
			for (const activation of run.activations) {
				try {
					void activation.dispose();
				} catch {
					/* best effort: dispose must never break a stop */
				}
			}
			if (run.timeout !== undefined) {
				clearTimeout(run.timeout);
				timers.delete(run.timeout);
			}
			dispatchRuns.delete(node.id);
			for (const child of node.children ?? []) {
				if (child.status === 'pending' || child.status === 'running') child.status = 'cancelled';
			}
			if (node.status === 'running') {
				touchNode(node, 'failed');
				node.error = fail('dispatch-stopped', 'the run was stopped before the fan-out settled');
			}
		}
	}

	//#endregion

	//#region cross-session waits

	function clearWaitWatchdog(nodeId) {
		clearWatchdog(nodeId);
	}

	function armWaitWatchdog(sessionId, graph, node) {
		clearWatchdog(node.id);
		const handle = later(async () => {
			if (node.status !== 'running') return;
			const entry = waitRegistry.get(node.id);
			if (entry === undefined) return;
			waitRegistry.delete(node.id);
			touchNode(node, 'failed');
			node.error = fail('wait-timeout', `the target did not satisfy "${node.until}" within the timeout`);
			if (graph.run.waitingNode === node.id) graph.run.waitingNode = null;
			const next = followEdge(graph, node.id, 'next');
			if (next === undefined) finishRun(graph, 'error', 'wait-timeout');
			else {
				graph.run.cursor = next;
				if (graph.run.state === 'running') later(() => advance(sessionId), SETTLE_MS);
			}
			await persist(sessionId, graph);
		}, node.timeoutMs);
		watchdogs.set(node.id, handle);
	}

	/** Try to settle every registered wait whose target just changed. */
	function probeWaits() {
		for (const [nodeId, entry] of [...waitRegistry.entries()]) {
			const { graph, node, sessionId } = entry;
			if (graph.run.waitingNode !== nodeId || node.status !== 'running') {
				waitRegistry.delete(nodeId);
				continue;
			}
			const seen = activity.get(entry.targetSessionId);
			if (seen === undefined) continue;
			let satisfied = false;
			if (entry.mode === 'marker') {
				satisfied = typeof seen.lastText === 'string' && seen.lastText.includes(entry.marker);
			} else {
				// idle: the target finished at least one turn after this wait
				// started and is not mid-turn right now.
				satisfied = Number.isFinite(seen.lastTurnEndAt) && seen.lastTurnEndAt >= entry.since && isIdle(entry.targetSessionId);
			}
			if (!satisfied) continue;
			waitRegistry.delete(nodeId);
			clearWaitWatchdog(nodeId);
			touchNode(node, 'completed');
			node.result = entry.mode === 'marker' ? `收到标记 ${entry.marker}` : '目标已空闲';
			graph.run.waitingNode = null;
			const next = followEdge(graph, node.id, 'next');
			if (next === undefined) finishRun(graph, 'done', 'no outgoing connection');
			else graph.run.cursor = next;
			void persist(sessionId, graph);
			if (graph.run.state === 'running') later(() => advance(sessionId), SETTLE_MS);
		}
	}

	/** Cancel in-flight waits of one graph (run.stop). */
	function cancelWaits(graph) {
		for (const node of graph.nodes) {
			if (node.kind !== 'wait' || node.status !== 'running') continue;
			waitRegistry.delete(node.id);
			clearWaitWatchdog(node.id);
			touchNode(node, 'failed');
			node.error = fail('wait-stopped', 'the run was stopped while waiting');
		}
		if (graph.run.waitingNode !== null) {
			const waiting = graph.run.waitingNode === null ? undefined : findNode(graph, graph.run.waitingNode);
			if (waiting !== undefined && waiting.kind === 'wait') graph.run.waitingNode = null;
		}
	}

	//#endregion

	function finishRun(graph, state, message) {
		graph.run.state = state;
		graph.run.message = message ?? '';
		graph.run.waitingNode = null;
		graph.run.waitingJudge = null;
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
				// A natural-language condition first needs its judge turn; the
				// verdict arrives on node.judge when that turn ends.
				if (node.condition.type === 'judge' && node.judge === undefined) {
					return dispatchJudge(sessionId, graph, node);
				}
				touchNode(node, 'running');
				const ok = node.condition.type === 'judge' ? judgeVerdict(node) : evaluate(node.condition, graph.outcome);
				touchNode(node, 'completed');
				node.title = ok ? '条件成立' : '条件不成立';
				return { port: ok ? 'yes' : 'no' };
			}
			case 'loop': {
				if (node.condition.type === 'judge' && node.judge === undefined) {
					return dispatchJudge(sessionId, graph, node);
				}
				const used = graph.counters[node.id] ?? 0;
				const done = node.condition.type === 'judge' ? judgeVerdict(node) : evaluate(node.condition, graph.outcome);
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
			case 'dispatch': {
				// Already fanning out: wait beside the boundary until the
				// activations settle and move the cursor themselves.
				if (
					node.status === 'running'
					&& Array.isArray(node.children)
					&& node.children.some((child) => child.status === 'pending' || child.status === 'running')
				) {
					return { wait: true };
				}
				return startDispatch(sessionId, graph, node);
			}
			case 'send': {
				const text = typeof node.text === 'string' ? node.text.trim() : '';
				if (text === '') {
					touchNode(node, 'failed');
					node.error = fail('empty-prompt', 'this step has no message text');
					return { port: 'next' };
				}
				const target = await resolveTargetRef(sessionId, node.targetRef);
				if (target === undefined) {
					touchNode(node, 'failed');
					node.error = fail('no-target', 'the target session is not in the fleet and not a valid session id');
					return { port: 'next' };
				}
				// Hop guard rides with the feature (discipline: guardrails
				// never ship later than the capability they cap).
				if (target.fleet.hops >= maxCommHops) {
					touchNode(node, 'failed');
					node.error = fail('hop-limit', `the fleet reached the ${maxCommHops}-send cap; reset hops in the fleet view to continue`);
					return { port: 'next' };
				}
				target.fleet.hops += 1;
				void persistFleet(target.fleet);
				touchNode(node, 'running');
				await persist(sessionId, graph);
				const payload = node.attachChanges === true
					? `${text}\n\n${changeBlockOf(sessionId, graph.run.startedAt || node.startedAt || Date.now()).text}`
					: text;
				node.target = target.sessionId.slice(0, MAX_IDENT_CHARS);
				await enqueueSend(sessionId, target.sessionId, payload);
				touchNode(node, 'completed');
				node.result = `已入队 → ${target.alias !== '' ? target.alias : target.sessionId.slice(0, 12)}（先存后发）`;
				return { port: 'next' };
			}
			case 'wait': {
				const target = await resolveTargetRef(sessionId, node.targetRef);
				if (target === undefined) {
					touchNode(node, 'failed');
					node.error = fail('no-target', 'the target session is not in the fleet and not a valid session id');
					return { port: 'next' };
				}
				touchNode(node, 'running');
				node.target = target.sessionId.slice(0, MAX_IDENT_CHARS);
				graph.run.waitingNode = node.id;
				await persist(sessionId, graph);
				waitRegistry.set(node.id, {
					sessionId,
					graph,
					node,
					targetSessionId: target.sessionId,
					since: Date.now(),
					mode: node.until,
					marker: node.marker,
				});
				armWaitWatchdog(sessionId, graph, node);
				// Already satisfied (target idle with a fresh-enough turn, or
				// marker in its latest text)? Settle without waiting an event.
				later(() => probeWaits(), SETTLE_MS);
				return { wait: true };
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

		// Attribution bookkeeping runs for every session this process sees:
		// lineage (subagent -> parent), the session's cwd and latest turn.
		try {
			const header = session === undefined ? undefined : session.header;
			if (header !== null && typeof header === 'object') {
				if (typeof header.parentSession === 'string' && header.parentSession !== '') attribution.parents.set(sessionId, header.parentSession);
				if (typeof header.cwd === 'string' && header.cwd !== '') attribution.cwds.set(sessionId, header.cwd);
			}
			if (event.type === 'turn/start' && typeof event.data?.turn === 'number') attribution.latestTurn.set(sessionId, event.data.turn);
		} catch {
			/* bookkeeping must never break event handling */
		}

		// Fleet-level activity for every session: the L0 view and wait steps
		// observe sessions that have no flow graph of their own.
		if (event.type === 'assistant/message') {
			const data = event.data ?? {};
			const text = textOfBlocks(data.message === undefined ? [] : data.message.content);
			const seen = activity.get(sessionId) ?? { lastTurnEndAt: 0, lastText: '' };
			if (text !== '') seen.lastText = text;
			activity.set(sessionId, seen);
		}

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
			const promptNode = runningPromptNode(graph);
			const judgeNode = graph.run.waitingJudge === null ? undefined : findNode(graph, graph.run.waitingJudge);
			if (promptNode === undefined && judgeNode === undefined) return;
			const turn = event.data === undefined ? undefined : event.data.turn;
			for (const active of [promptNode, judgeNode]) {
				if (active === undefined) continue;
				if (typeof turn === 'number') active.turn = turn;
				clearWatchdog(active.id);
			}
			return;
		}

		if (event.type !== 'turn/end') return;
		{
			const seen = activity.get(sessionId) ?? { lastTurnEndAt: 0, lastText: '' };
			seen.lastTurnEndAt = Date.now();
			activity.set(sessionId, seen);
		}
		probeWaits();
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
		// A judge turn just settled: parse its verdict off the same turn's text
		// and leave the cursor on the branch/loop node so the next advance takes
		// the yes/no edge.
		if (graph.run.waitingJudge !== null) {
			const judgeNode = findNode(graph, graph.run.waitingJudge);
			graph.run.waitingJudge = null;
			if (judgeNode !== undefined) {
				clearWatchdog(judgeNode.id);
				const lastTurn = graph.lastTurn !== undefined && graph.lastTurn.turn === data.turn ? graph.lastTurn : undefined;
				const answer = parseJudgeAnswer(lastTurn === undefined ? '' : lastTurn.text);
				if (answer === undefined) {
					touchNode(judgeNode, 'failed');
					judgeNode.error = fail('no-decision', 'the judge turn did not answer YES or NO on its first line');
					graph.run.state = 'error';
					graph.run.message = 'no-decision';
				} else {
					judgeNode.judge = { answer };
					const reason = lastTurn === undefined || lastTurn.text === undefined ? '' : lastTurn.text.split('\n').slice(1).join(' ').trim().slice(0, 160);
					judgeNode.result = `${answer ? 'YES' : 'NO'}${reason === '' ? '' : ` · ${reason}`}`;
					graph.outcome = { kind: 'judge', text: `${judgeNode.condition.value}\n${lastTurn?.text ?? ''}`, ok: answer, toolErrors: [] };
				}
				void persist(sessionId, graph);
			}
		}
		if (graph.run.state === 'running') later(() => advance(sessionId), SETTLE_MS);
	});

	ctx.on('agent/status', (payload) => {
		const agent = payload === undefined ? undefined : payload.agent;
		const status = payload === undefined ? undefined : payload.status;
		if (agent === undefined || typeof agent.id !== 'string') return;
		if (status === 'idle') {
			// A wait target going idle may settle a registered wait.
			probeWaits();
		}
		if (status !== 'idle') return;
		const sessionId = agent.id;
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
				delete copy.children;
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
		if (node.kind === 'dispatch') {
			if (Array.isArray(patch.targets)) node.targets = normalizeDispatchTargets(patch.targets);
			if (DISPATCH_COLLECT_MODES.has(patch.collect)) {
				node.collect = patch.collect;
				if (patch.collect === 'first') node.k = 1;
			}
			const total = Math.max(Array.isArray(node.targets) ? node.targets.length : 1, 1);
			if (Number.isFinite(patch.k)) node.k = Math.min(Math.max(Math.round(patch.k), 1), total);
			if (Number.isFinite(patch.timeoutMs)) node.timeoutMs = Math.min(Math.max(Math.round(patch.timeoutMs), dispatchMinTimeoutMs), DISPATCH_MAX_TIMEOUT_MS);
		}
		if (node.kind === 'send') {
			if (typeof patch.targetRef === 'string') node.targetRef = patch.targetRef.slice(0, MAX_IDENT_CHARS);
			if (patch.attachChanges !== undefined) node.attachChanges = patch.attachChanges === true;
		}
		if (node.kind === 'wait') {
			if (typeof patch.targetRef === 'string') node.targetRef = patch.targetRef.slice(0, MAX_IDENT_CHARS);
			if (WAIT_MODES.has(patch.until)) node.until = patch.until;
			if (typeof patch.marker === 'string') node.marker = patch.marker.trim().slice(0, 200) === '' ? DEFAULT_REPORT_MARKER : patch.marker.trim().slice(0, 200);
			if (Number.isFinite(patch.timeoutMs)) node.timeoutMs = Math.min(Math.max(Math.round(patch.timeoutMs), waitMinTimeoutMs), WAIT_MAX_TIMEOUT_MS);
		}
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
					targets: payload.targets,
					collect: payload.collect,
					k: payload.k,
					timeoutMs: payload.timeoutMs,
					targetRef: payload.targetRef,
					until: payload.until,
					marker: payload.marker,
					attachChanges: payload.attachChanges,
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
					delete node.judge;
					delete node.children;
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
					delete node.judge;
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
				cancelDispatches(graph);
				cancelWaits(graph);
				graph.run.state = 'idle';
				graph.run.cursor = null;
				graph.run.waitingNode = null;
				graph.run.waitingJudge = null;
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

	ctx.effect(
		() =>
			ctx.webServer.register({
				kind: 'exact',
				path: `${PREFIX}/fleet`,
				handler: async (req, res) => {
					if (!admitted(req)) return writeJson(res, 403, { ok: false, error: fail('forbidden', 'forbidden') });
					if (req.method === 'GET' || req.method === 'HEAD') {
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
							const fleet = await ensureFleet(await fleetKeyOf(sessionId));
							const agents = ctx.get('agents');
							const tokenMeter = ctx.get('tokenMeter');
							const sessionViews = [];
							for (const entry of fleet.sessions) {
								const agent = agents === undefined ? undefined : agents.get(entry.sessionId);
								const seen = activity.get(entry.sessionId);
								let outbox = { pending: 0, dead: 0 };
								try {
									const entries = await readOutbox(entry.sessionId);
									outbox = {
										pending: entries.filter((item) => item.status === 'pending').length,
										dead: entries.filter((item) => item.status === 'dead').length,
									};
								} catch {
									/* keep the zero summary */
								}
								// Context-surface estimate (tokenMeter.measure), not a
								// cost meter: a big number here is the "this role's
								// conversation is rotting" early warning.
								let contextTokens;
								if (tokenMeter !== undefined && typeof tokenMeter.measure === 'function' && agent !== undefined && agent.session !== undefined) {
									try {
										const measurement = tokenMeter.measure(agent.session);
										if (measurement !== undefined && Number.isFinite(measurement.totalTokens)) contextTokens = measurement.totalTokens;
									} catch {
										/* the badge is optional */
									}
								}
								const view = {
									...entry,
									agentStatus: agent === undefined ? 'offline' : agent.status === 'running' ? 'running' : 'idle',
									lastTurnEndAt: seen === undefined ? 0 : seen.lastTurnEndAt,
									outbox,
								};
								if (contextTokens !== undefined) view.contextTokens = contextTokens;
								sessionViews.push(view);
							}
							writeJson(res, 200, { ok: true, value: { fleet, sessions: sessionViews, maxHops: maxCommHops } });
						} catch (error) {
							writeJson(res, 500, { ok: false, error: fail('fleet-failed', error instanceof Error ? error.message : String(error)) });
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
						const fleet = await ensureFleet(await fleetKeyOf(sessionId));
						switch (payload.op) {
							case 'session.add': {
								if (fleet.sessions.length >= MAX_FLEET_SESSIONS) throw fail('fleet-full', `the fleet already has ${MAX_FLEET_SESSIONS} sessions`);
								const session = normalizeFleetSession({ sessionId: payload.targetSessionId, alias: payload.alias, role: payload.role, note: payload.note });
								if (session === undefined) throw fail('invalid-session', 'a valid targetSessionId is required');
								if (fleet.sessions.some((entry) => entry.sessionId === session.sessionId)) throw fail('fleet-duplicate', 'that session is already in the fleet');
								fleet.sessions.push(session);
								await persistFleet(fleet);
								return writeJson(res, 200, { ok: true, value: { session } });
							}
							case 'session.update': {
								const entry = fleet.sessions.find((candidate) => candidate.nodeId === payload.nodeId);
								if (entry === undefined) throw fail('invalid-node', 'no such fleet session');
								if (typeof payload.alias === 'string') entry.alias = payload.alias.slice(0, 60);
								if (typeof payload.role === 'string') entry.role = payload.role.slice(0, 40);
								if (typeof payload.note === 'string') entry.note = payload.note.slice(0, 300);
								await persistFleet(fleet);
								return writeJson(res, 200, { ok: true, value: { session: entry } });
							}
							case 'session.remove': {
								const nodeId = typeof payload.nodeId === 'string' ? payload.nodeId : '';
								fleet.sessions = fleet.sessions.filter((candidate) => candidate.nodeId !== nodeId);
								fleet.edges = fleet.edges.filter((edge) => edge.from !== nodeId && edge.to !== nodeId);
								await persistFleet(fleet);
								return writeJson(res, 200, { ok: true, value: { removed: nodeId } });
							}
							case 'edge.add': {
								const edge = normalizeFleetEdge({ from: payload.from, to: payload.to, kind: payload.kind }, fleet.sessions);
								if (edge === undefined) throw fail('invalid-edge', 'both ends must be fleet sessions');
								if (fleet.edges.length >= MAX_FLEET_EDGES) throw fail('fleet-full', `the fleet already has ${MAX_FLEET_EDGES} connections`);
								fleet.edges.push(edge);
								await persistFleet(fleet);
								return writeJson(res, 200, { ok: true, value: { edge } });
							}
							case 'edge.remove': {
								fleet.edges = fleet.edges.filter((edge) => edge.id !== payload.id);
								await persistFleet(fleet);
								return writeJson(res, 200, { ok: true, value: { removed: payload.id } });
							}
							case 'reset-hops': {
								fleet.hops = 0;
								await persistFleet(fleet);
								return writeJson(res, 200, { ok: true, value: { hops: 0 } });
							}
							case 'outbox.retry': {
								const target = typeof payload.targetSessionId === 'string' ? payload.targetSessionId : '';
								const entries = await readOutbox(target);
								let revived = 0;
								for (const entry of entries) {
									if (entry.status !== 'dead') continue;
									entry.status = 'pending';
									entry.attempts = 0;
									entry.nextAt = Date.now();
									delete entry.error;
									revived += 1;
								}
								if (revived > 0) {
									await writeOutbox(target, entries);
									scheduleOutboxPump();
								}
								return writeJson(res, 200, { ok: true, value: { revived } });
							}
							case 'stop': {
								// Fleet-wide emergency stop: pause every live run
								// (cancelling fan-outs and waits) and cancel every
								// pending delivery. Idempotent by construction.
								let paused = 0;
								for (const graph of graphs.values()) {
									if (graph.run.state !== 'running' && graph.run.state !== 'waiting') continue;
									cancelDispatches(graph);
									cancelWaits(graph);
									graph.run.state = 'paused';
									graph.run.message = 'fleet stopped';
									graph.run.waitingNode = null;
									graph.run.waitingJudge = null;
									void persist(graph.sessionId, graph);
									paused += 1;
								}
								let cleared = 0;
								let files = [];
								try {
									files = await readdir(outboxDir);
								} catch {
									files = [];
								}
								for (const name of files) {
									if (!name.endsWith('.jsonl')) continue;
									const target = name.slice(0, -'.jsonl'.length);
									const entries = await readOutbox(target);
									let dirty = false;
									for (const entry of entries) {
										if (entry.status !== 'pending') continue;
										entry.status = 'cancelled';
										dirty = true;
										cleared += 1;
									}
									if (dirty) await writeOutbox(target, entries);
								}
								return writeJson(res, 200, { ok: true, value: { paused, cleared } });
							}
							default:
								throw fail('invalid-command', 'unknown op');
						}
					} catch (error) {
						if (error !== null && typeof error === 'object' && typeof error.code === 'string') {
							return writeJson(res, 400, { ok: false, error });
						}
						return writeJson(res, 500, { ok: false, error: fail('fleet-failed', error instanceof Error ? error.message : String(error)) });
					}
				},
			}),
		'dsh-flow-queue: fleet route',
	);

	// The report protocol ships as a loadable flow package, not engine logic:
	// a worker-prompt template that pins the four-state turn-end marker, and
	// the matching marker wait. Written once ('wx'), never overwritten — an
	// edited copy stays edited.
	void (async () => {
		try {
			await mkdir(packageDir, { recursive: true });
			const promptId = newId();
			const waitId = newId();
			const body = {
				version: STORE_VERSION,
				id: 'report-protocol',
				name: '报告协议（四态标记）',
				description: 'worker 提示词模板：轮末输出 [[FLOW:REPORT waiting|ready|blocked|failed]]＋摘要；后接等标记的 wait 步骤。载入后把 wait 的目标选成编制里的 worker 会话。',
				createdAt: 0,
				entry: promptId,
				nodes: [
					{
						id: promptId,
						kind: 'prompt',
						x: 40,
						y: 40,
						text: '【任务】\n（在这里写下这个 worker 要做的事）\n\n【报告纪律】每轮结束的最后一行必须输出且只输出一行报告标记：\n[[FLOW:REPORT waiting]] 继续工作中\n[[FLOW:REPORT ready]] 已完成，待审阅\n[[FLOW:REPORT blocked]] 被阻塞，需要人或其他会话介入\n[[FLOW:REPORT failed]] 失败，说明原因\n标记后跟一句中文摘要。',
					},
					{ id: waitId, kind: 'wait', x: 40, y: 200, targetRef: '', until: 'marker', marker: '[[FLOW:REPORT ready]]', timeoutMs: WAIT_DEFAULT_TIMEOUT_MS },
				],
				edges: [{ id: newId('e'), from: promptId, port: 'next', to: waitId }],
			};
			await writeFile(path.join(packageDir, 'report-protocol.json'), JSON.stringify(body, null, '\t'), { encoding: 'utf8', flag: 'wx' });
		} catch {
			/* already present, or the package dir is read-only: either way the
			   library simply lists what is on disk */
		}
	})();

	//#endregion
}
