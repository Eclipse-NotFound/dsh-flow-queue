/* global window, document, fetch, setInterval, clearInterval, FileReader, btoa, Intl */
// dsh-flow-queue — client half (v2: visual flow editor).
//
// Two surfaces share one Host graph:
//   • a right-sidebar tab: compact run state + step list + run controls;
//   • a frame-wide overlay editor: canvas with draggable steps, connections,
//     an inspector for the selected step, and a package library.
// Everything is staged on the Host and executed only at a turn boundary, so
// the visual order is exactly the execution order.
window.__ModuleLoader__.load({
	id: 'dsh-flow-queue',
	factory(require) {
		const React = require('react');
		const { useCallback, useEffect, useMemo, useRef, useState } = React;
		const h = React.createElement;

		const TAB_ID = 'flow-queue';
		const NS = 'flow-queue';
		const POLL_MS = 1200;
		const NODE_W = 196;
		const PORT_GAP = 22;

		let localeSvc = null;
		let pluginCtx = null;

		const DICT = {
			zh: {
				'tab.title': '流程队列',
				'tab.guide.description': '把提示词、切模型、命令、条件分支与循环画成一张流程图；只在轮次边界执行，执行到哪一步一眼可见。',
				'trigger.open': '打开流程队列',
				'overlay.title': '流程编辑器',
				'overlay.close': '关闭',
				'run.idle': '未运行',
				'run.running': '运行中',
				'run.paused': '已暂停',
				'run.waiting': '等待继续',
				'run.done': '已完成',
				'run.error': '出错',
				'run.steps': '{n} 步',
				'action.start': '开始',
				'action.pause': '暂停',
				'action.resume': '继续',
				'action.stop': '停止',
				'action.reset': '重置',
				'action.clear': '清空',
				'action.layout': '自动排版',
				'action.library': '流程包',
				'action.back': '返回',
				'action.menu': '更多操作',
				'action.openEditor': '打开编辑器',
				'action.from': '从此步开始',
				'action.add': '添加',
				'action.delete': '删除',
				'action.detach': '移除附件',
				'action.upload': '添加附件',
				'kind.prompt': '提示词',
				'kind.model': '切换模型',
				'kind.command': '执行命令',
				'kind.branch': '条件分支',
				'kind.loop': '循环',
				'kind.checkpoint': '断点',
				'status.idle': '待运行',
				'status.running': '执行中',
				'status.completed': '完成',
				'status.failed': '失败',
				'status.skipped': '跳过',
				'status.waiting': '等待继续',
				'palette.title': '步骤',
				'palette.hint': '点一个步骤加到画布；从右侧圆点拖到另一个步骤即可连线。',
				'inspector.empty': '选中一个步骤来编辑它；点画布空白处取消选择。',
				'inspector.title': '编辑步骤',
				'inspector.text': '提示词内容',
				'inspector.command': '命令（例如 /compact）',
				'inspector.model': '模型',
				'inspector.modelInherit': '不设置（沿用当前）',
				'inspector.note': '断点说明',
				'inspector.condition': '条件',
				'inspector.conditionValue': '匹配内容',
				'inspector.negate': '取反（条件不成立时走「是」）',
				'inspector.maxIterations': '最多轮数',
				'inspector.attachments': '附件',
				'inspector.noAttachments': '还没有附件。图片与文件都会随这一条提示词一起发出。',
				'inspector.usedModel': '实际模型',
				'inspector.turn': '轮次',
				'inspector.error': '错误',
				'inspector.result': '结果',
				'cond.always': '总是成立',
				'cond.regex': '正则匹配上一轮输出',
				'cond.contains': '包含文本',
				'cond.marker': '包含完成标记 [[FLOW:DONE]]',
				'cond.tool-failed': '上一轮有工具失败',
				'cond.last-failed': '上一步失败',
				'cond.value.always': '',
				'cond.value.regex': '(测试通过|all tests passed)',
				'cond.value.contains': '关键词',
				'cond.value.marker': '[[FLOW:DONE]]',
				'cond.value.tool-failed': '',
				'cond.value.last-failed': '',
				'port.next': '下一步',
				'port.yes': '是',
				'port.no': '否',
				'port.again': '再循环',
				'port.exit': '退出循环',
				'canvas.empty': '画布还是空的：从左边点一个「提示词」开始。',
				'canvas.connectHint': '正在连线：点目标步骤完成连接，Esc 取消。',
				'canvas.edgeHint': '点连线可断开它。',
				'library.title': '流程包',
				'library.save': '把当前流程存为流程包',
				'library.name': '名称',
				'library.description': '说明',
				'library.empty': '还没有流程包。',
				'library.load': '载入',
				'library.delete': '删除',
				'library.saved': '已保存',
				'empty.title': '还没有步骤',
				'empty.hint': '打开编辑器，把提示词、切模型、命令、分支与循环画成一条流程。',
				'error.state': '读取流程失败',
				'error.command': '操作失败',
				'error.network': '连接失败',
				'error.code.no-turn': '提示词已被接受，但没有启动新一轮',
				'error.code.interrupted': 'Harness 重启中断了这一步',
				'error.code.dispatch-failed': '派发失败',
				'error.code.turn-error': '这一轮出错',
				'error.code.turn-aborted': '这一轮被中止',
				'error.code.turn-interrupted': '这一轮被中断',
				'error.code.empty-prompt': '这一步没有文字也没有附件',
				'error.code.unknown-command': '没有这个命令',
				'error.code.command-failed': '命令执行失败',
				'error.code.unknown': '未知错误',
				'hint.boundary': '严格轮次边界：执行中的请求与它的断联重试保持原模型。',
				'hint.shortcut': 'Ctrl/⌘ + Enter 保存文本',
				'busy': '处理中…',
			},
			en: {
				'tab.title': 'Flow queue',
				'tab.guide.description': 'Draw prompts, model switches, commands, branches and loops as one flow. It runs only at turn boundaries and shows exactly where it is.',
				'trigger.open': 'Open the flow queue',
				'overlay.title': 'Flow editor',
				'overlay.close': 'Close',
				'run.idle': 'Idle',
				'run.running': 'Running',
				'run.paused': 'Paused',
				'run.waiting': 'Waiting',
				'run.done': 'Done',
				'run.error': 'Error',
				'run.steps': '{n} steps',
				'action.start': 'Start',
				'action.pause': 'Pause',
				'action.resume': 'Continue',
				'action.stop': 'Stop',
				'action.reset': 'Reset',
				'action.clear': 'Clear',
				'action.layout': 'Auto layout',
				'action.library': 'Packages',
				'action.back': 'Back',
				'action.menu': 'More actions',
				'action.openEditor': 'Open editor',
				'action.from': 'Run from here',
				'action.add': 'Add',
				'action.delete': 'Delete',
				'action.detach': 'Remove',
				'action.upload': 'Add attachment',
				'kind.prompt': 'Prompt',
				'kind.model': 'Model switch',
				'kind.command': 'Command',
				'kind.branch': 'Branch',
				'kind.loop': 'Loop',
				'kind.checkpoint': 'Checkpoint',
				'status.idle': 'Pending',
				'status.running': 'Running',
				'status.completed': 'Done',
				'status.failed': 'Failed',
				'status.skipped': 'Skipped',
				'status.waiting': 'Waiting',
				'palette.title': 'Steps',
				'palette.hint': 'Click a step to add it. Drag from its right dot onto another step to connect.',
				'inspector.empty': 'Select a step to edit it; click empty canvas to deselect.',
				'inspector.title': 'Edit step',
				'inspector.text': 'Prompt text',
				'inspector.command': 'Command (for example /compact)',
				'inspector.model': 'Model',
				'inspector.modelInherit': 'Not set (keep current)',
				'inspector.note': 'Checkpoint note',
				'inspector.condition': 'Condition',
				'inspector.conditionValue': 'Match value',
				'inspector.negate': 'Negate (take “yes” when the condition fails)',
				'inspector.maxIterations': 'Max iterations',
				'inspector.attachments': 'Attachments',
				'inspector.noAttachments': 'No attachments yet. Images and files are sent with this prompt.',
				'inspector.usedModel': 'Served by',
				'inspector.turn': 'Turn',
				'inspector.error': 'Error',
				'inspector.result': 'Result',
				'cond.always': 'Always true',
				'cond.regex': 'Regex on the previous turn',
				'cond.contains': 'Contains text',
				'cond.marker': 'Contains [[FLOW:DONE]]',
				'cond.tool-failed': 'A tool failed last turn',
				'cond.last-failed': 'The previous step failed',
				'cond.value.always': '',
				'cond.value.regex': '(tests? passed|all green)',
				'cond.value.contains': 'keyword',
				'cond.value.marker': '[[FLOW:DONE]]',
				'cond.value.tool-failed': '',
				'cond.value.last-failed': '',
				'port.next': 'Next',
				'port.yes': 'Yes',
				'port.no': 'No',
				'port.again': 'Loop again',
				'port.exit': 'Exit loop',
				'canvas.empty': 'The canvas is empty: add a Prompt step from the left.',
				'canvas.connectHint': 'Connecting: click the target step to finish, Esc to cancel.',
				'canvas.edgeHint': 'Click a connection to remove it.',
				'library.title': 'Flow packages',
				'library.save': 'Save this flow as a package',
				'library.name': 'Name',
				'library.description': 'Description',
				'library.empty': 'No packages yet.',
				'library.load': 'Load',
				'library.delete': 'Delete',
				'library.saved': 'Saved',
				'empty.title': 'No steps yet',
				'empty.hint': 'Open the editor and draw prompts, model switches, commands, branches and loops into one flow.',
				'error.state': 'Could not read the flow',
				'error.command': 'The action failed',
				'error.network': 'Connection failed',
				'error.code.no-turn': 'The prompt was accepted but no turn started',
				'error.code.interrupted': 'A Harness restart interrupted this step',
				'error.code.dispatch-failed': 'Dispatch failed',
				'error.code.turn-error': 'This turn failed',
				'error.code.turn-aborted': 'This turn was aborted',
				'error.code.turn-interrupted': 'This turn was interrupted',
				'error.code.empty-prompt': 'This step has neither text nor an attachment',
				'error.code.unknown-command': 'No such command',
				'error.code.command-failed': 'The command failed',
				'error.code.unknown': 'Unknown error',
				'hint.boundary': 'Strict turn boundary: the running request and its retries keep their original model.',
				'hint.shortcut': 'Ctrl/⌘ + Enter to save',
				'busy': 'Working…',
			},
		};

		const C = {
			text: 'var(--dsw-alias-label-primary)',
			dim: 'var(--dsw-alias-label-secondary)',
			dimmer: 'var(--dsw-alias-label-tertiary)',
			border: 'var(--dsw-alias-border-l2)',
			field: 'var(--dsw-alias-bg-layer-1)',
			surface: 'var(--dsw-alias-bg-layer-3)',
			layer2: 'var(--dsw-alias-bg-layer-2)',
			accent: 'var(--dsw-alias-brand-primary)',
			onAccent: 'var(--dsw-alias-label-primary-foreground)',
			hover: 'var(--dsw-alias-interactive-bg-hover)',
			success: 'var(--dsw-alias-state-success-primary)',
			warn: 'var(--dsw-alias-state-warn-primary)',
			error: 'var(--dsw-alias-state-error-primary)',
		};

		const KIND_COLOR = {
			prompt: C.accent,
			model: C.success,
			command: C.warn,
			branch: C.warn,
			loop: C.error,
			checkpoint: C.dim,
		};

		const KIND_ORDER = ['prompt', 'model', 'command', 'branch', 'loop', 'checkpoint'];
		const PORT_LABEL = { next: 'port.next', yes: 'port.yes', no: 'port.no', again: 'port.again', exit: 'port.exit' };
		const CONDITION_TYPES = ['always', 'regex', 'contains', 'marker', 'tool-failed', 'last-failed'];

		const srOnly = {
			position: 'absolute',
			width: '1px',
			height: '1px',
			overflow: 'hidden',
			clipPath: 'inset(50%)',
			whiteSpace: 'nowrap',
		};

		function fill(template, params) {
			if (params === undefined) return template;
			return String(template).replace(/\{(\w+)\}/g, (whole, key) => (params[key] === undefined ? whole : String(params[key])));
		}

		function ttext(key, params) {
			if (localeSvc !== null) {
				try {
					return localeSvc.bind(NS)(key, params);
				} catch {
					/* fall through to the bundled dictionary */
				}
			}
			return fill(DICT.zh[key] !== undefined ? DICT.zh[key] : key, params);
		}

		function useT() {
			const [, bump] = useState(0);
			useEffect(() => {
				if (localeSvc === null || typeof localeSvc.subscribe !== 'function') return undefined;
				return localeSvc.subscribe(() => bump((value) => value + 1));
			}, []);
			return useCallback((key, params) => ttext(key, params), []);
		}


		//#region transport

		function requestJson(url, init) {
			return fetch(url, { cache: 'no-store', ...init }).then((response) => response.json());
		}

		function unwrap(data) {
			if (data !== null && data !== undefined && data.ok === true) return data.value;
			const error = new Error((data && data.error && (data.error.message || data.error.code)) || 'request failed');
			error.code = data && data.error ? data.error.code : 'unknown';
			throw error;
		}

		function postCommand(payload) {
			return requestJson('/flow-queue/api/command', {
				method: 'POST',
				headers: { 'content-type': 'application/json' },
				body: JSON.stringify(payload),
			}).then(unwrap);
		}

		function readState(sessionId) {
			return requestJson(`/flow-queue/api/state?sessionId=${encodeURIComponent(sessionId)}`).then(unwrap);
		}

		function readModels() {
			return requestJson('/flow-queue/api/models').then(unwrap);
		}

		function readPackages() {
			return requestJson('/flow-queue/api/packages').then(unwrap);
		}

		function packagesOp(payload) {
			return requestJson('/flow-queue/api/packages', {
				method: 'POST',
				headers: { 'content-type': 'application/json' },
				body: JSON.stringify(payload),
			}).then(unwrap);
		}

		function errorText(t, error) {
			if (error === undefined || error === null) return '';
			const code = typeof error === 'string' ? 'unknown' : error.code === undefined ? 'unknown' : error.code;
			const label = t(`error.code.${code}`) || t('error.code.unknown');
			const message = typeof error === 'string' ? error : error.message;
			return message ? `${label}: ${message}` : label;
		}

		function fileToBase64(file) {
			return new Promise((resolve, reject) => {
				const reader = new FileReader();
				reader.onload = () => {
					const result = String(reader.result || '');
					const comma = result.indexOf(',');
					resolve(comma >= 0 ? result.slice(comma + 1) : result);
				};
				reader.onerror = () => reject(new Error('could not read the file'));
				reader.readAsDataURL(file);
			});
		}

		//#endregion

		//#region atoms

		function Glyph(props) {
			const size = props && typeof props.size === 'number' ? props.size : 16;
			return h(
				'svg',
				{ viewBox: '0 0 24 24', width: size, height: size, 'aria-hidden': true, style: { display: 'block', flex: 'none' } },
				h('path', { d: 'M4 5.5h5.5M4 18.5h5.5', stroke: 'currentColor', strokeWidth: 1.6, strokeLinecap: 'round', fill: 'none' }),
				h('rect', { x: 11, y: 3, width: 9, height: 5, rx: 1.6, stroke: 'currentColor', strokeWidth: 1.6, fill: 'none' }),
				h('rect', { x: 11, y: 16, width: 9, height: 5, rx: 1.6, stroke: 'currentColor', strokeWidth: 1.6, fill: 'none' }),
				h('path', { d: 'M9.5 5.5h1.5M9.5 18.5h1.5', stroke: 'currentColor', strokeWidth: 1.6, strokeLinecap: 'round', fill: 'none' }),
			);
		}

		function Button(props) {
			const primary = props.primary === true;
			const danger = props.danger === true;
			const disabled = props.disabled === true;
			const style = {
				appearance: 'none',
				border: primary ? '1px solid transparent' : `1px solid ${danger ? C.error : C.border}`,
				background: primary ? C.accent : 'transparent',
				color: primary ? C.onAccent : danger ? C.error : C.text,
				borderRadius: '6px',
				padding: props.compact === true ? '0 6px' : '3px 10px',
				fontSize: props.compact === true ? '11px' : '12px',
				lineHeight: props.compact === true ? '18px' : '22px',
				cursor: disabled ? 'default' : 'pointer',
				opacity: disabled ? 0.45 : 1,
				fontFamily: 'inherit',
				flex: 'none',
				whiteSpace: 'nowrap',
			};
			return h(
				'button',
				{
					type: 'button',
					style,
					onClick: disabled ? undefined : props.onClick,
					disabled,
					title: props.title,
					'aria-label': props.title,
				},
				props.children,
			);
		}

		function Chip(props) {
			const color = props.color || C.dim;
			return h(
				'span',
				{
					style: {
						display: 'inline-flex',
						alignItems: 'center',
						gap: '4px',
						padding: '1px 7px',
						borderRadius: '999px',
						fontSize: '11px',
						lineHeight: '16px',
						color,
						border: `1px solid ${color}`,
						whiteSpace: 'nowrap',
					},
				},
				props.children,
			);
		}

		function StatusDot(props) {
			const color = props.color || C.dimmer;
			return h('span', {
				'aria-hidden': true,
				style: {
					width: '7px',
					height: '7px',
					borderRadius: '50%',
					background: color,
					flex: 'none',
					boxShadow: props.pulse === true ? `0 0 0 3px ${C.hover}` : 'none',
				},
			});
		}

		function rowChipStyle(kind) {
			return { color: KIND_COLOR[kind] || C.dim, borderColor: KIND_COLOR[kind] || C.dim };
		}

		function Field(props) {
			return h(
				'label',
				{ style: { display: 'flex', flexDirection: 'column', gap: '4px', fontSize: '11px', color: C.dim } },
				h('span', null, props.label),
				props.children,
			);
		}

		const inputStyle = {
			width: '100%',
			boxSizing: 'border-box',
			border: `1px solid ${C.border}`,
			borderRadius: '6px',
			background: C.field,
			color: C.text,
			fontSize: '12px',
			lineHeight: '18px',
			padding: '5px 7px',
			fontFamily: 'inherit',
			outline: 'none',
		};

		//#endregion

		//#region summary helpers

		function statusColor(status) {
			if (status === 'running') return C.accent;
			if (status === 'completed') return C.success;
			if (status === 'failed') return C.error;
			if (status === 'waiting') return C.warn;
			return C.dimmer;
		}

		function runColor(state) {
			if (state === 'running') return C.accent;
			if (state === 'paused') return C.dim;
			if (state === 'waiting') return C.warn;
			if (state === 'done') return C.success;
			if (state === 'error') return C.error;
			return C.dimmer;
		}

		function sortedNodes(nodes) {
			return nodes.slice().sort((left, right) => (left.y === right.y ? left.x - right.x : left.y - right.y));
		}

		function nodeSummary(node, t) {
			if (node.kind === 'prompt') {
				const text = typeof node.text === 'string' ? node.text.trim() : '';
				const head = text.split('\n')[0].slice(0, 80);
				const count = Array.isArray(node.attachments) ? node.attachments.length : 0;
				return `${head}${text.length > 80 ? '…' : ''}${count > 0 ? `  📎${count}` : ''}`;
			}
			if (node.kind === 'model') return `${node.provider || '?'} / ${node.model || '?'}`;
			if (node.kind === 'command') return node.command || t('kind.command');
			if (node.kind === 'checkpoint') return node.note || t('kind.checkpoint');
			if (node.kind === 'branch' || node.kind === 'loop') {
				const type = node.condition === undefined ? 'always' : node.condition.type;
				const base = `${t(`cond.${type}`)}${node.condition && node.condition.value ? ` · ${node.condition.value}` : ''}`;
				return node.kind === 'loop' ? `${base} · ≤${node.maxIterations || 1}` : base;
			}
			return '';
		}

		function conditionValuePlaceholder(type) {
			return ttext(`cond.value.${type}`);
		}

		//#endregion

		//#region flow canvas

		/** Vertical center of an output dot, in canvas coordinates. */
		function outputY(node, index) {
			return node.y + 23.5 + index * PORT_GAP;
		}

		/** Vertical center of the input dot, in canvas coordinates. */
		function inputY(node) {
			return node.y + 27.5;
		}

		function portsOf(node) {
			if (node.kind === 'branch') return ['yes', 'no'];
			if (node.kind === 'loop') return ['again', 'exit'];
			return ['next'];
		}

		function InputPort(props) {
			return h('span', {
				'data-fq-input': '1',
				title: props.title,
				style: {
					position: 'absolute',
					left: '-6px',
					top: `${props.top}px`,
					width: '11px',
					height: '11px',
					borderRadius: '50%',
					border: `2px solid ${props.active === true ? C.accent : C.border}`,
					background: C.surface,
					boxSizing: 'border-box',
				},
			});
		}

		function OutputPort(props) {
			return h(
				'div',
				{
					'data-fq-port': props.port,
					onPointerDown: (event) => {
						event.stopPropagation();
						event.preventDefault();
						props.onStart(props.port, event);
					},
					title: props.title,
					style: {
						position: 'absolute',
						right: '-7px',
						top: `${props.top}px`,
						width: '13px',
						height: '13px',
						borderRadius: '50%',
						border: `2px solid ${props.color}`,
						background: C.surface,
						boxSizing: 'border-box',
						cursor: 'crosshair',
						touchAction: 'none',
					},
				},
			);
		}

		function FlowNodeCard(props) {
			const node = props.node;
			const t = props.t;
			const color = KIND_COLOR[node.kind] || C.dim;
			const status = node.status || 'idle';
			const selected = props.selected === true;
			const running = status === 'running';
			const cardStyle = {
				position: 'absolute',
				left: `${node.x}px`,
				top: `${node.y}px`,
				width: `${NODE_W}px`,
				boxSizing: 'border-box',
				borderRadius: '10px',
				border: `1.5px solid ${selected ? C.accent : running ? C.accent : C.border}`,
				background: C.surface,
				boxShadow: selected ? `0 0 0 3px ${C.hover}` : 'none',
				display: 'flex',
				flexDirection: 'column',
				cursor: 'default',
				userSelect: 'none',
			};
			const headStyle = {
				display: 'flex',
				alignItems: 'center',
				gap: '6px',
				padding: '7px 9px',
				borderBottom: `1px solid ${C.border}`,
				borderRadius: '9px 9px 0 0',
				background: C.layer2,
				cursor: props.busy === true ? 'default' : 'grab',
				touchAction: 'none',
			};
			const bodyStyle = {
				padding: '8px 9px',
				fontSize: '12px',
				lineHeight: '17px',
				color: node.kind === 'prompt' ? C.text : C.dim,
				whiteSpace: 'pre-wrap',
				wordBreak: 'break-word',
				maxHeight: '96px',
				overflow: 'hidden',
			};
			const attachments = Array.isArray(node.attachments) ? node.attachments.length : 0;
			const body = nodeSummary(node, t);
			return h(
				'div',
				{ 'data-fq-node': node.id, style: cardStyle },
				h(
					'div',
					{
						style: headStyle,
						onPointerDown: (event) => {
							if (event.button !== 0) return;
							event.stopPropagation();
							props.onSelect(node.id);
							props.onDragStart(node.id, event);
						},
					},
					h(StatusDot, { color: statusColor(status), pulse: running }),
					h('span', { style: { fontSize: '10px', border: `1px solid ${color}`, borderRadius: '999px', padding: '0 6px', lineHeight: '15px', color } }, t(`kind.${node.kind}`)),
					h('span', { style: { flex: 1, minWidth: 0, fontSize: '11px', color: C.dim, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' } }, t(`status.${status}`)),
					node.usedModel ? h('span', { style: { fontSize: '10px', color: C.dimmer, maxWidth: '88px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }, title: node.usedModel }, node.usedModel) : null,
				),
				h('div', { style: bodyStyle }, body || h('span', { style: { color: C.dimmer } }, '—')),
				node.error
					? h('div', { style: { padding: '0 9px 7px', fontSize: '11px', lineHeight: '15px', color: C.error } }, errorText(t, node.error))
					: null,
				node.result && node.kind !== 'prompt'
					? h('div', { style: { padding: '0 9px 7px', fontSize: '11px', lineHeight: '15px', color: C.dimmer } }, String(node.result).slice(0, 120))
					: null,
				h(InputPort, { top: 22, active: props.connecting === true }),
				portsOf(node).map((port, index) =>
					h(OutputPort, {
						key: port,
						port,
						top: 17 + index * PORT_GAP,
						color: status === 'failed' ? C.error : C.border,
						title: `${t(`port.${port}`)} · ${t('canvas.edgeHint')}`,
						onStart: props.onPortStart,
					})),
				portsOf(node).length > 1
					? portsOf(node).map((port, index) =>
						h(
							'span',
							{
								key: `label-${port}`,
								style: {
									position: 'absolute',
									right: '14px',
									top: `${11 + index * PORT_GAP}px`,
									fontSize: '10px',
									color: C.dimmer,
									pointerEvents: 'none',
								},
							},
							t(`port.${port}`),
						))
					: null,
				attachments > 0 && node.kind === 'prompt'
					? h('span', { style: { position: 'absolute', left: '8px', bottom: '5px', fontSize: '10px', color: C.dimmer } }, `📎 ${attachments}`)
					: null,
			);
		}

		function edgePath(from, to, port) {
			const index = Math.max(0, portsOf(from).indexOf(port));
			const y1 = outputY(from, index);
			const x1 = from.x + NODE_W;
			const y2 = inputY(to);
			const x2 = to.x;
			const bend = Math.max(40, Math.abs(x2 - x1) * 0.45);
			return `M ${x1} ${y1} C ${x1 + bend} ${y1}, ${x2 - bend} ${y2}, ${x2} ${y2}`;
		}

		function FlowCanvas(props) {
			const t = props.t;
			const containerRef = useRef(null);
			const [view, setView] = useState({ x: 0, y: 0, scale: 1 });
			const [connecting, setConnecting] = useState(null);
			const [pointer, setPointer] = useState({ x: 0, y: 0 });
			const dragRef = useRef(null);
			const connectingRef = useRef(null);
			const panRef = useRef(null);
			const viewRef = useRef(view);
			const handlersRef = useRef(props);
			viewRef.current = view;
			handlersRef.current = props;

			const nodesById = useMemo(() => {
				const map = new Map();
				for (const node of props.nodes) map.set(node.id, node);
				return map;
			}, [props.nodes]);
			const nodesByIdRef = useRef(nodesById);
			nodesByIdRef.current = nodesById;

			const toCanvas = useCallback((clientX, clientY) => {
				const element = containerRef.current;
				const current = viewRef.current;
				if (element === null) return { x: 0, y: 0 };
				const rect = element.getBoundingClientRect();
				return {
					x: (clientX - rect.left - current.x) / current.scale,
					y: (clientY - rect.top - current.y) / current.scale,
				};
			}, []);

			// One stable pair of document listeners: dragging and connecting read the
			// live refs above, so a re-render never drops a pointer event.
			useEffect(() => {
				const onMove = (event) => {
					const point = toCanvas(event.clientX, event.clientY);
					if (panRef.current !== null) {
						const pan = panRef.current;
						setView({ x: pan.vx + (event.clientX - pan.x), y: pan.vy + (event.clientY - pan.y), scale: viewRef.current.scale });
						return;
					}
					if (dragRef.current !== null) {
						const base = dragRef.current;
						handlersRef.current.onMove(base.id, Math.max(0, point.x - base.dx), Math.max(0, point.y - base.dy));
						return;
					}
					if (connectingRef.current !== null) setPointer(point);
				};
				const onUp = (event) => {
					if (panRef.current !== null) {
						panRef.current = null;
						return;
					}
					if (connectingRef.current !== null) {
						const connection = connectingRef.current;
						connectingRef.current = null;
						setConnecting(null);
						const target = document.elementFromPoint(event.clientX, event.clientY);
						const holder = target === null ? null : target.closest('[data-fq-node]');
						if (holder !== null) {
							const to = holder.getAttribute('data-fq-node');
							if (to !== null && to !== connection.from) handlersRef.current.onConnect(connection.from, connection.port, to);
						}
						return;
					}
					if (dragRef.current !== null) {
						const base = dragRef.current;
						dragRef.current = null;
						handlersRef.current.onCommitMove(base.id);
					}
				};
				const onKey = (event) => {
					if (event.key !== 'Escape') return;
					if (connectingRef.current !== null) {
						connectingRef.current = null;
						setConnecting(null);
					}
					if (dragRef.current !== null) {
						const base = dragRef.current;
						dragRef.current = null;
						handlersRef.current.onCancelMove(base.id);
					}
				};
				document.addEventListener('pointermove', onMove);
				document.addEventListener('pointerup', onUp);
				document.addEventListener('keydown', onKey);
				return () => {
					document.removeEventListener('pointermove', onMove);
					document.removeEventListener('pointerup', onUp);
					document.removeEventListener('keydown', onKey);
				};
			}, [toCanvas]);

			const startDrag = useCallback((id, event) => {
				if (handlersRef.current.busy === true) return;
				const node = nodesByIdRef.current.get(id);
				if (node === undefined) return;
				const point = toCanvas(event.clientX, event.clientY);
				dragRef.current = { id, dx: point.x - node.x, dy: point.y - node.y };
			}, [toCanvas]);

			const startPort = useCallback((id, port, event) => {
				if (handlersRef.current.busy === true) return;
				connectingRef.current = { from: id, port };
				setConnecting({ from: id, port });
				setPointer(toCanvas(event.clientX, event.clientY));
			}, [toCanvas]);

			const onCanvasPointerDown = (event) => {
				if (event.button !== 0) return;
				const target = event.target;
				if (target !== null && target.closest !== undefined) {
					if (target.closest('[data-fq-node]') !== null) return;
					if (target.closest('[data-fq-port]') !== null) return;
				}
				handlersRef.current.onSelect('');
				// Empty canvas drags pan the view: a narrow column needs it.
				panRef.current = { x: event.clientX, y: event.clientY, vx: viewRef.current.x, vy: viewRef.current.y };
			};

			const edges = props.edges.map((edge) => {
				const from = nodesById.get(edge.from);
				const to = nodesById.get(edge.to);
				if (from === undefined || to === undefined) return null;
				const color = from.status === 'failed' ? C.error : from.status === 'completed' ? C.success : C.border;
				return h('path', {
					key: edge.id,
					d: edgePath(from, to, edge.port),
					fill: 'none',
					stroke: color,
					strokeWidth: 1.8,
					strokeDasharray: from.status === 'running' ? '5 4' : undefined,
					style: { pointerEvents: 'stroke', cursor: 'pointer' },
					onClick: (event) => {
						event.stopPropagation();
						handlersRef.current.onRemoveEdge(edge.id);
					},
				});
			});

			const connectingFrom = connecting === null ? undefined : nodesById.get(connecting.from);
			const ghost = connectingFrom === undefined
				? null
				: h('path', {
					d: `M ${connectingFrom.x + NODE_W} ${outputY(connectingFrom, Math.max(0, portsOf(connectingFrom).indexOf(connecting.port)))} L ${pointer.x} ${pointer.y}`,
					stroke: C.accent,
					strokeWidth: 1.6,
					strokeDasharray: '5 4',
					fill: 'none',
				});

			const onWheel = (event) => {
				if (event.ctrlKey !== true && event.metaKey !== true) return;
				event.preventDefault();
				const next = Math.min(1.6, Math.max(0.4, view.scale * (event.deltaY > 0 ? 0.92 : 1.08)));
				setView({ ...view, scale: next });
			};

			return h(
				'div',
				{
					ref: containerRef,
					onWheel,
					onPointerDown: onCanvasPointerDown,
					style: {
						position: 'relative',
						flex: 1,
						minHeight: 0,
						overflow: 'hidden',
						background: C.field,
						borderRadius: '10px',
						border: `1px solid ${C.border}`,
						backgroundImage: `radial-gradient(${C.border} 1px, transparent 1px)`,
						backgroundSize: `${22 * view.scale}px ${22 * view.scale}px`,
						backgroundPosition: `${view.x}px ${view.y}px`,
					},
				},
				props.nodes.length === 0
					? h(
						'div',
						{ style: { position: 'absolute', inset: 0, display: 'flex', alignItems: 'center', justifyContent: 'center', color: C.dim, fontSize: '12px', pointerEvents: 'none' } },
						t('canvas.empty'),
					)
					: null,
				h(
					'div',
					{ style: { position: 'absolute', inset: 0, transformOrigin: '0 0', transform: `translate(${view.x}px, ${view.y}px) scale(${view.scale})` } },
					h(
						'svg',
						{ width: 4000, height: 3000, style: { position: 'absolute', left: 0, top: 0, overflow: 'visible', pointerEvents: 'none' } },
						edges,
						ghost,
					),
					props.nodes.map((node) =>
						h(FlowNodeCard, {
							key: node.id,
							node,
							t,
							busy: props.busy,
							selected: props.selected === node.id,
							connecting: connecting !== null,
							onSelect: props.onSelect,
							onDragStart: startDrag,
							onPortStart: startPort,
						})),
				),
				h(
					'div',
					{ style: { position: 'absolute', right: '10px', bottom: '10px', display: 'flex', gap: '6px', alignItems: 'center', background: C.surface, border: `1px solid ${C.border}`, borderRadius: '8px', padding: '3px 6px' } },
					h(Button, { compact: true, onClick: () => setView({ ...view, scale: Math.max(0.4, view.scale - 0.1) }), title: '−' }, '−'),
					h('span', { style: { fontSize: '11px', color: C.dim, minWidth: '34px', textAlign: 'center' } }, `${Math.round(view.scale * 100)}%`),
					h(Button, { compact: true, onClick: () => setView({ ...view, scale: Math.min(1.6, view.scale + 0.1) }), title: '+' }, '+'),
					h(Button, { compact: true, onClick: () => setView({ x: 0, y: 0, scale: 1 }), title: t('action.reset') }, '⤢'),
				),
				connecting !== null
					? h('div', { style: { position: 'absolute', left: '12px', top: '12px', padding: '4px 8px', borderRadius: '6px', background: C.surface, border: `1px solid ${C.accent}`, color: C.accent, fontSize: '11px' } }, t('canvas.connectHint'))
					: null,
			);
		}

		//#endregion

		//#region inspector

		function ModelSelect(props) {
			const groups = props.catalog !== null && Array.isArray(props.catalog.groups) ? props.catalog.groups : [];
			const value = props.allowEmpty === true && (props.value === undefined || props.value === '') ? '' : props.value;
			const options = [];
			for (const group of groups) {
				const models = Array.isArray(group.models) ? group.models : [];
				for (const model of models) {
					if (typeof group.id !== 'string' || typeof model.id !== 'string') continue;
					options.push({ value: `${group.id}\u0000${model.id}`, label: `${group.name || group.id} · ${model.name || model.id}` });
				}
			}
			return h(
				'select',
				{
					value: value,
					style: inputStyle,
					onChange: (event) => {
						const raw = event.target.value;
						if (raw === '') return props.onChange(undefined);
						const [provider, model] = raw.split('\u0000');
						const chosen = options.find((option) => option.value === raw);
						props.onChange({ provider, model, label: chosen === undefined ? model : chosen.label });
					},
				},
				props.allowEmpty === true ? h('option', { value: '' }, ttext('inspector.modelInherit')) : null,
				options.map((option) => h('option', { key: option.value, value: option.value }, option.label)),
			);
		}

		function AttachmentList(props) {
			const t = props.t;
			const attachments = Array.isArray(props.node.attachments) ? props.node.attachments : [];
			return h(
				'div',
				{ style: { display: 'flex', flexDirection: 'column', gap: '5px' } },
				attachments.length === 0
					? h('div', { style: { fontSize: '11px', color: C.dimmer, lineHeight: '16px' } }, t('inspector.noAttachments'))
					: attachments.map((item) =>
						h(
							'div',
							{ key: item.attachment.attachmentId, style: { display: 'flex', alignItems: 'center', gap: '6px', border: `1px solid ${C.border}`, borderRadius: '6px', padding: '3px 6px' } },
							h('span', { style: { fontSize: '11px', color: C.dim, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1 } },
								`${item.type === 'image' ? '🖼' : '📄'} ${item.attachment.name || item.attachment.attachmentId.slice(0, 10)}`),
							h(Button, { compact: true, onClick: () => props.onDetach(item.attachment.attachmentId), title: t('action.detach') }, '✕'),
						)),
			);
		}

		function Inspector(props) {
			const t = props.t;
			const node = props.node;
			const [text, setText] = useState('');
			const [command, setCommand] = useState('');
			const [note, setNote] = useState('');
			const [conditionValue, setConditionValue] = useState('');
			const [maxIterations, setMaxIterations] = useState(5);

			useEffect(() => {
				if (node === undefined) return;
				setText(typeof node.text === 'string' ? node.text : '');
				setCommand(typeof node.command === 'string' ? node.command : '');
				setNote(typeof node.note === 'string' ? node.note : '');
				setConditionValue(node.condition !== undefined && typeof node.condition.value === 'string' ? node.condition.value : '');
				setMaxIterations(Number.isFinite(node.maxIterations) ? node.maxIterations : 5);
			}, [node === undefined ? '' : node.id]);

			if (node === undefined) {
				return h('div', { style: { padding: '12px', fontSize: '12px', color: C.dim, lineHeight: '18px' } }, t('inspector.empty'));
			}

			const patch = (change) => props.onPatch(node.id, change);
			const upload = (kind) => (event) => {
				const file = event.target.files !== undefined && event.target.files !== null ? event.target.files[0] : undefined;
				event.target.value = '';
				if (file === undefined) return;
				props.onUpload(node.id, kind, file);
			};

			const rows = [];
			rows.push(h('div', { key: 'kind', style: { display: 'flex', alignItems: 'center', gap: '6px' } },
				h('span', { style: { ...rowChipStyle(node.kind), border: `1px solid ${KIND_COLOR[node.kind] || C.dim}`, borderRadius: '999px', padding: '0 7px', fontSize: '10px', lineHeight: '16px', color: KIND_COLOR[node.kind] || C.dim } }, t(`kind.${node.kind}`)),
				h(Chip, { color: statusColor(node.status) }, t(`status.${node.status || 'idle'}`)),
				node.usedModel ? h('span', { style: { fontSize: '10px', color: C.dimmer } }, node.usedModel) : null,
				node.turn !== undefined ? h('span', { style: { fontSize: '10px', color: C.dimmer } }, `#${node.turn}`) : null));

			if (node.kind === 'prompt') {
				rows.push(h(Field, { key: 'text', label: t('inspector.text') },
					h('textarea', {
						value: text,
						rows: 6,
						style: { ...inputStyle, resize: 'vertical', minHeight: '96px' },
						onChange: (event) => setText(event.target.value),
						onBlur: () => patch({ text }),
						onKeyDown: (event) => {
							if (event.key === 'Enter' && (event.ctrlKey || event.metaKey)) {
								event.preventDefault();
								patch({ text });
							}
						},
					})));
				rows.push(h(Field, { key: 'model', label: t('inspector.model') },
					h(ModelSelect, {
						catalog: props.catalog,
						allowEmpty: true,
						value: node.provider !== undefined && node.model !== undefined ? `${node.provider}\u0000${node.model}` : '',
						onChange: (selection) => {
							if (selection === undefined) patch({ provider: null, model: null, modelLabel: null });
							else patch({ provider: selection.provider, model: selection.model, modelLabel: selection.label });
						},
					})));
				rows.push(h(Field, { key: 'attachments', label: t('inspector.attachments') },
					h(AttachmentList, {
						t,
						node,
						onDetach: (attachmentId) => props.onDetach(node.id, attachmentId),
					})));
				rows.push(h('div', { key: 'upload', style: { display: 'flex', gap: '6px' } },
					h('label', { style: { ...ButtonStyleLike(), position: 'relative', overflow: 'hidden', display: 'inline-flex', alignItems: 'center', cursor: 'pointer' } },
						t('action.upload') + ' (🖼)',
						h('input', { type: 'file', accept: 'image/*', onChange: upload('image'), style: { position: 'absolute', inset: 0, opacity: 0, cursor: 'pointer' } })),
					h('label', { style: { ...ButtonStyleLike(), position: 'relative', overflow: 'hidden', display: 'inline-flex', alignItems: 'center', cursor: 'pointer' } },
						t('action.upload') + ' (📄)',
						h('input', { type: 'file', onChange: upload('file'), style: { position: 'absolute', inset: 0, opacity: 0, cursor: 'pointer' } }))));
			}

			if (node.kind === 'model') {
				rows.push(h(Field, { key: 'model', label: t('inspector.model') },
					h(ModelSelect, {
						catalog: props.catalog,
						value: node.provider !== undefined && node.model !== undefined ? `${node.provider}\u0000${node.model}` : '',
						onChange: (selection) => {
							if (selection === undefined) return;
							patch({ provider: selection.provider, model: selection.model, modelLabel: selection.label });
						},
					})));
			}

			if (node.kind === 'command') {
				rows.push(h(Field, { key: 'command', label: t('inspector.command') },
					h('input', {
						value: command,
						style: inputStyle,
						onChange: (event) => setCommand(event.target.value),
						onBlur: () => patch({ command }),
					})));
			}

			if (node.kind === 'checkpoint') {
				rows.push(h(Field, { key: 'note', label: t('inspector.note') },
					h('input', {
						value: note,
						style: inputStyle,
						onChange: (event) => setNote(event.target.value),
						onBlur: () => patch({ note }),
					})));
			}

			if (node.kind === 'branch' || node.kind === 'loop') {
				const condition = node.condition !== undefined ? node.condition : { type: 'always', value: '', negate: false };
				rows.push(h(Field, { key: 'condition', label: t('inspector.condition') },
					h('select', {
						value: condition.type,
						style: inputStyle,
						onChange: (event) => patch({ condition: { type: event.target.value, value: conditionValue, negate: condition.negate === true } }),
					}, CONDITION_TYPES.map((type) => h('option', { key: type, value: type }, t(`cond.${type}`))))));
				if (condition.type !== 'always' && condition.type !== 'tool-failed' && condition.type !== 'last-failed') {
					rows.push(h(Field, { key: 'conditionValue', label: t('inspector.conditionValue') },
						h('input', {
							value: conditionValue,
							placeholder: conditionValuePlaceholder(condition.type),
							style: inputStyle,
							onChange: (event) => setConditionValue(event.target.value),
							onBlur: () => patch({ condition: { type: condition.type, value: conditionValue, negate: condition.negate === true } }),
						})));
				}
				rows.push(h('label', { key: 'negate', style: { display: 'flex', gap: '6px', alignItems: 'center', fontSize: '11px', color: C.dim } },
					h('input', {
						type: 'checkbox',
						checked: condition.negate === true,
						onChange: (event) => patch({ condition: { type: condition.type, value: conditionValue, negate: event.target.checked } }),
					}),
					t('inspector.negate')));
				if (node.kind === 'loop') {
					rows.push(h(Field, { key: 'max', label: t('inspector.maxIterations') },
						h('input', {
							type: 'number',
							min: 1,
							max: 100,
							value: maxIterations,
							style: inputStyle,
							onChange: (event) => setMaxIterations(Number(event.target.value) || 1),
							onBlur: () => patch({ maxIterations }),
						})));
				}
			}

			if (node.error !== undefined) rows.push(h('div', { key: 'error', style: { fontSize: '11px', color: C.error, lineHeight: '16px' } }, errorText(t, node.error)));
			if (typeof node.result === 'string' && node.result !== '' && node.kind !== 'prompt') {
				rows.push(h(Field, { key: 'result', label: t('inspector.result') },
					h('div', { style: { fontSize: '11px', color: C.dim, lineHeight: '16px', whiteSpace: 'pre-wrap', maxHeight: '90px', overflow: 'auto' } }, node.result)));
			}

			// Run-from / delete live in the sheet header (the panel owns them), not here.
			return h('div', { style: { display: 'flex', flexDirection: 'column', gap: '9px' } }, ...rows);
		}

		function ButtonStyleLike() {
			return {
				appearance: 'none',
				border: `1px solid ${C.border}`,
				background: 'transparent',
				color: C.text,
				borderRadius: '6px',
				padding: '3px 10px',
				fontSize: '12px',
				lineHeight: '22px',
				fontFamily: 'inherit',
			};
		}

		//#endregion

		//#region library

		function Library(props) {
			const t = props.t;
			const [name, setName] = useState('');
			const [description, setDescription] = useState('');
			return h(
				'div',
				{ style: { display: 'flex', flexDirection: 'column', gap: '8px' } },
				h('input', { value: name, placeholder: t('library.name'), style: inputStyle, onChange: (event) => setName(event.target.value) }),
				h('input', { value: description, placeholder: t('library.description'), style: inputStyle, onChange: (event) => setDescription(event.target.value) }),
				h(Button, {
					disabled: props.busy,
					onClick: () => {
						props.onSave(name, description);
						setName('');
						setDescription('');
					},
				}, t('library.save')),
				props.packages.length === 0
					? h('div', { style: { fontSize: '11px', color: C.dimmer } }, t('library.empty'))
					: props.packages.map((item) =>
						h(
							'div',
							{ key: item.id, style: { border: `1px solid ${C.border}`, borderRadius: '7px', padding: '6px 7px', display: 'flex', flexDirection: 'column', gap: '4px' } },
							h('div', { style: { fontSize: '12px', color: C.text, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' } }, item.name),
							item.description !== '' ? h('div', { style: { fontSize: '10px', color: C.dimmer, lineHeight: '14px' } }, item.description) : null,
							h('div', { style: { fontSize: '10px', color: C.dimmer } }, `${item.nodes} 步 · ${item.edges} 连线`),
							h('div', { style: { display: 'flex', gap: '6px' } },
								h(Button, { compact: true, disabled: props.busy, onClick: () => props.onLoad(item.id) }, t('library.load')),
								h(Button, { compact: true, danger: true, disabled: props.busy, onClick: () => props.onDelete(item.id) }, t('library.delete'))),
						)),
			);
		}

		//#endregion

		//#region sidebar editor

		function FlowQueuePanel(props) {
			const sessionId = props !== null && props !== undefined && typeof props.sessionId === 'string' ? props.sessionId : '';
			const t = useT();
			const mounted = useRef(true);
			const [snapshot, setSnapshot] = useState(null);
			const [models, setModels] = useState(null);
			const [packages, setPackages] = useState([]);
			const [selected, setSelected] = useState('');
			const [sheet, setSheet] = useState('');
			const [menuOpen, setMenuOpen] = useState(false);
			const [busy, setBusy] = useState(false);
			const [error, setError] = useState(null);
			const [localPos, setLocalPos] = useState({});

			useEffect(() => {
				mounted.current = true;
				return () => {
					mounted.current = false;
				};
			}, []);

			const refresh = useCallback(() => {
				if (sessionId === '') return Promise.resolve();
				return readState(sessionId)
					.then((value) => {
						if (!mounted.current) return;
						setSnapshot(value);
						setError(null);
					})
					.catch((err) => {
						if (mounted.current) setError(err.message || String(err));
					});
			}, [sessionId]);

			useEffect(() => {
				if (sessionId === '') return undefined;
				refresh();
				readModels().then((value) => { if (mounted.current) setModels(value); }).catch(() => {});
				readPackages().then((value) => { if (mounted.current) setPackages(Array.isArray(value) ? value : []); }).catch(() => {});
				const handle = setInterval(refresh, POLL_MS);
				return () => clearInterval(handle);
			}, [refresh, sessionId]);

			const run = useCallback((payload) => {
				if (sessionId === '') return Promise.resolve();
				setBusy(true);
				return postCommand({ sessionId, ...payload })
					.then((value) => {
						if (!mounted.current) return;
						if (value !== null && value !== undefined && value.state !== undefined) setSnapshot(value.state);
						else if (value !== null && value !== undefined && value.nodes !== undefined) setSnapshot(value);
						setError(null);
						refresh();
					})
					.catch((err) => {
						if (mounted.current) setError(err.message || String(err));
					})
					.finally(() => {
						if (mounted.current) setBusy(false);
					});
			}, [sessionId, refresh]);

			const reloadPackages = useCallback(() => {
				readPackages()
					.then((value) => { if (mounted.current) setPackages(Array.isArray(value) ? value : []); })
					.catch(() => {});
			}, []);

			useEffect(() => {
				if (!menuOpen) return undefined;
				const close = () => setMenuOpen(false);
				const handle = setTimeout(() => document.addEventListener('pointerdown', close), 0);
				return () => {
					clearTimeout(handle);
					document.removeEventListener('pointerdown', close);
				};
			}, [menuOpen]);

			if (sessionId === '') {
				return h('div', { style: { padding: '16px', fontSize: '12px', color: C.dim } }, 'No Session selected.');
			}

			const nodes = snapshot !== null && Array.isArray(snapshot.nodes) ? snapshot.nodes : [];
			const edges = snapshot !== null && Array.isArray(snapshot.edges) ? snapshot.edges : [];
			const runState = snapshot !== null && snapshot.run !== undefined ? snapshot.run.state : 'idle';
			const steps = snapshot !== null && snapshot.run !== undefined ? snapshot.run.steps : 0;
			const renderedNodes = nodes.map((node) => (localPos[node.id] === undefined ? node : { ...node, ...localPos[node.id] }));
			const selectedNode = renderedNodes.find((node) => node.id === selected);

			const addNode = (kind) => {
				const index = nodes.length;
				void run({ op: 'node.add', kind, x: 36 + (index % 3) * 14, y: 40 + index * 40 });
			};

			const moveNode = (id, x, y) => setLocalPos((current) => ({ ...current, [id]: { x, y } }));
			const commitMove = (id) => {
				const position = localPos[id];
				if (position === undefined) return;
				setLocalPos((current) => {
					const next = { ...current };
					delete next[id];
					return next;
				});
				void run({ op: 'node.update', id, patch: { x: position.x, y: position.y } });
			};
			const cancelMove = () => {
				setLocalPos({});
				void refresh();
			};

			// A narrow column reads top-down: depth-first order, one step per row.
			const autoLayout = () => {
				const byId = new Map(nodes.map((node) => [node.id, node]));
				const order = [];
				const seen = new Set();
				const walk = (id) => {
					if (seen.has(id) || !byId.has(id)) return;
					seen.add(id);
					order.push(id);
					for (const edge of edges) {
						if (edge.from === id) walk(edge.to);
					}
				};
				const start = snapshot !== null && typeof snapshot.entry === 'string'
					? byId.get(snapshot.entry)
					: nodes.find((node) => !edges.some((edge) => edge.to === node.id));
				if (start !== undefined) walk(start.id);
				for (const node of nodes) walk(node.id);
				const positions = {};
				order.forEach((id, index) => {
					positions[id] = { x: 36 + (index % 2) * 22, y: 36 + index * 152 };
				});
				setLocalPos(positions);
				for (const [id, position] of Object.entries(positions)) {
					void postCommand({ sessionId, op: 'node.update', id, patch: position });
				}
			};

			const menuItem = (label, action, danger) => h(
				'button',
				{
					key: label,
					type: 'button',
					onClick: () => {
						setMenuOpen(false);
						action();
					},
					style: {
						appearance: 'none',
						border: 'none',
						background: 'transparent',
						color: danger === true ? C.error : C.text,
						textAlign: 'left',
						padding: '5px 9px',
						borderRadius: '6px',
						fontSize: '12px',
						cursor: 'pointer',
						fontFamily: 'inherit',
						whiteSpace: 'nowrap',
					},
				},
				label,
			);

			const primaryAction = runState === 'running'
				? h(Button, { compact: true, disabled: busy, onClick: () => void run({ op: 'run.pause' }) }, t('action.pause'))
				: runState === 'waiting'
					? h(Button, { compact: true, primary: true, disabled: busy, onClick: () => void run({ op: 'run.resume' }) }, t('action.resume'))
					: h(Button, { compact: true, primary: true, disabled: busy || nodes.length === 0, onClick: () => void run({ op: 'run.start' }) }, t('action.start'));

			const header = h(
				'div',
				{
					style: {
						position: 'relative',
						display: 'flex',
						alignItems: 'center',
						gap: '6px',
						padding: '8px 10px',
						borderBottom: `1px solid ${C.border}`,
						flexWrap: 'wrap',
					},
				},
				h('span', { style: { color: C.accent, display: 'flex' } }, h(Glyph, { size: 16 })),
				h('span', { style: { fontSize: '13px', fontWeight: 600, color: C.text } }, t('tab.title')),
				h(Chip, { color: runColor(runState) }, t(`run.${runState}`)),
				h('span', { style: { flex: 1 } }),
				primaryAction,
				runState === 'running' || runState === 'waiting'
					? h(Button, { compact: true, disabled: busy, onClick: () => void run({ op: 'run.stop' }) }, t('action.stop'))
					: null,
				h(Button, {
					compact: true,
					title: t('action.menu'),
					onClick: () => setMenuOpen((value) => !value),
				}, '⋯'),
				menuOpen
					? h(
						'div',
						{
							style: {
								position: 'absolute',
								right: '8px',
								top: 'calc(100% - 2px)',
								zIndex: 8,
								background: C.surface,
								border: `1px solid ${C.border}`,
								borderRadius: '8px',
								padding: '4px',
								display: 'flex',
								flexDirection: 'column',
								gap: '1px',
								boxShadow: '0 8px 24px rgba(0,0,0,0.28)',
								minWidth: '150px',
							},
						},
						menuItem(t('action.layout'), autoLayout),
						menuItem(t('action.library'), () => {
							setSelected('');
							setSheet('library');
						}),
						menuItem(t('action.reset'), () => void run({ op: 'graph.reset' })),
						menuItem(t('action.clear'), () => {
							setSelected('');
							void run({ op: 'graph.clear' });
						}, true),
					)
					: null,
			);

			const palette = h(
				'div',
				{ style: { display: 'flex', flexWrap: 'wrap', gap: '4px', padding: '6px 10px', borderBottom: `1px solid ${C.border}` } },
				KIND_ORDER.map((kind) =>
					h(Button, {
						key: kind,
						compact: true,
						disabled: busy,
						title: t(`kind.${kind}`),
						onClick: () => addNode(kind),
					}, `+${t(`kind.${kind}`)}`)),
			);

			const canvasArea = h(
				'div',
				{ style: { flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column', padding: '8px', gap: '6px' } },
				h(FlowCanvas, {
					t,
					nodes: renderedNodes,
					edges,
					selected,
					busy,
					onSelect: (id) => {
						setSelected(id);
						setSheet('');
					},
					onMove: moveNode,
					onCommitMove: commitMove,
					onCancelMove: cancelMove,
					onConnect: (from, port, to) => void run({ op: 'edge.add', from, port, to }),
					onRemoveEdge: (id) => void run({ op: 'edge.remove', id }),
				}),
			);

			const inspectorArea = h(
				'div',
				{ style: { flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column' } },
				h(
					'div',
					{ style: { display: 'flex', alignItems: 'center', gap: '6px', padding: '6px 10px', borderBottom: `1px solid ${C.border}` } },
					h(Button, { compact: true, onClick: () => setSelected(''), title: t('action.back') }, '←'),
					h('span', { style: { fontSize: '12px', fontWeight: 600, color: C.text } }, t('inspector.title')),
					h('span', { style: { flex: 1 } }),
					h(Button, {
						compact: true,
						disabled: busy || selectedNode === undefined,
						onClick: () => {
							if (selectedNode === undefined) return;
							const id = selectedNode.id;
							setSelected('');
							void run({ op: 'run.from', id });
						},
					}, t('action.from')),
					h(Button, {
						compact: true,
						danger: true,
						disabled: busy || selectedNode === undefined,
						onClick: () => {
							if (selectedNode === undefined) return;
							const id = selectedNode.id;
							setSelected('');
							void run({ op: 'node.remove', id });
						},
					}, t('action.delete')),
				),
				h(
					'div',
					{ style: { flex: 1, minHeight: 0, overflowY: 'auto', padding: '8px 10px' } },
					h(Inspector, {
						t,
						node: selectedNode,
						catalog: models,
						busy,
						onPatch: (id, patch) => void run({ op: 'node.update', id, patch }),
						onDetach: (id, attachmentId) => void run({ op: 'node.detach', id, attachmentId }),
						onUpload: (id, kind, file) => {
							fileToBase64(file)
								.then((data) => run({ op: 'node.attach', id, kind, data, name: file.name, mediaType: file.type }))
								.catch((err) => { if (mounted.current) setError(err.message || String(err)); });
						},
						onDelete: (id) => {
							setSelected('');
							void run({ op: 'node.remove', id });
						},
						onRunFrom: (id) => {
							setSelected('');
							void run({ op: 'run.from', id });
						},
					}),
				),
			);

			const libraryArea = h(
				'div',
				{ style: { flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column' } },
				h(
					'div',
					{ style: { display: 'flex', alignItems: 'center', gap: '6px', padding: '6px 10px', borderBottom: `1px solid ${C.border}` } },
					h(Button, { compact: true, onClick: () => setSheet(''), title: t('action.back') }, '←'),
					h('span', { style: { fontSize: '12px', fontWeight: 600, color: C.text } }, t('library.title')),
				),
				h(
					'div',
					{ style: { flex: 1, minHeight: 0, overflowY: 'auto', padding: '8px 10px' } },
					h(Library, {
						t,
						packages,
						busy,
						onSave: (name, description) => {
							packagesOp({ op: 'save', sessionId, name, description })
								.then(() => reloadPackages())
								.catch((err) => { if (mounted.current) setError(err.message || String(err)); });
						},
						onLoad: (id) => {
							packagesOp({ op: 'load', sessionId, id })
								.then((value) => {
									if (!mounted.current) return;
									setSnapshot(value);
									setSelected('');
									setSheet('');
								})
								.catch((err) => { if (mounted.current) setError(err.message || String(err)); });
						},
						onDelete: (id) => {
							packagesOp({ op: 'delete', sessionId, id })
								.then(() => reloadPackages())
								.catch((err) => { if (mounted.current) setError(err.message || String(err)); });
						},
					}),
				),
			);

			const body = selectedNode !== undefined ? inspectorArea : sheet === 'library' ? libraryArea : canvasArea;

			const footer = h(
				'div',
				{ style: { borderTop: `1px solid ${C.border}`, padding: '5px 10px', display: 'flex', alignItems: 'center', gap: '6px' } },
				h('span', { style: { fontSize: '10px', color: C.dimmer, flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' } }, t('hint.boundary')),
				h('span', { style: { fontSize: '10px', color: C.dimmer, whiteSpace: 'nowrap' } }, t('run.steps', { n: steps })),
			);

			return h(
				'div',
				{ style: { display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0, color: C.text, fontFamily: 'inherit' } },
				header,
				palette,
				body,
				error !== null ? h('div', { style: { padding: '6px 10px', borderTop: `1px solid ${C.border}`, fontSize: '11px', color: C.error } }, error) : null,
				footer,
			);
		}

		function OpenFlowQueueButton() {
			const t = useT();
			const open = () => {
				try {
					const face = pluginCtx === null ? undefined : pluginCtx.get('sidebarRight');
					if (face !== undefined && typeof face.openTab === 'function') face.openTab(TAB_ID);
				} catch {
					/* nothing else can open the tab; the right sidebar guide still lists it */
				}
			};
			return h(
				'button',
				{
					type: 'button',
					title: t('trigger.open'),
					'aria-label': t('trigger.open'),
					onClick: open,
					style: {
						appearance: 'none',
						border: 'none',
						background: 'transparent',
						color: C.dim,
						cursor: 'pointer',
						display: 'flex',
						alignItems: 'center',
						justifyContent: 'center',
						padding: '6px',
						borderRadius: '6px',
						fontFamily: 'inherit',
					},
				},
				h(Glyph, { size: 16 }),
				h('span', { style: srOnly }, t('trigger.open')),
			);
		}

		//#endregion


		const CSS = `
@keyframes fq-pulse { 0% { opacity: 1 } 50% { opacity: .45 } 100% { opacity: 1 } }
[data-fq-node] { transition: box-shadow .12s ease; }
`;

		const inject = ['slots', 'locale', 'sidebarRightTabs'];

		function apply(ctx) {
			pluginCtx = ctx;
			const locale = ctx.get('locale');
			if (locale !== null && locale !== undefined && typeof locale.register === 'function') {
				localeSvc = locale;
				ctx.effect(() => locale.register(NS, { zh: DICT.zh, en: DICT.en }), 'dsh-flow-queue: locale dictionary');
			}

			const tabs = ctx.get('sidebarRightTabs');
			if (tabs !== null && tabs !== undefined && typeof tabs.register === 'function') {
				const title = () => ttext('tab.title');
				const description = () => ttext('tab.guide.description');
				ctx.effect(
					() =>
						tabs.register({
							id: TAB_ID,
							kind: TAB_ID,
							title,
							guide: [{ order: 15, title, description, icon: Glyph }],
						}),
					'dsh-flow-queue: right sidebar tab type',
				);
			}

			ctx.slots.inject('sidebar.right.pane.tab', () =>
				ctx.slots.register({ name: 'sidebar.right.pane.tab', key: TAB_ID }, FlowQueuePanel));

			ctx.slots.inject('sidebar.footer.action', () =>
				ctx.slots.register(
					{ name: 'sidebar.footer.action', id: 'flow-queue', order: 20, label: () => ttext('tab.title') },
					OpenFlowQueueButton,
				));

			ctx.effect(() => {
				const style = document.createElement('style');
				style.setAttribute('data-dsh-plugin', 'flow-queue');
				style.textContent = CSS;
				document.head.appendChild(style);
				return () => {
					style.remove();
				};
			}, 'dsh-flow-queue: styles');
		}

		return { name: 'dsh-flow-queue', inject, apply };
	},
});
