// Offline check for the client half's activation path.
//
// Emulates the browser module loader, a minimal React, the DOM bits `apply`
// touches, and a Cordis-like context, then runs `apply` and reports every
// registration it made. Catches activation-time throws without a live page.
//
//   node test/client-apply-check.mjs

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const source = readFileSync(path.join(here, '..', 'client.js'), 'utf8');

let factory;
const fakeWindow = {
	__ModuleLoader__: {
		load(definition) {
			factory = definition.factory;
		},
	},
};
const fakeDocument = {
	createElement() {
		return { setAttribute() {}, remove() {}, textContent: '' };
	},
	head: { appendChild() {} },
	addEventListener() {},
	removeEventListener() {},
};

const reactStub = {
	createElement: () => null,
	useState: (initial) => [typeof initial === 'function' ? initial() : initial, () => {}],
	useEffect: () => {},
	useCallback: (fn) => fn,
	useMemo: (fn) => fn(),
	useRef: (value) => ({ current: value }),
	useLayoutEffect: () => {},
};

const calls = { tabs: [], slots: [], locale: [], effects: [], injects: [] };
const context = {
	get(name) {
		if (name === 'locale') {
			return {
				register: (ns, dicts) => {
					calls.locale.push({ ns, locales: Object.keys(dicts) });
					return () => {};
				},
				bind: () => (key) => key,
				subscribe: () => () => {},
			};
		}
		if (name === 'sidebarRightTabs') {
			return {
				register: (definition) => {
					calls.tabs.push(definition);
					return () => {};
				},
			};
		}
		if (name === 'sidebarRight') return { openTab: () => {} };
		return undefined;
	},
	effect(run, label) {
		calls.effects.push(label);
		const dispose = run();
		return typeof dispose === 'function' ? dispose : () => {};
	},
	slots: {
		inject(key, callback) {
			calls.injects.push(key);
			const result = callback();
			return () => {};
		},
		register(options, component) {
			calls.slots.push({ options, component });
			return () => {};
		},
	},
};

const sandbox = { window: fakeWindow, document: fakeDocument, fetch: async () => ({ json: async () => ({}) }) };
const runner = new Function('window', 'document', 'fetch', `${source}\nreturn window.__ModuleLoader__;`);
runner(sandbox.window, fakeDocument, sandbox.fetch);

assert.ok(typeof factory === 'function', 'the module loader should receive a factory');
const moduleExports = factory((name) => {
	if (name === 'react') return reactStub;
	throw new Error(`unexpected require(${name})`);
});

assert.equal(moduleExports.name, 'dsh-flow-queue');
assert.ok(Array.isArray(moduleExports.inject), 'the module should declare inject');
moduleExports.apply(context);

console.log('locale registrations:', JSON.stringify(calls.locale));
console.log('tab registrations:', calls.tabs.map((tab) => `${tab.id}/${tab.kind} guide=${Array.isArray(tab.guide) ? tab.guide.length : 0}`));
console.log('slot registrations:', calls.slots.map((slot) => `${slot.options.name}[${slot.options.key ?? slot.options.id}] -> ${slot.component && slot.component.name}`));
console.log('slot injects:', JSON.stringify(calls.injects));
console.log('effects:', JSON.stringify(calls.effects));

assert.equal(calls.locale.length, 1, 'one locale dictionary');
assert.equal(calls.tabs.length, 1, 'one tab type');
assert.equal(calls.tabs[0].id, 'flow-queue');
assert.equal(calls.tabs[0].guide.length, 1, 'one guide entry');
assert.ok(calls.slots.some((slot) => slot.options.name === 'sidebar.right.pane.tab' && slot.options.key === 'flow-queue'), 'the tab body is registered');
assert.ok(calls.slots.some((slot) => slot.options.name === 'sidebar.footer.action' && slot.options.id === 'flow-queue'), 'the sidebar footer action is registered');

console.log('dsh-flow-queue client apply check: passed');
