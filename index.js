// dsh-flow-queue — package entry.
//
// The implementation lives in the newest versioned `host*.js` file and is
// re-exported here. `cordis.patch.yml` targets that same versioned subpath on
// purpose: a running Harness caches an imported module by its resolved URL, so
// pointing the row at a fresh file is what makes a Host update visible without
// restarting the app.
export { apply, inject } from './host2.js';
