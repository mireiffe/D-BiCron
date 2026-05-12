/**
 * Unit tests for canvas layout pure functions.
 * Run: node tests/test_canvas_layout.js
 */

let passed = 0, failed = 0;

function assert(cond, msg) {
  if (cond) { passed++; }
  else { failed++; console.error(`  FAIL: ${msg}`); }
}

function assertDeepEqual(actual, expected, msg) {
  const a = JSON.stringify(actual), b = JSON.stringify(expected);
  assert(a === b, `${msg}\n    expected: ${b}\n    actual:   ${a}`);
}

// ── Extract topoSortDBs ──────────────────────────────────────

function topoSortDBs(dbKeys, pipeConns, epConns) {
  const adj = new Map();
  const inDeg = new Map();
  for (const k of dbKeys) { adj.set(k, new Set()); inDeg.set(k, 0); }
  for (const c of pipeConns) {
    const from = c.from.db, to = c.to.db;
    if (from !== to && adj.has(from) && adj.has(to) && !adj.get(from).has(to)) {
      adj.get(from).add(to);
      inDeg.set(to, inDeg.get(to) + 1);
    }
  }
  const epTargetDBs = new Set();
  for (const epc of epConns) { if (epc.target?.db) epTargetDBs.add(epc.target.db); }
  const queue = dbKeys.filter(k => inDeg.get(k) === 0)
    .sort((a, b) => (epTargetDBs.has(a) ? 0 : 1) - (epTargetDBs.has(b) ? 0 : 1));
  const sorted = [];
  while (queue.length) {
    const u = queue.shift();
    sorted.push(u);
    for (const v of adj.get(u)) {
      inDeg.set(v, inDeg.get(v) - 1);
      if (inDeg.get(v) === 0) queue.push(v);
    }
  }
  for (const k of dbKeys) { if (!sorted.includes(k)) sorted.push(k); }
  return sorted;
}

// ── Extract arrowPath ────────────────────────────────────────

function connectionEndpoints(conn) {
  const s = conn.source, t = conn.target;
  const scx = s.x + (s.w || 0) / 2;
  const scy = s.y + (s.h || 0) / 2;
  const tcx = t.x + (t.w || 0) / 2;
  const tcy = t.y + (t.h || 0) / 2;
  const dx = tcx - scx;
  const dy = tcy - scy;

  if (Math.abs(dx) >= Math.abs(dy)) {
    return {
      sx: dx >= 0 ? s.x + (s.w || 0) : s.x,
      sy: scy,
      tx: dx >= 0 ? t.x : t.x + (t.w || 0),
      ty: tcy,
      axis: "x",
      dir: dx >= 0 ? 1 : -1,
    };
  }
  return {
    sx: scx,
    sy: dy >= 0 ? s.y + (s.h || 0) : s.y,
    tx: tcx,
    ty: dy >= 0 ? t.y : t.y + (t.h || 0),
    axis: "y",
    dir: dy >= 0 ? 1 : -1,
  };
}

function connectionMidpoint(conn) {
  const p = connectionEndpoints(conn);
  return { x: (p.sx + p.tx) / 2, y: (p.sy + p.ty) / 2 };
}

function arrowPath(conn) {
  const p = connectionEndpoints(conn);
  const dist = Math.hypot(p.tx - p.sx, p.ty - p.sy);
  const bend = Math.min(210, Math.max(60, dist * 0.42));

  if (p.axis === "x") {
    return `M${p.sx},${p.sy} C${p.sx + bend * p.dir},${p.sy} ${p.tx - bend * p.dir},${p.ty} ${p.tx},${p.ty}`;
  }
  return `M${p.sx},${p.sy} C${p.sx},${p.sy + bend * p.dir} ${p.tx},${p.ty - bend * p.dir} ${p.tx},${p.ty}`;
}

// ══════════════════════════════════════════════════════════════
// topoSortDBs tests
// ══════════════════════════════════════════════════════════════

console.log("topoSortDBs:");

// Test: linear pipeline A → B → C
{
  const dbs = ["c", "a", "b"];
  const pipes = [
    { from: { db: "a" }, to: { db: "b" } },
    { from: { db: "b" }, to: { db: "c" } },
  ];
  const result = topoSortDBs(dbs, pipes, []);
  assert(result.indexOf("a") < result.indexOf("b"), "a before b");
  assert(result.indexOf("b") < result.indexOf("c"), "b before c");
  console.log(`  linear pipeline: [${result}]`);
}

// Test: no connections — original order preserved
{
  const dbs = ["x", "y", "z"];
  const result = topoSortDBs(dbs, [], []);
  assertDeepEqual(result, ["x", "y", "z"], "no conns preserves order");
  console.log(`  no connections: [${result}]`);
}

// Test: EP-targeted DB comes first among roots
{
  const dbs = ["pg", "ch", "mysql"];
  const pipes = [
    { from: { db: "pg" }, to: { db: "ch" } },
    { from: { db: "mysql" }, to: { db: "ch" } },
  ];
  const eps = [{ target: { db: "pg" } }];
  const result = topoSortDBs(dbs, pipes, eps);
  assert(result[0] === "pg", "EP-target pg is first root");
  assert(result.indexOf("ch") > result.indexOf("pg"), "ch after pg");
  assert(result.indexOf("ch") > result.indexOf("mysql"), "ch after mysql");
  console.log(`  EP priority: [${result}]`);
}

// Test: cycle handled gracefully
{
  const dbs = ["a", "b"];
  const pipes = [
    { from: { db: "a" }, to: { db: "b" } },
    { from: { db: "b" }, to: { db: "a" } },
  ];
  const result = topoSortDBs(dbs, pipes, []);
  assert(result.length === 2, "cycle: all DBs present");
  assert(result.includes("a") && result.includes("b"), "cycle: both a and b");
  console.log(`  cycle: [${result}]`);
}

// Test: diamond A → B, A → C, B → D, C → D
{
  const dbs = ["d", "b", "c", "a"];
  const pipes = [
    { from: { db: "a" }, to: { db: "b" } },
    { from: { db: "a" }, to: { db: "c" } },
    { from: { db: "b" }, to: { db: "d" } },
    { from: { db: "c" }, to: { db: "d" } },
  ];
  const result = topoSortDBs(dbs, pipes, []);
  assert(result[0] === "a", "diamond: a is first");
  assert(result[result.length - 1] === "d", "diamond: d is last");
  console.log(`  diamond: [${result}]`);
}

// Test: duplicate edges
{
  const dbs = ["a", "b"];
  const pipes = [
    { from: { db: "a" }, to: { db: "b" } },
    { from: { db: "a" }, to: { db: "b" } },
    { from: { db: "a" }, to: { db: "b" } },
  ];
  const result = topoSortDBs(dbs, pipes, []);
  assertDeepEqual(result, ["a", "b"], "duplicate edges: a then b");
  console.log(`  duplicate edges: [${result}]`);
}

// ══════════════════════════════════════════════════════════════
// arrowPath tests
// ══════════════════════════════════════════════════════════════

console.log("\narrowPath:");

// Test: rightward connection uses nearest horizontal sides
{
  const conn = {
    type: "pipeline",
    source: { x: 100, y: 50, w: 200, h: 36 },
    target: { x: 500, y: 100, w: 200, h: 36 },
  };
  const path = arrowPath(conn);
  assert(path.startsWith("M300,68"), "rightward: starts at source right edge");
  assert(path.endsWith("500,118"), "rightward: ends at target left edge");
  console.log(`  rightward: ${path.slice(0, 55)}...`);
}

// Test: leftward connection uses nearest horizontal sides
{
  const conn = {
    type: "pipeline",
    source: { x: 500, y: 100, w: 200, h: 36 },
    target: { x: 100, y: 50, w: 200, h: 36 },
  };
  const path = arrowPath(conn);
  assert(path.startsWith("M500,118"), "leftward: starts at source left edge");
  assert(path.endsWith("300,68"), "leftward: ends at target right edge");
  console.log(`  leftward: ${path.slice(0, 55)}...`);
}

// Test: vertical connection uses top/bottom sides
{
  const conn = {
    type: "pipeline",
    source: { x: 100, y: 50, w: 200, h: 36 },
    target: { x: 120, y: 250, w: 200, h: 36 },
  };
  const path = arrowPath(conn);
  assert(path.startsWith("M200,86"), "vertical: starts at source bottom center");
  assert(path.endsWith("220,250"), "vertical: ends at target top center");
  console.log(`  vertical: ${path.slice(0, 55)}...`);
}

// Test: FK same-DB connection still routes between nearest sides
{
  const conn = {
    type: "fk",
    source: { x: 400, y: 50, w: 200, h: 36, dbKey: "pg" },
    target: { x: 400, y: 150, w: 200, h: 36, dbKey: "pg" },
  };
  const path = arrowPath(conn);
  assert(path.startsWith("M500,86"), "fk: starts at source bottom center");
  assert(path.endsWith("500,150"), "fk: ends at target top center");
  console.log(`  fk same-db: ${path.slice(0, 60)}...`);
}

// Test: connectionMidpoint returns midpoint of routed endpoints
{
  const conn = {
    type: "pipeline",
    source: { x: 100, y: 50, w: 200, h: 36 },
    target: { x: 500, y: 100, w: 200, h: 36 },
  };
  const mid = connectionMidpoint(conn);
  assertDeepEqual(mid, { x: 400, y: 93 }, "midpoint uses routed endpoints");
  console.log(`  midpoint: ${JSON.stringify(mid)}`);
}

// ══════════════════════════════════════════════════════════════
// resolveDbOrder tests (canvas_order vs topo sort)
// ══════════════════════════════════════════════════════════════

// Replicate the logic from computeLayout
function resolveDbOrder(rawDbKeys, databases, pipeConns, epConns) {
  const orderMap = new Map();
  for (const d of databases) { if (d.canvas_order != null) orderMap.set(d.id, d.canvas_order); }
  if (orderMap.size > 0) {
    return [...rawDbKeys].sort((a, b) => (orderMap.get(a) ?? Infinity) - (orderMap.get(b) ?? Infinity));
  }
  return topoSortDBs(rawDbKeys, pipeConns, epConns);
}

console.log("\nresolveDbOrder:");

// Test: canvas_order overrides topo sort
{
  const dbs = [{ id: "ch", canvas_order: 0 }, { id: "pg", canvas_order: 1 }, { id: "mysql", canvas_order: 2 }];
  const pipes = [{ from: { db: "pg" }, to: { db: "ch" } }]; // topo would put pg first
  const result = resolveDbOrder(["pg", "ch", "mysql"], dbs, pipes, []);
  assertDeepEqual(result, ["ch", "pg", "mysql"], "canvas_order overrides topo");
  console.log(`  canvas_order overrides: [${result}]`);
}

// Test: no canvas_order falls back to topo sort
{
  const dbs = [{ id: "a" }, { id: "b" }];
  const pipes = [{ from: { db: "a" }, to: { db: "b" } }];
  const result = resolveDbOrder(["b", "a"], dbs, pipes, []);
  assertDeepEqual(result, ["a", "b"], "no canvas_order: topo sort used");
  console.log(`  no canvas_order fallback: [${result}]`);
}

// Test: partial canvas_order — ordered DBs first, rest at end
{
  const dbs = [{ id: "x", canvas_order: 0 }, { id: "y" }, { id: "z", canvas_order: 1 }];
  const result = resolveDbOrder(["y", "z", "x"], dbs, [], []);
  assert(result[0] === "x", "partial: x (order 0) first");
  assert(result[1] === "z", "partial: z (order 1) second");
  assert(result[2] === "y", "partial: y (no order) last");
  console.log(`  partial order: [${result}]`);
}

// Test: canvas_order with value 0 is respected (not treated as falsy)
{
  const dbs = [{ id: "a", canvas_order: 2 }, { id: "b", canvas_order: 0 }, { id: "c", canvas_order: 1 }];
  const result = resolveDbOrder(["a", "b", "c"], dbs, [], []);
  assertDeepEqual(result, ["b", "c", "a"], "order 0 is valid");
  console.log(`  zero order: [${result}]`);
}

// ══════════════════════════════════════════════════════════════
// Summary
// ══════════════════════════════════════════════════════════════

console.log(`\n${passed + failed} tests: ${passed} passed, ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
