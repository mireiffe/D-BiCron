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

// ── Extract arrowPath (with mock CS.layout) ──────────────────

let mockLayout = { gapColumns: [], dbContainers: [] };

function arrowPath(conn) {
  const s = conn.source, t = conn.target;
  const sy = s.y + s.h / 2;
  const ty = t.y + t.h / 2;

  if (conn.type === "fk" && s.dbKey && s.dbKey === t.dbKey) {
    const rightEdge = Math.max(s.x + s.w, t.x + t.w);
    const dist = Math.abs(sy - ty);
    const bump = Math.min(60, Math.max(25, dist * 0.35));
    return `M${s.x + s.w},${sy} C${rightEdge + bump},${sy} ${rightEdge + bump},${ty} ${t.x + t.w},${ty}`;
  }

  const sx = s.x + (s.w || 0);
  const tx = t.x;

  if (tx > sx + 10) {
    const gaps = mockLayout.gapColumns || [];
    const relevant = gaps.filter(g => g.midX > sx && g.midX < tx);
    if (relevant.length <= 1) {
      const mx = relevant.length === 1 ? relevant[0].midX : (sx + tx) / 2;
      return `M${sx},${sy} C${mx},${sy} ${mx},${ty} ${tx},${ty}`;
    }
    const cx1 = relevant[0].midX;
    const cx2 = relevant[relevant.length - 1].midX;
    return `M${sx},${sy} C${cx1},${sy} ${cx2},${ty} ${tx},${ty}`;
  }

  const containers = mockLayout.dbContainers || [];
  let topY = Math.min(sy, ty);
  for (const c of containers) {
    if (c.x + c.w > Math.min(tx, sx) - 10 && c.x < Math.max(tx, sx) + 10) {
      topY = Math.min(topY, c.y);
    }
  }
  topY -= 40;
  const loopOut = 50;
  return `M${sx},${sy} C${sx + loopOut},${sy} ${sx + loopOut},${topY} ${(sx + tx) / 2},${topY}` +
    ` C${tx - loopOut},${topY} ${tx - loopOut},${ty} ${tx},${ty}`;
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

// Test: forward connection with no gaps (adjacent DBs)
{
  mockLayout = { gapColumns: [], dbContainers: [] };
  const conn = {
    type: "pipeline",
    source: { x: 100, y: 50, w: 200, h: 36 },
    target: { x: 500, y: 100, w: 200, h: 36 },
  };
  const path = arrowPath(conn);
  assert(path.startsWith("M300,68"), "forward no-gap: starts at source right edge");
  assert(path.includes("C400,"), "forward no-gap: control at midpoint x=400");
  console.log(`  forward no-gap: ${path.slice(0, 50)}...`);
}

// Test: forward connection through single gap
{
  mockLayout = { gapColumns: [{ midX: 450 }], dbContainers: [] };
  const conn = {
    type: "pipeline",
    source: { x: 100, y: 50, w: 200, h: 36 },
    target: { x: 600, y: 100, w: 200, h: 36 },
  };
  const path = arrowPath(conn);
  assert(path.includes("C450,"), "single gap: control at gap midX=450");
  console.log(`  single gap: ${path.slice(0, 50)}...`);
}

// Test: forward connection through multiple gaps
{
  mockLayout = { gapColumns: [{ midX: 400 }, { midX: 600 }, { midX: 800 }], dbContainers: [] };
  const conn = {
    type: "pipeline",
    source: { x: 100, y: 50, w: 200, h: 36 },
    target: { x: 900, y: 100, w: 200, h: 36 },
  };
  const path = arrowPath(conn);
  assert(path.includes("C400,"), "multi-gap: first control at gap 400");
  assert(path.includes("800,"), "multi-gap: second control at gap 800");
  console.log(`  multi-gap: ${path.slice(0, 60)}...`);
}

// Test: FK same-DB connection — uses right-side bump
{
  mockLayout = { gapColumns: [], dbContainers: [] };
  const conn = {
    type: "fk",
    source: { x: 400, y: 50, w: 200, h: 36, dbKey: "pg" },
    target: { x: 400, y: 150, w: 200, h: 36, dbKey: "pg" },
  };
  const path = arrowPath(conn);
  // Source exits right (600), target enters right (600), bump goes beyond 600
  assert(path.startsWith("M600,68"), "fk: starts at source right edge");
  assert(path.endsWith(",168"), "fk: ends at target center-y");
  // Bump should push control points beyond rightEdge (600)
  const match = path.match(/C(\d+)/);
  assert(match && parseInt(match[1]) > 600, "fk: control point is right of edge");
  console.log(`  fk same-db: ${path.slice(0, 60)}...`);
}

// Test: backward connection — routes above
{
  mockLayout = {
    gapColumns: [],
    dbContainers: [
      { x: 100, y: 0, w: 250, h: 300, key: "a" },
      { x: 500, y: 0, w: 250, h: 300, key: "b" },
    ],
  };
  const conn = {
    type: "pipeline",
    source: { x: 500, y: 150, w: 200, h: 36 },
    target: { x: 100, y: 100, w: 200, h: 36 },
  };
  const path = arrowPath(conn);
  // Should route above (negative y or at least above container top y=0)
  assert(path.includes("-40"), "backward: routes above containers (y = -40)");
  console.log(`  backward: ${path.slice(0, 70)}...`);
}

// ══════════════════════════════════════════════════════════════
// Summary
// ══════════════════════════════════════════════════════════════

console.log(`\n${passed + failed} tests: ${passed} passed, ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
