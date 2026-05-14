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

// ── Extract table filter helpers ──────────────────────────────

function globMatch(pattern, str) {
  const re = new RegExp("^" + pattern.replace(/[.+^${}()|[\]\\]/g, "\\$&")
    .replace(/\*/g, ".*").replace(/\?/g, ".") + "$");
  return re.test(str);
}

function tableNameCandidates(tableName) {
  if (!tableName.includes(".")) return [tableName];
  return [tableName, tableName.split(".").pop()];
}

function tablePatternMatch(pattern, tableName) {
  return tableNameCandidates(tableName).some(name => globMatch(pattern, name));
}

function shouldIncludeTable(tableName, dbCfg) {
  const includes = dbCfg.include_tables || [];
  const excludes = dbCfg.exclude_tables || [];
  if (includes.length && !includes.some(pat => tablePatternMatch(pat, tableName))) return false;
  if (excludes.length && excludes.some(pat => tablePatternMatch(pat, tableName))) return false;
  return true;
}

const TABLE_COLLIDE_PAD = 18, SAME_SCHEMA_TABLE_COLLIDE_PAD = 8;
const ENTRY_DB_COLLIDE_PAD = 34;

function tableCollisionPadding(a, b) {
  if (a?.type === "table" && b?.type === "table" && a.groupKey && a.groupKey === b.groupKey) {
    return SAME_SCHEMA_TABLE_COLLIDE_PAD;
  }
  return TABLE_COLLIDE_PAD;
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

function containerOverlapMoves(containers, padding) {
  const moves = new Map();
  const addMove = (key, dx, dy) => {
    const cur = moves.get(key) || { x: 0, y: 0 };
    cur.x += dx;
    cur.y += dy;
    moves.set(key, cur);
  };

  for (let i = 0; i < containers.length; i++) {
    for (let j = i + 1; j < containers.length; j++) {
      const a = containers[i];
      const b = containers[j];
      const ax = a.x + a.w / 2;
      const ay = a.y + a.h / 2;
      const bx = b.x + b.w / 2;
      const by = b.y + b.h / 2;
      let dx = ax - bx;
      let dy = ay - by;
      if (dx === 0 && dy === 0) {
        dx = i < j ? -0.1 : 0.1;
        dy = i < j ? -0.1 : 0.1;
      }

      const overlapX = (a.w + b.w) / 2 + padding - Math.abs(dx);
      const overlapY = (a.h + b.h) / 2 + padding - Math.abs(dy);
      if (overlapX <= 0 || overlapY <= 0) continue;

      if (overlapX < overlapY) {
        const dir = dx < 0 ? -1 : 1;
        const push = overlapX / 2 + 1;
        addMove(a.key, dir * push, 0);
        addMove(b.key, -dir * push, 0);
      } else {
        const dir = dy < 0 ? -1 : 1;
        const push = overlapY / 2 + 1;
        addMove(a.key, 0, dir * push);
        addMove(b.key, 0, -dir * push);
      }
    }
  }

  return moves;
}

function schemaOverlapMoves(layout, padding) {
  const moves = new Map();
  const addMove = (key, dx, dy) => {
    const cur = moves.get(key) || { x: 0, y: 0 };
    cur.x += dx;
    cur.y += dy;
    moves.set(key, cur);
  };
  const byDb = new Map();

  for (const group of layout?.schemaGroups || []) {
    const groups = byDb.get(group.dbKey) || [];
    groups.push(group);
    byDb.set(group.dbKey, groups);
  }

  for (const groups of byDb.values()) {
    const groupMoves = containerOverlapMoves(groups, padding);
    for (const [key, move] of groupMoves) addMove(key, move.x, move.y);
  }

  return moves;
}

function entryDbOverlapMoves(layout, padding) {
  const entryMoves = new Map();
  const dbMoves = new Map();
  const addMove = (map, key, dx, dy) => {
    const cur = map.get(key) || { x: 0, y: 0 };
    cur.x += dx;
    cur.y += dy;
    map.set(key, cur);
  };

  const entries = layout?.entryPoints || [];
  const containers = layout?.dbContainers || [];
  for (const entry of entries) {
    for (const db of containers) {
      const ex = entry.x + entry.w / 2;
      const ey = entry.y + entry.h / 2;
      const dx = ex - (db.x + db.w / 2);
      const dy = ey - (db.y + db.h / 2);
      const overlapX = (entry.w + db.w) / 2 + padding - Math.abs(dx);
      const overlapY = (entry.h + db.h) / 2 + padding - Math.abs(dy);
      if (overlapX <= 0 || overlapY <= 0) continue;

      if (overlapX < overlapY) {
        const dir = dx < 0 ? -1 : 1;
        const push = overlapX + 1;
        addMove(entryMoves, entry.key, dir * push * 0.82, 0);
        addMove(dbMoves, db.key, -dir * push * 0.18, 0);
      } else {
        const dir = dy < 0 ? -1 : 1;
        const push = overlapY + 1;
        addMove(entryMoves, entry.key, 0, dir * push * 0.82);
        addMove(dbMoves, db.key, 0, -dir * push * 0.18);
      }
    }
  }

  return { entries: entryMoves, dbs: dbMoves };
}

// ══════════════════════════════════════════════════════════════
// table filters tests
// ══════════════════════════════════════════════════════════════

console.log("table filters:");

// Test: schema-qualified include matches full schema.table name
{
  const cfg = { include_tables: ["sales.*"] };
  assert(shouldIncludeTable("sales.orders", cfg), "schema include: sales.orders included");
  assert(!shouldIncludeTable("public.orders", cfg), "schema include: public.orders excluded");
  console.log("  schema include: sales.*");
}

// Test: existing table-only filters still match schema-qualified names
{
  const cfg = { include_tables: ["orders"] };
  assert(shouldIncludeTable("public.orders", cfg), "table-only include matches qualified table");
  assert(!shouldIncludeTable("public.users", cfg), "table-only include rejects other tables");
  console.log("  table-only include with qualified names");
}

// Test: schema-qualified exclude filters out a whole schema
{
  const cfg = { exclude_tables: ["archive.*"] };
  assert(shouldIncludeTable("public.orders", cfg), "schema exclude keeps public.orders");
  assert(!shouldIncludeTable("archive.orders", cfg), "schema exclude removes archive.orders");
  console.log("  schema exclude: archive.*");
}

// ══════════════════════════════════════════════════════════════
// topoSortDBs tests
// ══════════════════════════════════════════════════════════════

console.log("\ntopoSortDBs:");

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
// containerOverlapMoves tests
// ══════════════════════════════════════════════════════════════

console.log("\ncontainerOverlapMoves:");

// Test: same-schema table collision padding is tighter than generic padding
{
  const same = tableCollisionPadding(
    { type: "table", groupKey: "db:public" },
    { type: "table", groupKey: "db:public" },
  );
  const other = tableCollisionPadding(
    { type: "table", groupKey: "db:public" },
    { type: "table", groupKey: "db:raw" },
  );
  assert(same < other, "same-schema collision padding is tighter");
  assertDeepEqual({ same, other }, { same: 8, other: 18 }, "collision padding values");
  console.log(`  table collision padding: same=${same}, other=${other}`);
}

// Test: overlapping DB boxes push apart on the shallow axis
{
  const moves = containerOverlapMoves([
    { key: "a", x: 0, y: 0, w: 100, h: 100 },
    { key: "b", x: 80, y: 0, w: 100, h: 100 },
  ], 10);
  assertDeepEqual(moves.get("a"), { x: -16, y: 0 }, "horizontal overlap: a moves left");
  assertDeepEqual(moves.get("b"), { x: 16, y: 0 }, "horizontal overlap: b moves right");
  console.log(`  horizontal overlap: a=${JSON.stringify(moves.get("a"))}, b=${JSON.stringify(moves.get("b"))}`);
}

// Test: vertically overlapping DB boxes push apart vertically
{
  const moves = containerOverlapMoves([
    { key: "a", x: 0, y: 0, w: 100, h: 100 },
    { key: "b", x: 0, y: 70, w: 100, h: 100 },
  ], 10);
  assertDeepEqual(moves.get("a"), { x: 0, y: -21 }, "vertical overlap: a moves up");
  assertDeepEqual(moves.get("b"), { x: 0, y: 21 }, "vertical overlap: b moves down");
  console.log(`  vertical overlap: a=${JSON.stringify(moves.get("a"))}, b=${JSON.stringify(moves.get("b"))}`);
}

// Test: boxes outside padding do not move
{
  const moves = containerOverlapMoves([
    { key: "a", x: 0, y: 0, w: 100, h: 100 },
    { key: "b", x: 120, y: 0, w: 100, h: 100 },
  ], 10);
  assert(moves.size === 0, "no overlap: no moves");
  console.log("  no overlap: no moves");
}

// ══════════════════════════════════════════════════════════════
// schemaOverlapMoves tests
// ══════════════════════════════════════════════════════════════

console.log("\nschemaOverlapMoves:");

// Test: overlapping schema groups in the same DB push apart
{
  const moves = schemaOverlapMoves({
    schemaGroups: [
      { key: "db:public", dbKey: "db", x: 0, y: 0, w: 100, h: 100 },
      { key: "db:raw", dbKey: "db", x: 80, y: 0, w: 100, h: 100 },
    ],
  }, 10);
  assertDeepEqual(moves.get("db:public"), { x: -16, y: 0 }, "same DB schema overlap: public moves left");
  assertDeepEqual(moves.get("db:raw"), { x: 16, y: 0 }, "same DB schema overlap: raw moves right");
  console.log(`  same DB overlap: public=${JSON.stringify(moves.get("db:public"))}, raw=${JSON.stringify(moves.get("db:raw"))}`);
}

// Test: overlapping schema groups in different DBs do not push each other
{
  const moves = schemaOverlapMoves({
    schemaGroups: [
      { key: "db1:public", dbKey: "db1", x: 0, y: 0, w: 100, h: 100 },
      { key: "db2:public", dbKey: "db2", x: 80, y: 0, w: 100, h: 100 },
    ],
  }, 10);
  assert(moves.size === 0, "different DB schemas: no moves");
  console.log("  different DB overlap: no moves");
}

// ══════════════════════════════════════════════════════════════
// entryDbOverlapMoves tests
// ══════════════════════════════════════════════════════════════

console.log("\nentryDbOverlapMoves:");

// Test: an API node overlapping a DB box is pushed outside and nudges the DB away
{
  const moves = entryDbOverlapMoves({
    entryPoints: [{ key: "api", x: 0, y: 0, w: 100, h: 50 }],
    dbContainers: [{ key: "db", x: 80, y: 0, w: 200, h: 100 }],
  }, ENTRY_DB_COLLIDE_PAD);
  const apiMove = moves.entries.get("api");
  const dbMove = moves.dbs.get("db");
  assert(apiMove.x < 0 && apiMove.y === 0, "entry/db overlap: api moves left");
  assert(dbMove.x > 0 && dbMove.y === 0, "entry/db overlap: db moves right");
  assert(Math.abs(apiMove.x) > Math.abs(dbMove.x), "entry/db overlap: api gets the larger push");
  console.log(`  overlap: api=${JSON.stringify(apiMove)}, db=${JSON.stringify(dbMove)}`);
}

// Test: an API node already outside DB padding is not moved
{
  const moves = entryDbOverlapMoves({
    entryPoints: [{ key: "api", x: 0, y: 0, w: 100, h: 50 }],
    dbContainers: [{ key: "db", x: 250, y: 0, w: 200, h: 100 }],
  }, ENTRY_DB_COLLIDE_PAD);
  assert(moves.entries.size === 0 && moves.dbs.size === 0, "entry/db no overlap: no moves");
  console.log("  no overlap: no moves");
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
