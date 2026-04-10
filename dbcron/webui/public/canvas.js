/* ================================================================
   D-BiCron Canvas — D3.js pipeline visualization
   ================================================================ */

const CS = {
  metadata: null,
  pipelineConfig: null,
  layout: null,
  selectedTable: null,
  nodePositions: {},
};

// Layout constants
const TABLE_W = 200, TABLE_H = 36, TABLE_PAD = 10;
const DB_PAD_TOP = 48, DB_PAD_X = 24, DB_PAD_BOTTOM = 24;
const EP_W = 150, EP_H = 40;
const DB_GAP = 200;

let svgEl, gRoot, zoomBehavior;

// ── Helpers ────────────────────────────────────────────────────

function _el(tag, attrs, children) {
  const el = document.createElement(tag);
  if (attrs) for (const [k, v] of Object.entries(attrs)) {
    if (k === "className") el.className = v;
    else if (k === "textContent") el.textContent = v;
    else if (k === "style" && typeof v === "object") Object.assign(el.style, v);
    else el.setAttribute(k, v);
  }
  if (typeof children === "string") el.textContent = children;
  else if (Array.isArray(children)) for (const c of children) { if (c) el.appendChild(c); }
  return el;
}

function formatCount(n) {
  if (n >= 1e9) return (n / 1e9).toFixed(1) + "B";
  if (n >= 1e6) return (n / 1e6).toFixed(1) + "M";
  if (n >= 1e3) return (n / 1e3).toFixed(1) + "K";
  return String(n);
}

// ── Initialization ─────────────────────────────────────────────

function canvasInit() {
  loadSavedPositions();
  svgEl = d3.select("#pipeline-canvas");
  gRoot = svgEl.append("g").attr("class", "canvas-root");

  zoomBehavior = d3.zoom()
    .scaleExtent([0.15, 3])
    .on("zoom", (event) => gRoot.attr("transform", event.transform));
  svgEl.call(zoomBehavior);

  canvasResize();
  const ro = new ResizeObserver(() => canvasResize());
  ro.observe(document.getElementById("canvas-view"));

  loadCanvasData();
}

function canvasResize() {
  const container = document.getElementById("canvas-view");
  if (!container) return;
  svgEl.attr("width", container.clientWidth).attr("height", container.clientHeight);
}

// ── Data loading ───────────────────────────────────────────────

async function loadCanvasData() {
  const [metaRes, pipeRes, dbRes] = await Promise.allSettled([
    fetch("/api/metadata").then(r => r.ok ? r.json() : null),
    fetch("/api/pipeline-config").then(r => r.ok ? r.json() : null),
    fetch("/api/databases").then(r => r.ok ? r.json() : []),
  ]);

  CS.metadata = metaRes.status === "fulfilled" ? metaRes.value : null;
  CS.pipelineConfig = pipeRes.status === "fulfilled" ? pipeRes.value : null;
  CS.databases = dbRes.status === "fulfilled" ? dbRes.value : [];
  CS.drift = null;

  if (!CS.metadata) {
    const emptyEl = document.getElementById("canvas-empty");
    const emptyMsg = emptyEl.querySelector(".empty-msg");
    if (!CS.databases.length) {
      emptyMsg.textContent = "No databases registered";
      emptyEl.querySelector(".btn").textContent = "Register Database";
      emptyEl.querySelector(".btn").onclick = () => { if (typeof openDbModal === "function") openDbModal(); };
    } else {
      emptyMsg.textContent = "No metadata snapshot available";
      emptyEl.querySelector(".btn").textContent = "Run Metadata Snapshot";
      emptyEl.querySelector(".btn").onclick = refreshMetadata;
    }
    emptyEl.style.display = "";
    document.getElementById("canvas-snapshot-info").style.display = "none";
    document.getElementById("canvas-legend").style.display = "none";
    gRoot.selectAll("*").remove();
    return;
  }

  document.getElementById("canvas-empty").style.display = "none";
  document.getElementById("canvas-legend").style.display = "";

  const info = document.getElementById("canvas-snapshot-info");
  info.style.display = "";
  info.textContent = "Snapshot: " + new Date(CS.metadata.snapshot_at).toLocaleString();

  // Load drift data
  try {
    const driftRes = await fetch("/api/metadata/drift").then(r => r.ok ? r.json() : null);
    CS.drift = driftRes;
  } catch { CS.drift = null; }

  CS.layout = computeLayout();
  renderCanvas();
  requestAnimationFrame(() => canvasFit());
}

// ── Layout computation ─────────────────────────────────────────

function computeLayout() {
  const layout = { entryPoints: [], dbContainers: [], tableNodes: [], connections: [] };
  if (!CS.metadata) return layout;

  const dbKeys = Object.keys(CS.metadata.databases);
  const cfg = CS.pipelineConfig || { databases: {}, entry_points: [], pipelines: [] };
  const pipeConns = collectPipelineConnections(cfg);
  const epConns = collectEntryPointConnections(cfg);

  // Connected pairs for y-ordering
  const connectedPairs = pipeConns.map(c => ({
    fromDb: c.from.db, fromKey: `${c.from.schema}.${c.from.table}`,
    toDb: c.to.db, toKey: `${c.to.schema}.${c.to.table}`, label: c.label || "",
  }));

  // DB container x positions
  let dbX = 380;
  const dbPositions = {};
  for (const dbKey of dbKeys) {
    dbPositions[dbKey] = dbX;
    dbX += TABLE_W + DB_PAD_X * 2 + DB_GAP;
  }

  for (const dbKey of dbKeys) {
    const db = CS.metadata.databases[dbKey];
    if (!db || !db.tables) continue;

    const dbLabel = cfg.databases?.[dbKey]?.label || dbKey;
    const dbColor = cfg.databases?.[dbKey]?.color || "#00e5ff";
    const allTableKeys = Object.keys(db.tables);

    const connectedSet = new Set();
    const connectedOrder = [];
    for (const pair of connectedPairs) {
      if (pair.fromDb === dbKey && !connectedSet.has(pair.fromKey)) { connectedSet.add(pair.fromKey); connectedOrder.push(pair.fromKey); }
      if (pair.toDb === dbKey && !connectedSet.has(pair.toKey)) { connectedSet.add(pair.toKey); connectedOrder.push(pair.toKey); }
    }
    for (const epc of epConns) {
      const k = `${epc.target.schema}.${epc.target.table}`;
      if (epc.target.db === dbKey && !connectedSet.has(k)) { connectedSet.add(k); connectedOrder.push(k); }
    }

    const unconnected = allTableKeys.filter(k => !connectedSet.has(k));
    const orderedKeys = [...connectedOrder.filter(k => allTableKeys.includes(k)), ...unconnected];

    const containerX = dbPositions[dbKey];
    let y = DB_PAD_TOP;

    for (const tKey of orderedKeys) {
      const tData = db.tables[tKey];
      if (!tData) continue;
      const nodeKey = `${dbKey}:${tKey}`;
      const saved = CS.nodePositions[nodeKey];
      layout.tableNodes.push({
        key: nodeKey, dbKey, tKey,
        label: tData.table,
        rowCount: tData.estimated_row_count,
        x: saved ? saved.x : containerX + DB_PAD_X,
        y: saved ? saved.y : y,
        w: TABLE_W, h: TABLE_H,
        data: tData, dbColor,
      });
      y += TABLE_H + TABLE_PAD;
    }

    layout.dbContainers.push({
      key: dbKey, label: dbLabel, color: dbColor,
      x: containerX, y: 0,
      w: TABLE_W + DB_PAD_X * 2,
      h: Math.max(y + DB_PAD_BOTTOM, DB_PAD_TOP + TABLE_H + DB_PAD_BOTTOM),
    });
  }

  // Entry points
  if (cfg.entry_points) {
    let epY = 30;
    for (const ep of cfg.entry_points) {
      layout.entryPoints.push({
        key: ep.id, name: ep.name,
        description: ep.description || "",
        type: ep.type || "api",
        x: 50, y: epY, w: EP_W, h: EP_H,
      });
      epY += EP_H + 20;
    }
  }

  // Build node lookup
  const nodeMap = {};
  for (const n of layout.tableNodes) nodeMap[n.key] = n;

  for (const c of pipeConns) {
    const sk = `${c.from.db}:${c.from.schema}.${c.from.table}`;
    const tk = `${c.to.db}:${c.to.schema}.${c.to.table}`;
    if (nodeMap[sk] && nodeMap[tk]) {
      layout.connections.push({ key: `pipe:${sk}->${tk}`, type: "pipeline", source: nodeMap[sk], target: nodeMap[tk], label: c.label || "", jobName: c._job, description: c._desc });
    }
  }

  for (const epc of epConns) {
    const ep = layout.entryPoints.find(e => e.key === epc.epId);
    const tk = `${epc.target.db}:${epc.target.schema}.${epc.target.table}`;
    if (ep && nodeMap[tk]) {
      layout.connections.push({ key: `ep:${epc.epId}->${tk}`, type: "entry", source: ep, target: nodeMap[tk], label: epc.epName, epDesc: (cfg.entry_points || []).find(e => e.id === epc.epId)?.description || "" });
    }
  }

  for (const n of layout.tableNodes) {
    for (const fk of n.data.foreign_keys || []) {
      const rk = `${n.dbKey}:${fk.ref_table}`;
      if (nodeMap[rk]) {
        layout.connections.push({ key: `fk:${n.key}->${rk}:${fk.name}`, type: "fk", source: n, target: nodeMap[rk], label: fk.name });
      }
    }
  }

  return layout;
}

function collectPipelineConnections(cfg) {
  const out = [];
  for (const p of cfg.pipelines || []) {
    if (Array.isArray(p.connections)) {
      for (const c of p.connections) out.push({ ...c, _job: p.source_job || null, _desc: p.description || "" });
    }
  }
  return out;
}

function collectEntryPointConnections(cfg) {
  const out = [];
  for (const ep of cfg.entry_points || []) { for (const t of ep.targets || []) out.push({ epId: ep.id, epName: ep.name, target: t }); }
  return out;
}

// ── Rendering ──────────────────────────────────────────────────

function renderCanvas() {
  if (!CS.layout) return;
  gRoot.selectAll("*").remove();

  const connLayer = gRoot.append("g").attr("class", "layer-connections");
  const containerLayer = gRoot.append("g").attr("class", "layer-containers");
  const nodeLayer = gRoot.append("g").attr("class", "layer-nodes");
  const epLayer = gRoot.append("g").attr("class", "layer-entrypoints");

  renderDBContainers(containerLayer, CS.layout.dbContainers);
  renderTableNodes(nodeLayer, CS.layout.tableNodes);
  renderEntryPoints(epLayer, CS.layout.entryPoints);
  renderConnections(connLayer, CS.layout.connections);
}

function renderDBContainers(layer, containers) {
  const g = layer.selectAll("g.db-container")
    .data(containers, d => d.key).join("g")
    .attr("class", "db-container")
    .attr("transform", d => `translate(${d.x},${d.y})`);

  g.append("rect").attr("width", d => d.w).attr("height", d => d.h).attr("rx", 6)
    .attr("fill", "rgba(30,30,66,0.3)").attr("stroke", d => d.color)
    .attr("stroke-width", 2).attr("stroke-dasharray", "6,3").attr("opacity", 0.6);

  g.append("text").attr("x", d => d.w / 2).attr("y", 22)
    .attr("text-anchor", "middle").attr("font-family", "'Rajdhani', sans-serif")
    .attr("font-size", 14).attr("font-weight", 700).attr("fill", d => d.color)
    .attr("letter-spacing", 2).text(d => d.label.toUpperCase());
}

function renderTableNodes(layer, nodes) {
  const drag = d3.drag()
    .on("start", function () { d3.select(this).raise(); })
    .on("drag", function (event, d) {
      d.x = event.x; d.y = event.y;
      d3.select(this).attr("transform", `translate(${d.x},${d.y})`);
      updateConnections();
      updateContainerBounds();
    })
    .on("end", (_e, d) => { CS.nodePositions[d.key] = { x: d.x, y: d.y }; savePositions(); });

  const g = layer.selectAll("g.table-node")
    .data(nodes, d => d.key).join("g")
    .attr("class", "table-node")
    .attr("transform", d => `translate(${d.x},${d.y})`)
    .call(drag).style("cursor", "pointer")
    .on("click", (_e, d) => openDetailPanel(d))
    .on("mouseenter", (event, d) => showTooltip(event, d))
    .on("mouseleave", hideTooltip);

  g.append("rect").attr("width", TABLE_W).attr("height", TABLE_H).attr("rx", 3)
    .attr("fill", "#1e1e42").attr("stroke", d => d.dbColor).attr("stroke-width", 2).attr("opacity", 0.9);

  g.append("rect").attr("class", "node-glow").attr("width", TABLE_W).attr("height", TABLE_H)
    .attr("rx", 3).attr("fill", "none").attr("stroke", d => d.dbColor).attr("stroke-width", 1).attr("opacity", 0);

  g.on("mouseenter.glow", function () {
    d3.select(this).select(".node-glow").transition().duration(120).attr("opacity", 0.5).attr("stroke-width", 3);
  }).on("mouseleave.glow", function () {
    d3.select(this).select(".node-glow").transition().duration(200).attr("opacity", 0);
  });

  g.append("text").attr("x", 10).attr("y", TABLE_H / 2 + 1)
    .attr("dominant-baseline", "middle").attr("font-family", "'Fira Code', monospace")
    .attr("font-size", 11).attr("fill", "#e4e2f0")
    .text(d => d.label.length > 20 ? d.label.slice(0, 18) + ".." : d.label);

  g.append("rect").attr("x", TABLE_W - 68).attr("y", (TABLE_H - 16) / 2)
    .attr("width", 58).attr("height", 16).attr("rx", 2)
    .attr("fill", "rgba(200,255,0,0.08)").attr("stroke", "rgba(200,255,0,0.25)").attr("stroke-width", 1);

  g.append("text").attr("x", TABLE_W - 39).attr("y", TABLE_H / 2 + 1)
    .attr("dominant-baseline", "middle").attr("text-anchor", "middle")
    .attr("font-family", "'Fira Code', monospace").attr("font-size", 9).attr("fill", "#c8ff00")
    .text(d => formatCount(d.rowCount));

  // Drift badges
  if (CS.drift && CS.drift.drift && CS.drift.drift.length) {
    const driftByTable = {};
    for (const d of CS.drift.drift) {
      if (d.table) {
        const k = `${d.db}:${d.table}`;
        if (!driftByTable[k]) driftByTable[k] = { count: 0, breaking: false };
        driftByTable[k].count++;
        if (d.breaking) driftByTable[k].breaking = true;
      }
    }
    g.each(function (d) {
      const info = driftByTable[d.key];
      if (!info) return;
      const el = d3.select(this);
      const color = info.breaking ? "#ff3355" : "#ffd000";
      el.append("circle").attr("cx", TABLE_W - 4).attr("cy", 4).attr("r", 7)
        .attr("fill", color).attr("stroke", "#12122a").attr("stroke-width", 1.5);
      el.append("text").attr("x", TABLE_W - 4).attr("y", 5)
        .attr("text-anchor", "middle").attr("dominant-baseline", "middle")
        .attr("font-family", "'Fira Code', monospace").attr("font-size", 8).attr("font-weight", 700)
        .attr("fill", "#12122a").text(info.count > 9 ? "!" : info.count);
    });
  }
}

function renderEntryPoints(layer, entryPoints) {
  const g = layer.selectAll("g.entry-point")
    .data(entryPoints, d => d.key).join("g")
    .attr("class", "entry-point")
    .attr("transform", d => `translate(${d.x},${d.y})`)
    .on("mouseenter", (event, d) => {
      const tip = document.getElementById("canvas-tooltip");
      while (tip.firstChild) tip.removeChild(tip.firstChild);
      const b = document.createElement("strong"); b.textContent = d.name; tip.appendChild(b);
      tip.appendChild(document.createElement("br"));
      const sp = document.createElement("span"); sp.style.color = "#7b7898"; sp.textContent = d.description; tip.appendChild(sp);
      tip.style.left = (event.pageX + 12) + "px"; tip.style.top = (event.pageY - 8) + "px"; tip.style.display = "";
    })
    .on("mouseleave", hideTooltip);

  g.each(function (d) {
    const el = d3.select(this);
    const cx = EP_W / 2, cy = EP_H / 2;
    if (d.type === "api") {
      const r = EP_H / 2;
      const pts = Array.from({ length: 6 }, (_, i) => {
        const a = Math.PI / 3 * i - Math.PI / 6;
        return `${cx + r * Math.cos(a)},${cy + r * Math.sin(a)}`;
      }).join(" ");
      el.append("polygon").attr("points", pts)
        .attr("fill", "rgba(255,45,138,0.08)").attr("stroke", "#ff2d8a").attr("stroke-width", 2);
    } else if (d.type === "service") {
      el.append("polygon")
        .attr("points", `${cx},${cy - EP_H / 2} ${cx + EP_W / 3},${cy} ${cx},${cy + EP_H / 2} ${cx - EP_W / 3},${cy}`)
        .attr("fill", "rgba(255,45,138,0.08)").attr("stroke", "#ff2d8a").attr("stroke-width", 2);
    } else if (d.type === "file") {
      el.append("rect").attr("x", cx - EP_W / 3).attr("y", 0).attr("width", EP_W * 2 / 3).attr("height", EP_H)
        .attr("rx", 3).attr("fill", "rgba(255,45,138,0.08)").attr("stroke", "#ff2d8a").attr("stroke-width", 2);
    } else {
      el.append("circle").attr("cx", cx).attr("cy", cy).attr("r", EP_H / 2 - 2)
        .attr("fill", "rgba(255,45,138,0.08)").attr("stroke", "#ff2d8a").attr("stroke-width", 2);
    }
  });

  g.append("text").attr("x", EP_W / 2).attr("y", EP_H / 2 + 1)
    .attr("dominant-baseline", "middle").attr("text-anchor", "middle")
    .attr("font-family", "'Rajdhani', sans-serif").attr("font-size", 11).attr("font-weight", 700)
    .attr("fill", "#ff2d8a").attr("letter-spacing", 1)
    .text(d => d.name.length > 16 ? d.name.slice(0, 14) + ".." : d.name);

  g.append("text").attr("x", EP_W / 2).attr("y", EP_H + 14)
    .attr("text-anchor", "middle").attr("font-family", "'Fira Code', monospace")
    .attr("font-size", 8).attr("fill", "rgba(255,45,138,0.5)")
    .text(d => d.type.toUpperCase());
}

function renderConnections(layer, connections) {
  // 투명한 넓은 hit area (클릭 쉽게)
  layer.selectAll("path.conn-hit")
    .data(connections.filter(d => d.type !== "fk"), d => d.key).join("path")
    .attr("class", "conn-hit")
    .attr("d", d => arrowPath(d))
    .attr("fill", "none").attr("stroke", "transparent").attr("stroke-width", 14)
    .style("cursor", "pointer")
    .on("click", (_e, d) => openConnectionPanel(d))
    .on("mouseenter", function (_, d) {
      layer.selectAll(`path.connection`).attr("opacity", c => c.key === d.key ? 1 : (c.type === "fk" ? 0.15 : 0.3));
    })
    .on("mouseleave", function () {
      layer.selectAll("path.connection").attr("opacity", d => d.type === "fk" ? 0.4 : 0.85);
    });

  layer.selectAll("path.connection")
    .data(connections, d => d.key).join("path")
    .attr("class", d => `connection conn-${d.type}`)
    .attr("d", d => arrowPath(d))
    .attr("fill", "none")
    .attr("stroke", d => d.type === "pipeline" ? "#ffd000" : d.type === "entry" ? "#ff2d8a" : "rgba(0,229,255,0.3)")
    .attr("stroke-width", d => d.type === "fk" ? 1 : 2)
    .attr("stroke-dasharray", d => d.type === "entry" ? "6,4" : d.type === "fk" ? "3,3" : "none")
    .attr("marker-end", d => d.type === "pipeline" ? "url(#arrow-pipeline)" : d.type === "entry" ? "url(#arrow-entry)" : "url(#arrow-fk)")
    .attr("opacity", d => d.type === "fk" ? 0.4 : 0.85)
    .style("pointer-events", "none");

  // Pipeline labels
  layer.selectAll("text.conn-label")
    .data(connections.filter(d => d.type === "pipeline" && d.label), d => d.key).join("text")
    .attr("class", "conn-label")
    .attr("font-family", "'Fira Code', monospace").attr("font-size", 9)
    .attr("fill", "rgba(255,208,0,0.6)").attr("text-anchor", "middle")
    .each(function (d) {
      const sx = d.source.x + d.source.w, sy = d.source.y + d.source.h / 2;
      const tx = d.target.x, ty = d.target.y + d.target.h / 2;
      d3.select(this).attr("x", (sx + tx) / 2).attr("y", (sy + ty) / 2 - 6);
    })
    .text(d => d.label);
}

function arrowPath(conn) {
  const s = conn.source, t = conn.target;
  const sx = s.x + s.w, sy = s.y + s.h / 2;
  const tx = t.x, ty = t.y + t.h / 2;
  if (tx <= sx) {
    const mx = Math.max(sx, tx) + 60;
    return `M${sx},${sy} C${mx},${sy} ${mx},${ty} ${tx},${ty}`;
  }
  const mx = (sx + tx) / 2;
  return `M${sx},${sy} C${mx},${sy} ${mx},${ty} ${tx},${ty}`;
}

function updateConnections() {
  if (!CS.layout) return;
  gRoot.selectAll("path.connection").data(CS.layout.connections, d => d.key).attr("d", d => arrowPath(d));
  gRoot.selectAll("path.conn-hit").data(CS.layout.connections.filter(d => d.type !== "fk"), d => d.key).attr("d", d => arrowPath(d));
  gRoot.selectAll("text.conn-label")
    .data(CS.layout.connections.filter(d => d.type === "pipeline" && d.label), d => d.key)
    .each(function (d) {
      const sx = d.source.x + d.source.w, sy = d.source.y + d.source.h / 2;
      const tx = d.target.x, ty = d.target.y + d.target.h / 2;
      d3.select(this).attr("x", (sx + tx) / 2).attr("y", (sy + ty) / 2 - 6);
    });
}

function updateContainerBounds() {
  if (!CS.layout) return;
  for (const container of CS.layout.dbContainers) {
    const children = CS.layout.tableNodes.filter(n => n.dbKey === container.key);
    if (!children.length) continue;
    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    for (const c of children) {
      minX = Math.min(minX, c.x);
      minY = Math.min(minY, c.y);
      maxX = Math.max(maxX, c.x + c.w);
      maxY = Math.max(maxY, c.y + c.h);
    }
    container.x = minX - DB_PAD_X;
    container.y = minY - DB_PAD_TOP;
    container.w = maxX - minX + DB_PAD_X * 2;
    container.h = maxY - minY + DB_PAD_TOP + DB_PAD_BOTTOM;
  }
  gRoot.selectAll("g.db-container")
    .data(CS.layout.dbContainers, d => d.key)
    .attr("transform", d => `translate(${d.x},${d.y})`)
    .select("rect").attr("width", d => d.w).attr("height", d => d.h);
  gRoot.selectAll("g.db-container")
    .data(CS.layout.dbContainers, d => d.key)
    .select("text").attr("x", d => d.w / 2);
}

// ── Tooltip ────────────────────────────────────────────────────

function showTooltip(event, d) {
  const tip = document.getElementById("canvas-tooltip");
  while (tip.firstChild) tip.removeChild(tip.firstChild);
  const b = document.createElement("strong");
  b.textContent = d.data.schema + "." + d.data.table;
  tip.appendChild(b);
  tip.appendChild(document.createElement("br"));
  const rows = document.createElement("span"); rows.className = "tt-rows";
  rows.textContent = "~" + d.rowCount.toLocaleString() + " rows";
  tip.appendChild(rows);
  tip.appendChild(document.createElement("br"));
  const pk = document.createElement("span"); pk.className = "tt-pk";
  pk.textContent = "PK: " + (d.data.primary_key ? d.data.primary_key.columns.join(", ") : "-");
  tip.appendChild(pk);
  tip.style.left = (event.pageX + 14) + "px";
  tip.style.top = (event.pageY - 10) + "px";
  tip.style.display = "";
}

function hideTooltip() {
  document.getElementById("canvas-tooltip").style.display = "none";
}

// ── Detail Panel ───────────────────────────────────────────────

function openDetailPanel(node) {
  const panel = document.getElementById("detail-panel");
  const dbTag = document.getElementById("detail-db-tag");
  const tblName = document.getElementById("detail-table-name");
  const rowCount = document.getElementById("detail-row-count");
  const content = document.getElementById("detail-content");
  CS.selectedTable = node.key;

  dbTag.textContent = node.dbKey;
  dbTag.style.borderColor = node.dbColor;
  dbTag.style.color = node.dbColor;
  dbTag.style.background = node.dbColor + "15";
  tblName.textContent = node.data.schema + "." + node.data.table;
  rowCount.textContent = "~" + node.rowCount.toLocaleString() + " rows";

  while (content.firstChild) content.removeChild(content.firstChild);

  // Columns
  const cols = node.data.columns || [];
  const pkCols = new Set((node.data.primary_key?.columns) || []);
  const fkColSet = new Set();
  for (const fk of node.data.foreign_keys || []) for (const c of fk.columns) fkColSet.add(c);

  if (cols.length) {
    const sec = _makeSection("Columns");
    const tbl = _el("table", { className: "detail-tbl" });
    const thead = _el("thead");
    const hr = _el("tr", null, [_el("th", null, "Name"), _el("th", null, "Type"), _el("th", null, "Null"), _el("th")]);
    thead.appendChild(hr);
    tbl.appendChild(thead);
    const tbody = _el("tbody");
    for (const c of cols) {
      const nameTd = _el("td");
      nameTd.appendChild(document.createTextNode(c.name));
      if (pkCols.has(c.name)) { const badge = _el("span", { className: "pk-badge" }, "PK"); nameTd.appendChild(badge); }
      if (fkColSet.has(c.name)) { const badge = _el("span", { className: "fk-badge" }, "FK"); nameTd.appendChild(badge); }
      const tr = _el("tr", null, [
        nameTd,
        _el("td", { style: { color: "#7b7898" } }, c.type),
        _el("td", { style: { color: "#7b7898" } }, c.nullable ? "Y" : ""),
        _el("td"),
      ]);
      tbody.appendChild(tr);
    }
    tbl.appendChild(tbody);
    sec.appendChild(tbl);
    content.appendChild(sec);
  }

  // Indexes
  const idxs = node.data.indexes || [];
  if (idxs.length) {
    const sec = _makeSection("Indexes");
    const tbl = _el("table", { className: "detail-tbl" });
    const thead = _el("thead");
    thead.appendChild(_el("tr", null, [_el("th", null, "Name"), _el("th", null, "Columns"), _el("th", null, "Unique")]));
    tbl.appendChild(thead);
    const tbody = _el("tbody");
    for (const idx of idxs) {
      tbody.appendChild(_el("tr", null, [
        _el("td", null, idx.name),
        _el("td", { style: { color: "#7b7898" } }, idx.columns.join(", ")),
        _el("td", null, idx.unique ? [_el("span", { style: { color: "#ffd000" } }, "Y")] : ""),
      ]));
    }
    tbl.appendChild(tbody);
    sec.appendChild(tbl);
    content.appendChild(sec);
  }

  // Foreign Keys
  const fks = node.data.foreign_keys || [];
  if (fks.length) {
    const sec = _makeSection("Foreign Keys");
    const tbl = _el("table", { className: "detail-tbl" });
    const thead = _el("thead");
    thead.appendChild(_el("tr", null, [_el("th", null, "Constraint"), _el("th", null, "Column(s)"), _el("th", null, "References")]));
    tbl.appendChild(thead);
    const tbody = _el("tbody");
    for (const fk of fks) {
      tbody.appendChild(_el("tr", null, [
        _el("td", null, fk.name),
        _el("td", { style: { color: "#ff2d8a" } }, fk.columns.join(", ")),
        _el("td", { style: { color: "#7b7898" } }, fk.ref_table + "(" + fk.ref_columns.join(", ") + ")"),
      ]));
    }
    tbl.appendChild(tbody);
    sec.appendChild(tbl);
    content.appendChild(sec);
  }

  // Referenced By
  const refs = node.data.referenced_by || [];
  if (refs.length) {
    const sec = _makeSection("Referenced By");
    const tbl = _el("table", { className: "detail-tbl" });
    const thead = _el("thead");
    thead.appendChild(_el("tr", null, [_el("th", null, "From Table"), _el("th", null, "Column(s)"), _el("th", null, "Constraint")]));
    tbl.appendChild(thead);
    const tbody = _el("tbody");
    for (const ref of refs) {
      tbody.appendChild(_el("tr", null, [
        _el("td", { style: { color: "#ff2d8a" } }, ref.table),
        _el("td", { style: { color: "#7b7898" } }, ref.columns.join(", ")),
        _el("td", null, ref.name),
      ]));
    }
    tbl.appendChild(tbody);
    sec.appendChild(tbl);
    content.appendChild(sec);
  }

  // Pipelines
  const relConns = (CS.layout?.connections || []).filter(c =>
    c.type !== "fk" && (c.source.key === node.key || c.target.key === node.key));
  if (relConns.length) {
    const sec = _makeSection("Pipelines");
    const wrap = _el("div", { style: { fontSize: "11px" } });
    for (const c of relConns) {
      const div = _el("div", { style: { padding: "3px 0", borderBottom: "1px solid rgba(255,255,255,.03)" } });
      const dir = c.source.key === node.key ? "out" : "in";
      const other = dir === "out" ? c.target : c.source;
      const typeLabel = c.type === "pipeline" ? "SYNC" : "ENTRY";
      const typeColor = c.type === "pipeline" ? "#ffd000" : "#ff2d8a";
      const arrow = dir === "out" ? " \u2192 " : " \u2190 ";
      const tag = _el("span", { style: { color: typeColor } }, typeLabel);
      const name = _el("span", { style: { color: "#e4e2f0" } }, other.label || other.name || other.key);
      div.appendChild(tag);
      div.appendChild(document.createTextNode(arrow));
      div.appendChild(name);
      wrap.appendChild(div);
    }
    sec.appendChild(wrap);
    content.appendChild(sec);
  }

  // Drift section
  if (CS.drift && CS.drift.drift) {
    const tableDrift = CS.drift.drift.filter(d => d.db === node.dbKey && d.table === node.tKey);
    if (tableDrift.length) {
      const sec = _makeSection("Schema Changes");
      const wrap = _el("div", { style: { fontSize: "11px" } });
      for (const d of tableDrift) {
        const div = _el("div", { style: { padding: "3px 0", borderBottom: "1px solid rgba(255,255,255,.03)" } });
        const color = d.breaking ? "#ff3355" : "#ffd000";
        const tag = _el("span", { style: { color, fontWeight: "700", fontSize: "9px", marginRight: "6px" } }, d.breaking ? "BREAKING" : "CHANGE");
        const desc = _el("span", { style: { color: "#e4e2f0" } });
        if (d.type === "column_added") desc.textContent = "Column added: " + d.column;
        else if (d.type === "column_removed") desc.textContent = "Column removed: " + d.column;
        else if (d.type === "type_changed") desc.textContent = d.column + ": " + d.from + " \u2192 " + d.to;
        else if (d.type === "nullable_changed") desc.textContent = d.column + " nullable: " + d.from + " \u2192 " + d.to;
        else if (d.type === "pk_changed") desc.textContent = "Primary key changed";
        else desc.textContent = d.type;
        div.appendChild(tag);
        div.appendChild(desc);
        wrap.appendChild(div);
      }
      sec.appendChild(wrap);
      content.appendChild(sec);
    }
  }

  // Row count trend (async load)
  const trendSec = _makeSection("Row Count Trend");
  const trendWrap = _el("div", { style: { fontSize: "11px", color: "#7b7898" } }, "Loading...");
  trendSec.appendChild(trendWrap);
  content.appendChild(trendSec);
  fetch(`/api/freshness?db=${encodeURIComponent(node.dbKey)}&table=${encodeURIComponent(node.tKey)}`)
    .then(r => r.json())
    .then(data => {
      while (trendWrap.firstChild) trendWrap.removeChild(trendWrap.firstChild);
      if (!data.length) { trendWrap.textContent = "No history yet"; return; }
      for (const d of data.slice(-10)) {
        const row = _el("div", { style: { display: "flex", justifyContent: "space-between", padding: "2px 0", borderBottom: "1px solid rgba(255,255,255,.03)" } });
        row.appendChild(_el("span", { style: { color: "#7b7898", fontSize: "10px" } }, new Date(d.ts).toLocaleString()));
        row.appendChild(_el("span", { style: { color: "#c8ff00" } }, formatCount(d.rows)));
        trendWrap.appendChild(row);
      }
    })
    .catch(() => { trendWrap.textContent = "Failed to load"; });

  panel.classList.add("open");

  // Path highlighting: find connected nodes
  const connectedKeys = new Set([node.key]);
  const connectedEdges = new Set();
  for (const c of CS.layout?.connections || []) {
    if (c.source.key === node.key || c.target.key === node.key) {
      connectedKeys.add(c.source.key);
      connectedKeys.add(c.target.key);
      connectedEdges.add(c.key);
    }
  }

  gRoot.selectAll("g.table-node")
    .attr("opacity", d => connectedKeys.has(d.key) ? 1 : 0.2)
    .select("rect:first-child")
    .attr("stroke-width", d => d.key === node.key ? 3 : connectedKeys.has(d.key) ? 2.5 : 2);
  gRoot.selectAll("g.entry-point")
    .attr("opacity", d => connectedKeys.has(d.key) ? 1 : 0.2);
  gRoot.selectAll("path.connection")
    .attr("opacity", d => connectedEdges.has(d.key) ? 1 : 0.08);
}

function closeDetailPanel() {
  document.getElementById("detail-panel").classList.remove("open");
  CS.selectedTable = null;
  // Restore full opacity
  gRoot.selectAll("g.table-node").attr("opacity", 1)
    .select("rect:first-child").attr("stroke-width", 2);
  gRoot.selectAll("g.entry-point").attr("opacity", 1);
  gRoot.selectAll("path.connection")
    .attr("opacity", d => d.type === "fk" ? 0.4 : 0.85);
}

// ── Connection (arrow) detail panel ────────────────────────────

async function openConnectionPanel(conn) {
  const panel = document.getElementById("detail-panel");
  const dbTag = document.getElementById("detail-db-tag");
  const tblName = document.getElementById("detail-table-name");
  const rowCount = document.getElementById("detail-row-count");
  const content = document.getElementById("detail-content");

  const isPipeline = conn.type === "pipeline";
  const color = isPipeline ? "#ffd000" : "#ff2d8a";

  dbTag.textContent = isPipeline ? "PIPELINE" : "ENTRY";
  dbTag.style.borderColor = color;
  dbTag.style.color = color;
  dbTag.style.background = color + "15";
  tblName.textContent = conn.label || conn.key;
  rowCount.textContent = "";

  while (content.firstChild) content.removeChild(content.firstChild);

  // Connection info
  const infoSec = _makeSection("Connection");
  const infoWrap = _el("div", { style: { fontSize: "12px", lineHeight: "1.8" } });
  const src = conn.source;
  const tgt = conn.target;
  infoWrap.appendChild(_infoRow("From", (src.label || src.name || src.key), src.dbKey ? src.dbColor : "#ff2d8a"));
  infoWrap.appendChild(_infoRow("To", (tgt.label || tgt.name || tgt.key), tgt.dbColor || "#00e5ff"));
  if (conn.description) infoWrap.appendChild(_infoRow("Description", conn.description));
  infoSec.appendChild(infoWrap);
  content.appendChild(infoSec);

  // Job info (pipeline only)
  if (isPipeline && conn.jobName) {
    const jobSec = _makeSection("Cron Job");
    const jobWrap = _el("div", { style: { fontSize: "12px", lineHeight: "1.8" } });
    jobWrap.appendChild(_infoRow("Job", conn.jobName, "#c8ff00"));

    // Fetch schedules for this job
    try {
      const schedules = await fetch("/api/schedules").then(r => r.json());
      const related = schedules.filter(s => s.jobName === conn.jobName);
      if (related.length) {
        for (const s of related) {
          const cronEl = _el("code", { style: { background: "#12122a", color: "#c8ff00", padding: "1px 6px", borderRadius: "3px", border: "1px solid rgba(200,255,0,.2)", fontSize: "11px" } }, s.cron);
          const row = _el("div", { style: { display: "flex", alignItems: "center", gap: "8px", padding: "2px 0" } });
          row.appendChild(_el("span", { style: { color: "#7b7898" } }, "Schedule"));
          row.appendChild(cronEl);
          jobWrap.appendChild(row);
        }
      } else {
        jobWrap.appendChild(_infoRow("Schedule", "No active schedule", "#7b7898"));
      }
    } catch {}

    // Run button
    const runBtn = _el("button", { className: "btn btn-lime btn-sm", style: { marginTop: "10px" } }, "RUN " + conn.jobName.toUpperCase());
    runBtn.addEventListener("click", async () => {
      runBtn.disabled = true;
      try {
        await fetch("/api/jobs/" + encodeURIComponent(conn.jobName) + "/run", { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });
        if (typeof toast === "function") toast(conn.jobName + " started", true);
      } catch { if (typeof toast === "function") toast("Failed to start", false); }
      runBtn.disabled = false;
    });
    jobWrap.appendChild(runBtn);
    jobSec.appendChild(jobWrap);
    content.appendChild(jobSec);
  }

  // Entry point description
  if (conn.type === "entry" && conn.epDesc) {
    const descSec = _makeSection("Entry Point");
    descSec.appendChild(_el("div", { style: { fontSize: "12px", color: "#e4e2f0" } }, conn.epDesc));
    content.appendChild(descSec);
  }

  panel.classList.add("open");
}

function _infoRow(label, value, valueColor) {
  const row = _el("div", { style: { display: "flex", gap: "8px", padding: "2px 0" } });
  row.appendChild(_el("span", { style: { color: "#7b7898", minWidth: "80px" } }, label));
  row.appendChild(_el("span", { style: { color: valueColor || "#e4e2f0" } }, value));
  return row;
}

function _makeSection(title) {
  const sec = _el("div", { className: "detail-section" });
  sec.appendChild(_el("h4", null, title));
  return sec;
}

// ── Zoom controls ──────────────────────────────────────────────

function canvasZoomIn() { svgEl.transition().duration(300).call(zoomBehavior.scaleBy, 1.4); }
function canvasZoomOut() { svgEl.transition().duration(300).call(zoomBehavior.scaleBy, 0.7); }

function canvasFit() {
  if (!CS.layout) return;
  const nodes = [...CS.layout.tableNodes, ...CS.layout.entryPoints, ...CS.layout.dbContainers];
  if (!nodes.length) return;

  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  for (const n of nodes) {
    minX = Math.min(minX, n.x); minY = Math.min(minY, n.y);
    maxX = Math.max(maxX, n.x + (n.w || 0)); maxY = Math.max(maxY, n.y + (n.h || 0));
  }

  const cw = +svgEl.attr("width"), ch = +svgEl.attr("height");
  const pad = 60;
  const bw = maxX - minX + pad * 2, bh = maxY - minY + pad * 2;
  const scale = Math.min(cw / bw, ch / bh, 1.5);
  const tx = (cw - bw * scale) / 2 - minX * scale + pad * scale;
  const ty = (ch - bh * scale) / 2 - minY * scale + pad * scale;

  svgEl.transition().duration(500).call(zoomBehavior.transform, d3.zoomIdentity.translate(tx, ty).scale(scale));
}

// ── Refresh metadata ───────────────────────────────────────────

async function refreshMetadata() {
  const btn = document.querySelector("#canvas-toolbar .btn-lime");
  if (btn) { btn.disabled = true; btn.textContent = "..."; }
  try {
    const res = await fetch("/api/jobs/metadata_snapshot/run", {
      method: "POST", headers: { "Content-Type": "application/json" }, body: "{}",
    });
    if (!res.ok) {
      if (typeof toast === "function") toast("Failed to start metadata snapshot", false);
      return;
    }
    const { runId } = await res.json();
    if (typeof toast === "function") toast("Metadata snapshot started", true);
    const poll = setInterval(async () => {
      const r = await fetch("/api/running/" + runId);
      if (!r.ok) {
        clearInterval(poll);
        await loadCanvasData();
        if (btn) { btn.disabled = false; btn.textContent = "REFRESH"; }
      }
    }, 2000);
  } catch {
    if (typeof toast === "function") toast("Failed to refresh", false);
    if (btn) { btn.disabled = false; btn.textContent = "REFRESH"; }
  }
}

// ── Position persistence ───────────────────────────────────────

// ── Layer toggles ──────────────────────────────────────────────

function toggleLayer(type, visible) {
  gRoot.selectAll(`path.conn-${type}`).style("display", visible ? null : "none");
  gRoot.selectAll("path.conn-hit").filter(d => d.type === type).style("display", visible ? null : "none");
  gRoot.selectAll("text.conn-label").filter(d => d.type === type).style("display", visible ? null : "none");
  if (type === "entry") {
    gRoot.selectAll("g.entry-point").style("display", visible ? null : "none");
  }
}

// ── Search ─────────────────────────────────────────────────────

function canvasSearch(query) {
  const q = (query || "").toLowerCase().trim();
  const countEl = document.getElementById("canvas-search-count");

  if (!q) {
    gRoot.selectAll("g.table-node").attr("opacity", 1);
    gRoot.selectAll("g.db-container").attr("opacity", 1);
    gRoot.selectAll("g.entry-point").attr("opacity", 1);
    gRoot.selectAll("path.connection").attr("opacity", d => d.type === "fk" ? 0.4 : 0.85);
    if (countEl) countEl.textContent = "";
    return;
  }

  let matchCount = 0;
  gRoot.selectAll("g.table-node").attr("opacity", function (d) {
    const match = d.label.toLowerCase().includes(q) || d.tKey.toLowerCase().includes(q) || d.dbKey.toLowerCase().includes(q);
    if (match) matchCount++;
    return match ? 1 : 0.15;
  });
  gRoot.selectAll("g.entry-point").attr("opacity", d =>
    d.name.toLowerCase().includes(q) ? 1 : 0.15);
  gRoot.selectAll("path.connection").attr("opacity", 0.1);
  if (countEl) countEl.textContent = matchCount ? matchCount + " found" : "no results";
}

// ── Position persistence ───────────────────────────────────────

function savePositions() {
  try { localStorage.setItem("dbicron-canvas-positions", JSON.stringify(CS.nodePositions)); } catch {}
}
function loadSavedPositions() {
  try { const r = localStorage.getItem("dbicron-canvas-positions"); if (r) CS.nodePositions = JSON.parse(r); } catch {}
}
