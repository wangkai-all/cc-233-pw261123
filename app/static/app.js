"use strict";

// ---------------------------------------------------------------------------
// state
// ---------------------------------------------------------------------------
const state = {
  sources: [],
  sourceId: "orders",
  status: null,
  plan: null,
  tab: "collect",
  selectedKeys: new Set(),
  traceKey: null,
  activeRunId: null,
  pollTimer: null,
  lastRecords: [],
};

const $ = (sel) => document.querySelector(sel);
const el = (tag, cls, text) => {
  const n = document.createElement(tag);
  if (cls) n.className = cls;
  if (text !== undefined && text !== null) n.textContent = text;
  return n;
};

function toast(msg, kind) {
  const box = $("#toast");
  const n = el("div", kind || "", msg);
  box.appendChild(n);
  setTimeout(() => n.remove(), kind === "err" ? 7000 : 4200);
}

async function api(path, opts) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...opts,
    body: opts && opts.body ? JSON.stringify(opts.body) : undefined,
  });
  let data = null;
  try { data = await res.json(); } catch (e) { /* no body */ }
  if (!res.ok) {
    const msg = (data && data.error) || res.statusText;
    throw new Error(msg);
  }
  return data;
}

const fmtTime = (t) => {
  if (!t) return "—";
  const d = new Date(t * 1000);
  const p = (n, w = 2) => String(n).padStart(w, "0");
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${p(d.getMilliseconds(), 3)}`;
};
const shortHash = (h) => (h ? String(h).slice(0, 10) : "—");
const safeJson = (v) => {
  try { return JSON.stringify(v, null, 1); } catch (e) { return String(v); }
};
const escapeHtml = (s) => String(s ?? "").replace(/[&<>"']/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

function badge(text, kind) {
  const b = el("span", "badge " + (kind || ""), text);
  return b;
}

// ---------------------------------------------------------------------------
// loading
// ---------------------------------------------------------------------------
async function loadSources() {
  state.sources = await api("/api/sources");
  if (!state.sources.find((s) => s.source_id === state.sourceId) && state.sources.length) {
    state.sourceId = state.sources[0].source_id;
  }
  renderSources();
}

function renderSources() {
  const box = $("#source-list");
  box.innerHTML = "";
  state.sources.forEach((s) => {
    const card = el("div", "source-card" + (s.source_id === state.sourceId ? " active" : ""));
    card.appendChild(el("div", "name", s.name));
    const meta = el("div", "meta");
    meta.textContent =
      `${s.source_id} · 推送游标 ${s.push_cursor}/${s.push_head} · ` +
      `覆盖率至写序号 ${s.coverage_epoch ?? "—"} · 页 ${s.page_size}` +
      (s.pull_enabled ? "" : " · 拉取停用");
    card.appendChild(meta);
    const badges = el("div", "row mt8");
    badges.appendChild(badge(s.push_enabled ? "推送启用" : "推送停用",
      s.push_enabled ? "info" : "warn"));
    badges.appendChild(badge(s.pull_enabled ? "拉取启用" : "拉取停用",
      s.pull_enabled ? "info" : "warn"));
    if (s.push_cursor <= s.push_head) badges.appendChild(badge(
      `待接收 ${s.push_head - s.push_cursor + 1}`, "warn"));
    card.appendChild(badges);
    card.addEventListener("click", () => {
      state.sourceId = s.source_id;
      state.traceKey = null;
      state.selectedKeys.clear();
      renderSources();
      refreshAll();
    });
    box.appendChild(card);
  });
}

async function loadStatus() {
  state.status = await api(`/api/sources/${state.sourceId}/status`);
  renderStatus();
}

async function loadPlan() {
  state.plan = await api(`/api/sources/${state.sourceId}/plan`);
  renderPlan();
}

async function refreshAll() {
  await Promise.all([loadSources(), loadStatus(), loadPlan()]);
  if (state.tab === "records") renderRecords();
  if (state.tab === "evidence") await loadEvidence();
  syncConfig();
}

// ---------------------------------------------------------------------------
// collect view
// ---------------------------------------------------------------------------
function renderStatus() {
  const st = state.status;
  if (!st) return;
  $("#source-title").textContent = st.source.name;

  const tb = $("#transport-badges");
  tb.innerHTML = "";
  tb.appendChild(badge(`推送 ${st.transport.push_cursor}/${st.transport.push_head}`,
    st.transport.pending_push > 0 ? "warn" : "ok"));
  tb.appendChild(badge(`拉取延迟 ${st.source.pull_delay_ms}ms`,
    st.source.pull_delay_ms > 0 ? "warn" : "info"));
  if (st.transport.outage_events_remaining > 0)
    tb.appendChild(badge(`待丢失 ${st.transport.outage_events_remaining}`, "bad"));
  if (st.conflicts.length) tb.appendChild(badge(`冲突 ${st.conflicts.length}`, "bad"));

  const grid = $("#stat-grid");
  grid.innerHTML = "";
  const stats = [
    ["本地记录", st.records.filter((r) => !r.deleted).length],
    ["已删除", st.records.filter((r) => r.deleted).length],
    ["未证实记录", st.records.filter((r) => !r.confirmed && !r.deleted).length],
    ["覆盖写序号", st.conclusion.coverage_seq || "—"],
    ["写入头部", st.conclusion.head_seq ?? "—"],
    ["未排除缺口", st.gaps.filter((g) => !g.resolved_at).length],
    ["疑点", st.suspects.length],
    ["请求总数", st.costs.total],
  ];
  stats.forEach(([k, v]) => {
    const s = el("div", "stat");
    s.appendChild(el("div", "k", k));
    s.appendChild(el("div", "v", String(v)));
    grid.appendChild(s);
  });

  const con = st.conclusion;
  const box = $("#conclusion");
  box.className = "conclusion mt8 " + con.status;
  box.innerHTML = "";
  const label = { proven: "已证明", open_items: "证据成立但有未排除项", not_proven: "尚未证明" }[con.status] || con.status;
  box.appendChild(el("div", "headline", label));
  const pmain = el("div", "hint mt8");
  pmain.textContent = con.statement;
  box.appendChild(pmain);
  const tail = el("div", "hint mt8");
  tail.textContent = con.tail_note;
  box.appendChild(tail);
  const cb = $("#conclusion-badge");
  cb.className = "badge " + ({ proven: "ok", open_items: "warn", not_proven: "bad" }[con.status] || "");
  cb.textContent = label;

  const ev = $("#evidence");
  ev.innerHTML = "";
  (st.evidence || []).forEach((line) => ev.appendChild(el("li", "", line)));

  renderGaps();
  renderSuspects();
  renderConflicts();
}

function renderGaps() {
  const box = $("#gaps");
  box.innerHTML = "";
  const st = state.status;
  if (!st.gaps.length) {
    box.appendChild(el("div", "empty", "没有缺口记录。"));
    return;
  }
  st.gaps.forEach((g) => {
    const item = el("div", "gap-item" + (g.resolved_at ? " resolved" : ""));
    const head = el("div", "row between");
    head.appendChild(el("b", "", `写序号 ${g.seq_from ?? "?"}..${g.seq_to ?? "?"}`));
    head.appendChild(badge(g.resolved_at ? "已排除" : "未排除", g.resolved_at ? "ok" : "warn"));
    item.appendChild(head);
    item.appendChild(el("div", "hint",
      `${g.reason} · 检测于 ${fmtTime(g.detected_at)}` +
      (g.resolution ? ` · 处理：${g.resolution}` : "")));
    if (g.manifest_keys && g.manifest_keys.length) {
      const keys = g.manifest_keys.map((k) => `${k.biz_key}@r${k.revision}`).join(", ");
      item.appendChild(el("div", "hint mono mt8", "恢复清单： " + keys));
    }
    box.appendChild(item);
  });
}

function renderSuspects() {
  const box = $("#suspects");
  box.innerHTML = "";
  const st = state.status;
  if (!st.suspects.length) {
    box.appendChild(el("div", "empty", "疑点队列为空。"));
    return;
  }
  st.suspects.forEach((s) => {
    const row = el("div", "row between");
    row.appendChild(el("span", "mono", s.biz_key));
    row.appendChild(badge(s.reason, "warn"));
    box.appendChild(row);
  });
}

function renderConflicts() {
  const box = $("#conflicts");
  box.innerHTML = "";
  const st = state.status;
  if (!st.conflicts.length) {
    box.appendChild(el("div", "empty", "没有需要裁决的同修订冲突。"));
    return;
  }
  st.conflicts.forEach((c) => {
    const item = el("div", "conflict-item");
    const head = el("div", "row between");
    head.appendChild(el("b", "", `${c.biz_key} @ 修订 ${c.revision}`));
    head.appendChild(badge(`经 ${c.first_via} 到达`, "purple"));
    item.appendChild(head);
    item.appendChild(el("div", "hint",
      `本地 ${shortHash(c.local_hash)} vs 远端 ${shortHash(c.remote_hash)} · ` +
      `重复投递 ${c.deliveries} 次 · 检测于 ${fmtTime(c.detected_at)}`));
    const pay = el("div", "payload mt8");
    const l = el("div", "");
    l.appendChild(el("div", "hint", "本地已有"));
    l.appendChild(el("pre", "", safeJson(JSON.parse(c.local_payload || "{}"))));
    const r = el("div", "");
    r.appendChild(el("div", "hint", "冲突版本"));
    r.appendChild(el("pre", "", safeJson(JSON.parse(c.remote_payload || "{}"))));
    pay.appendChild(l); pay.appendChild(r);
    item.appendChild(pay);
    const actions = el("div", "row mt8");
    const keep = el("button", "ghost", "保留本地");
    const take = el("button", "primary", "采纳冲突版本");
    const resolve = async (decision) => {
      try {
        await api("/api/conflict/resolve", {
          method: "POST",
          body: { source_id: state.sourceId, biz_key: c.biz_key, conflict_id: c.id, decision },
        });
        toast(decision === "keep_local" ? "已裁决：保留本地" : "已裁决：采纳冲突版本", "ok");
        await refreshAll();
      } catch (e) { toast(e.message, "err"); }
    };
    keep.addEventListener("click", () => resolve("keep_local"));
    take.addEventListener("click", () => resolve("accept_remote"));
    actions.appendChild(keep); actions.appendChild(take);
    item.appendChild(actions);
    box.appendChild(item);
  });
}

function renderPlan() {
  const p = state.plan;
  const box = $("#plan");
  if (!p) return;
  box.innerHTML = "";
  p.options.forEach((opt) => {
    const card = el("div", "panel");
    card.style.background = "#10161d";
    const head = el("div", "row between");
    head.appendChild(el("b", "", opt.label));
    head.appendChild(badge(
      `${opt.strategy === "full" ? "至少 " : ""}${opt.requests} ` +
      `${opt.unit === "page" ? "页请求" : "点查请求"}`,
      opt.strategy === "full" ? "info" : "purple"));
    card.appendChild(head);
    const dl = el("dl", "kv mt8");
    const rows = [
      ["可证明", opt.proves],
      ["不能证明", opt.cannot_prove],
    ];
    rows.forEach(([k, v]) => {
      dl.appendChild(el("dt", "", k));
      const dd = el("dd", "");
      dd.style.fontFamily = "inherit";
      dd.textContent = v;
      dl.appendChild(dd);
    });
    card.appendChild(dl);
    if (opt.strategy === "full") {
      card.appendChild(el("div", "hint mt8",
        `每页 ${opt.page_size} 行；成本按已知 ${opt.keys_examined_at_least} 个业务键估算，` +
        "未知键会让实际页数更多，因此这是下限而非承诺"));
    }
    if (opt.keys && opt.keys.length) {
      card.appendChild(el("div", "hint mono mt8", "键：" + opt.keys.slice(0, 12).join(", ") +
        (opt.keys.length > 12 ? ` …(+${opt.keys.length - 12})` : "")));
    }
    box.appendChild(card);
  });
  const rec = el("div", "panel");
  rec.style.background = "#10161d";
  rec.appendChild(el("div", "row", "建议：" + (p.recommendation.strategy === "full" ? "全量核对" : "定向补采")));
  rec.appendChild(el("div", "hint mt8", p.recommendation.reason));
  if (p.remaining_unproven && p.remaining_unproven.length) {
    rec.appendChild(el("div", "section-title", "仍未排除"));
    p.remaining_unproven.forEach((r) => rec.appendChild(el("div", "hint", "· " + r)));
  }
  box.appendChild(rec);
}

// ---------------------------------------------------------------------------
// records view
// ---------------------------------------------------------------------------
function renderRecords() {
  const st = state.status;
  if (!st) return;
  $("#records-source-label").textContent = st.source.name;
  const filter = ($("#record-filter").value || "").trim().toLowerCase();
  const body = $("#records-body");
  body.innerHTML = "";
  state.lastRecords = st.records.filter((r) =>
    !filter || r.biz_key.toLowerCase().includes(filter));
  state.lastRecords.forEach((r) => {
    const viaKind = r.via === "pull-full" || r.via === "pull-point" ? "purple"
      : r.via === "operator" ? "info" : "info";
    const viaLabel = { push: "推送", "pull-full": "全量拉取", "pull-point": "定向拉取", operator: "人工裁决" }[r.via] || r.via;
    const firstLabel = { push: "推送", "pull-full": "全量拉取", "pull-point": "定向拉取" }[r.first_via] || r.first_via;
    const tr = el("tr", "clickable" + (state.traceKey === r.biz_key ? " sel" : ""));
    const tdCk = el("td");
    const ck = el("input");
    ck.type = "checkbox";
    ck.checked = state.selectedKeys.has(r.biz_key);
    ck.addEventListener("click", (e) => {
      e.stopPropagation();
      if (ck.checked) state.selectedKeys.add(r.biz_key);
      else state.selectedKeys.delete(r.biz_key);
    });
    tdCk.appendChild(ck);
    tr.appendChild(tdCk);
    tr.appendChild(el("td", "mono", r.biz_key));
    tr.appendChild(el("td", "mono", "r" + r.revision));
    const tdVia = el("td");
    tdVia.appendChild(badge(viaLabel, viaKind));
    tr.appendChild(tdVia);
    tr.appendChild(el("td", "", firstLabel));
    const tdConf = el("td", "mono");
    tdConf.textContent = r.confirmed ? "r" + r.confirmed_rev : "未确认";
    if (!r.confirmed && !r.deleted) tdConf.style.color = "var(--warn)";
    tr.appendChild(tdConf);
    tr.appendChild(el("td", "mono", shortHash(r.content_hash)));
    tr.appendChild(el("td", "mono", fmtTime(r.updated_at)));
    if (r.deleted) tr.style.opacity = ".6";
    tr.addEventListener("click", () => {
      state.traceKey = r.biz_key;
      renderRecords();
      loadTrace(r.biz_key);
    });
    body.appendChild(tr);
  });
  if (!state.lastRecords.length) {
    const tr = el("tr");
    const td = el("td", "empty", "没有记录。先点击“接收推送”。");
    td.colSpan = 8;
    tr.appendChild(td);
    body.appendChild(tr);
  }
  $("#btn-point-selected").disabled = state.selectedKeys.size === 0;
}

async function loadTrace(key) {
  const box = $("#record-trace");
  box.className = "stack";
  box.innerHTML = "";
  let data;
  try {
    data = await api(`/api/sources/${state.sourceId}/events?key=${encodeURIComponent(key)}&limit=200`);
  } catch (e) { toast(e.message, "err"); return; }
  const rec = state.status.records.find((r) => r.biz_key === key);
  if (rec) {
    const head = el("div", "row between");
    head.appendChild(el("b", "", `${key} · 当前 r${rec.revision}`));
    head.appendChild(badge(rec.confirmed ? "已被拉取确认" : "尚未被拉取确认",
      rec.confirmed ? "ok" : "warn"));
    box.appendChild(head);
    box.appendChild(el("pre", "mt8", safeJson(rec.payload)));
  }
  box.appendChild(el("div", "section-title", "事件轨迹（新→旧）"));
  const label = {
    applied: "接受更高修订", tombstone: "接受墓碑", duplicate: "重复事件（计数，不重复入库）",
    stale: "低修订事件（忽略）", conflict: "同修订不同内容（冲突）",
    conflict_repeat: "冲突再次到达", conflict_acknowledged: "冲突已裁决后重复到达",
    conflict_resolved: "冲突已裁决", note: "记录",
  };
  data.forEach((e) => {
    const card = el("div", "panel");
    card.style.background = "#10161d";
    const head = el("div", "row between");
    head.appendChild(el("span", "", `${fmtTime(e.at)} · ${e.via}`));
    const kind = { applied: "ok", tombstone: "warn", conflict: "bad", conflict_repeat: "bad",
      stale: "warn", duplicate: "", note: "", conflict_resolved: "purple" }[e.kind] || "";
    head.appendChild(badge(label[e.kind] || e.kind, kind));
    card.appendChild(head);
    const scope = [];
    if (e.local_rev !== null && e.local_rev !== undefined) scope.push(`本地 r${e.local_rev}`);
    if (e.remote_rev !== null && e.remote_rev !== undefined) scope.push(`外部 r${e.remote_rev}`);
    if (scope.length) card.appendChild(el("div", "hint mono", scope.join(" → ")));
    const d = e.detail || {};
    const bits = [];
    if (d.run_id) bits.push(`run ${d.run_id}`);
    if (d.reason) bits.push(d.reason);
    if (d.decision) bits.push("decision=" + d.decision);
    if (d.before) bits.push(`之前 r${d.before.revision} (${shortHash(d.before.hash)})`);
    if (d.attempt) bits.push(`第 ${d.attempt} 轮`);
    if (bits.length) card.appendChild(el("div", "hint", bits.join(" · ")));
    box.appendChild(card);
  });
  if (!data.length) box.appendChild(el("div", "empty", "没有事件。"));
}

// ---------------------------------------------------------------------------
// evidence view
// ---------------------------------------------------------------------------
async function loadEvidence() {
  const st = state.status;
  if (!st) return;
  const box = $("#evidence-panel");
  box.innerHTML = "";
  const scan = st.scan;
  if (scan) {
    const card = el("div", "panel");
    card.style.background = "#10161d";
    card.appendChild(el("div", "row between", ""));
    card.firstChild.appendChild(el("b", "", "全量扫描证书"));
    card.firstChild.appendChild(badge(scan.verified_now ? "现场复算通过" : "复算失败",
      scan.verified_now ? "ok" : "bad"));
    const dl = el("dl", "kv mt8");
    [
      ["scan_id", scan.scan_id],
      ["覆盖写序号", String(scan.coverage_seq)],
      ["快照 cutoff", String(scan.snapshot_cutoff)],
      ["页数", String(scan.pages)],
      ["页哈希链头", scan.chain_head],
      ["覆盖键区间", scan.coverage ? `${scan.coverage[0]} … ${scan.coverage[1]}` : "—"],
      ["读取行数", String(scan.rows_read)],
      ["证书时间", fmtTime(scan.certified_at)],
    ].forEach(([k, v]) => { dl.appendChild(el("dt", "", k)); dl.appendChild(el("dd", "", v)); });
    card.appendChild(dl);
    if (scan.verify_errors && scan.verify_errors.length) {
      scan.verify_errors.forEach((e) => card.appendChild(el("div", "hint", "· " + e)));
    }
    box.appendChild(card);
  } else {
    box.appendChild(el("div", "empty", "没有证书。执行一次全量核对。"));
  }

  const conv = el("div", "panel");
  conv.style.background = "#10161d";
  conv.appendChild(el("b", "", "结论依据"));
  const ul = el("ul", "evidence");
  st.evidence.forEach((line) => ul.appendChild(el("li", "", line)));
  conv.appendChild(ul);
  box.appendChild(conv);

  const costBox = $("#request-costs");
  costBox.innerHTML = "";
  [["推送批次", st.costs.push_batches], ["全量页", st.costs.pull_full_pages],
   ["定向点查", st.costs.pull_point], ["合计调用", st.costs.total]].forEach(([k, v]) => {
    const s = el("div", "stat");
    s.appendChild(el("div", "k", k));
    s.appendChild(el("div", "v", String(v)));
    costBox.appendChild(s);
  });

  const reqs = await api(`/api/sources/${state.sourceId}/requests`);
  const tbody = $("#requests-body");
  tbody.innerHTML = "";
  reqs.forEach((r) => {
    const tr = el("tr");
    tr.appendChild(el("td", "mono", String(r.id)));
    const tdT = el("td");
    tdT.appendChild(badge({ push: "推送", "pull-full": "全量", "pull-point": "定向" }[r.transport] || r.transport,
      r.transport === "push" ? "info" : r.transport === "pull-full" ? "purple" : ""));
    tr.appendChild(tdT);
    tr.appendChild(el("td", "", r.request_kind));
    tr.appendChild(el("td", "mono", String(r.cost)));
    tr.appendChild(el("td", "mono", String(r.rows_returned)));
    tr.appendChild(el("td", "mono", `${r.applied_count}/${r.duplicate_count}/${r.conflict_count}`));
    tr.appendChild(el("td", "mono", r.covered_from || r.covered_to
      ? `${r.covered_from ?? ""}…${r.covered_to ?? ""}` : "—"));
    tr.appendChild(el("td", "mono", r.page_hash ? shortHash(r.page_hash) : "—"));
    tbody.appendChild(tr);
  });

  const runs = await api(`/api/sources/${state.sourceId}/runs`);
  const rb = $("#runs-body");
  rb.innerHTML = "";
  runs.forEach((r) => {
    const tr = el("tr");
    tr.appendChild(el("td", "mono", r.run_id.replace(/^run_/, "")));
    tr.appendChild(el("td", "", { full: "全量", point: "定向", push: "推送" }[r.kind] || r.kind));
    const tdS = el("td");
    const kind = { completed: "ok", running: "info", failed: "bad", stopped: "warn", interrupted: "warn" }[r.status] || "";
    tdS.appendChild(badge(r.status, kind));
    tr.appendChild(tdS);
    tr.appendChild(el("td", "mono", String(r.requests)));
    tr.appendChild(el("td", "mono", String(r.applied)));
    tr.appendChild(el("td", "mono", String(r.duplicates)));
    tr.appendChild(el("td", "mono", String(r.conflicts)));
    tr.appendChild(el("td", "mono", r.coverage_epoch ?? "—"));
    tr.appendChild(el("td", "mono", fmtTime(r.started_at)));
    const tdA = el("td");
    if (["full", "point"].includes(r.kind) &&
        ["interrupted", "failed", "stopped", "running"].includes(r.status)) {
      const btn = el("button", "ghost", "续跑");
      btn.addEventListener("click", async () => {
        try {
          const res = await api("/api/scan/resume", { method: "POST", body: { run_id: r.run_id, background: true, page_delay_ms: 500 } });
          watchRun(res.run_id);
          toast("已续跑 " + r.run_id, "ok");
        } catch (e) { toast(e.message, "err"); }
      });
      tdA.appendChild(btn);
    }
    tr.appendChild(tdA);
    rb.appendChild(tr);
  });

  const truth = st.truth;
  const tb = $("#truth");
  tb.innerHTML = "";
  const issues = truth.missing.length + truth.behind.length + truth.ahead.length +
    truth.hash_mismatch.length + truth.extra_local.length;
  tb.appendChild(el("div", "hint", truth.warning));
  const g = el("div", "stat-grid mt8");
  [["缺失键", truth.missing.length], ["落后修订", truth.behind.length],
   ["本地超前", truth.ahead.length], ["哈希不符", truth.hash_mismatch.length],
   ["本地多余", truth.extra_local.length]].forEach(([k, v]) => {
    const s = el("div", "stat");
    s.appendChild(el("div", "k", k));
    const vv = el("div", "v", String(v));
    if (v > 0) vv.style.color = "var(--bad)";
    s.appendChild(vv);
    g.appendChild(s);
  });
  tb.appendChild(g);
  if (issues === 0) tb.appendChild(el("div", "hint", "本地状态与虚构上游完全一致。"));
  if (truth.behind.length) tb.appendChild(el("pre", "", safeJson(truth.behind)));
  if (truth.hash_mismatch.length) tb.appendChild(el("pre", "", safeJson(truth.hash_mismatch)));
  if (truth.missing.length) tb.appendChild(el("div", "hint mono", "缺失：" + truth.missing.join(", ")));
}

// ---------------------------------------------------------------------------
// actions
// ---------------------------------------------------------------------------
function syncConfig() {
  const s = state.sources.find((x) => x.source_id === state.sourceId);
  if (!s) return;
  $("#cfg-push").value = s.push_enabled ? "1" : "0";
  $("#cfg-pull").value = s.pull_enabled ? "1" : "0";
  $("#cfg-page").value = s.page_size;
  $("#cfg-delay").value = s.pull_delay_ms;
  $("#cfg-drop").value = s.drop_push ? "1" : "0";
  $("#cfg-rate").value = s.drop_rate;
  const dl = $("#key-list");
  dl.innerHTML = "";
  if (state.status) {
    state.status.records.forEach((r) => {
      const o = el("option");
      o.value = r.biz_key;
      dl.appendChild(o);
    });
  }
}

async function act(fn, okMsg) {
  try {
    const res = await fn();
    if (okMsg) toast(typeof okMsg === "function" ? okMsg(res) : okMsg, "ok");
    await refreshAll();
    return res;
  } catch (e) {
    toast(e.message, "err");
    throw e;
  }
}

function watchRun(runId) {
  state.activeRunId = runId;
  if (state.pollTimer) clearInterval(state.pollTimer);
  const box = $("#run-progress");
  box.innerHTML = "";
  box.appendChild(el("div", "hint", "运行 " + runId + " 进行中…"));
  state.pollTimer = setInterval(async () => {
    try {
      const run = await api(`/api/runs/${runId}`);
      renderProgress(run);
      if (!run.worker_alive && run.status !== "running") {
        clearInterval(state.pollTimer);
        state.pollTimer = null;
        await refreshAll();
      }
    } catch (e) {
      clearInterval(state.pollTimer);
      state.pollTimer = null;
      toast(e.message, "err");
    }
  }, 700);
}

function renderProgress(run) {
  const box = $("#run-progress");
  box.innerHTML = "";
  const row = el("div", "row between");
  row.appendChild(el("span", "mono", run.run_id));
  row.appendChild(badge(run.status, { completed: "ok", running: "info", interrupted: "warn", stopped: "warn", failed: "bad" }[run.status] || ""));
  box.appendChild(row);
  const pr = el("div", "progress mt8");
  const inner = el("i");
  const done = run.requests_detail ? run.requests_detail.length : 0;
  inner.style.width = Math.min(100, done * 12) + "%";
  pr.appendChild(inner);
  box.appendChild(pr);
  box.appendChild(el("div", "hint mt8",
    `已请求 ${done} 次 · 应用 ${run.applied} · 重复 ${run.duplicates} · 冲突 ${run.conflicts}`));
  if (run.cursor) box.appendChild(el("div", "hint mono", "断点：" + run.cursor));
  if (run.proof_json && run.proof_json.claim) {
    box.appendChild(el("div", "hint mt8", "证书：" + run.proof_json.claim));
  }
  if (run.error) box.appendChild(el("div", "hint", "错误：" + run.error));
  if (run.status === "running" && run.worker_alive) {
    const stop = el("button", "ghost mt8", "停止并保留断点");
    stop.addEventListener("click", async () => {
      try {
        await api(`/api/runs/${run.run_id}/stop`, { method: "POST", body: {} });
        toast("已请求停止，当前页完成后保留断点", "ok");
      } catch (e) { toast(e.message, "err"); }
    });
    box.appendChild(stop);
  }
}

// ---------------------------------------------------------------------------
// wiring
// ---------------------------------------------------------------------------
function wire() {
  document.querySelectorAll(".tab").forEach((t) => {
    t.addEventListener("click", () => {
      document.querySelectorAll(".tab").forEach((x) => x.classList.remove("active"));
      t.classList.add("active");
      state.tab = t.dataset.tab;
      ["collect", "records", "evidence"].forEach((name) => {
        $("#tab-" + name).style.display = name === state.tab ? "" : "none";
      });
      if (state.tab === "records") renderRecords();
      if (state.tab === "evidence") loadEvidence();
    });
  });

  $("#btn-drain").addEventListener("click", () =>
    act(() => api("/api/push/drain", { method: "POST", body: { source_id: state.sourceId } }),
      (res) => {
        const r = res.result;
        return `接收 ${r.batches} 批 / ${r.events} 事件，发现 ${r.gaps.length} 段缺口`;
      }));

  $("#cfg-save").addEventListener("click", () =>
    act(() => api("/api/source/config", {
      method: "POST",
      body: {
        source_id: state.sourceId,
        push_enabled: $("#cfg-push").value === "1",
        pull_enabled: $("#cfg-pull").value === "1",
        page_size: Number($("#cfg-page").value),
        pull_delay_ms: Number($("#cfg-delay").value),
        drop_push: $("#cfg-drop").value === "1",
        drop_rate: Number($("#cfg-rate").value),
      },
    }), "配置已应用"));

  $("#btn-reset").addEventListener("click", () => {
    if (!confirm("重置该来源的本地采集状态与虚构上游数据？")) return;
    act(() => api("/api/reset", { method: "POST", body: { source_id: state.sourceId } }), "已重置");
  });

  $("#btn-write").addEventListener("click", () => {
    const key = $("#write-key").value.trim();
    if (!key) { toast("请填写业务键", "err"); return; }
    act(() => api("/api/sim/write", {
      method: "POST",
      body: { source_id: state.sourceId, biz_key: key,
        fields: { name: "手工写入·" + key, note: $("#write-note").value.trim() || "manual" } },
    }), "已在上游写入一条，推送事件已入队");
  });

  $("#btn-outage").addEventListener("click", () =>
    act(() => api("/api/sim/outage", { method: "POST", body: { source_id: state.sourceId, count: 3 } }),
      "接下来的 3 个推送事件将丢失"));

  $("#btn-inject").addEventListener("click", () => {
    const key = (state.traceKey || $("#write-key").value || "").trim();
    if (!key) { toast("请先选择或填写业务键", "err"); return; }
    act(() => api("/api/sim/inject-conflict", {
      method: "POST", body: { source_id: state.sourceId, biz_key: key },
    })).then(() => {
      toast("已注入冲突事件，点击“接收推送”查看裁决队列", "ok");
    });
  });

  $("#btn-full").addEventListener("click", async () => {
    const crash = $("#chk-crash").checked;
    try {
      const res = await api("/api/scan/full", {
        method: "POST",
        body: { source_id: state.sourceId, background: true, page_delay_ms: 550,
          crash_after_pages: crash ? 1 : null },
      });
      toast(crash ? "全量核对已启动，将在第 1 页后模拟工作进程停止" : "全量核对已启动", "ok");
      watchRun(res.run_id);
    } catch (e) { toast(e.message, "err"); }
  });

  const pointBody = (keys) => {
    const body = { source_id: state.sourceId, confirm: true };
    if (keys) body.keys = keys;
    if ($("#chk-revise").checked) {
      const targets = keys && keys.length ? keys
        : (state.status ? state.status.suspects.map((s) => s.biz_key) : []);
      const revise = {};
      targets.forEach((k) => { revise[k] = "revised-during-repair"; });
      body.revise_before_confirm = revise;
    }
    return body;
  };

  $("#btn-point").addEventListener("click", async () => {
    try {
      const res = await api("/api/point/run", {
        method: "POST",
        body: pointBody(null),
      });
      toast(`定向补采：${res.requests} 次点查，确认 ${res.confirmed}，未找到 ${res.not_found}` +
        (res.unstable && res.unstable.length ? `，期间被再次修订 ${res.unstable.length} 次` : ""), "ok");
      await refreshAll();
    } catch (e) { toast(e.message, "err"); }
  });

  $("#btn-point-selected").addEventListener("click", async () => {
    try {
      const keys = Array.from(state.selectedKeys);
      const res = await api("/api/point/run", {
        method: "POST",
        body: pointBody(keys),
      });
      toast(`对 ${keys.length} 个键补采：确认 ${res.confirmed}，未找到 ${res.not_found}` +
        (res.unstable && res.unstable.length ? `，期间被再次修订 ${res.unstable.length} 次` : ""), "ok");
      await refreshAll();
    } catch (e) { toast(e.message, "err"); }
  });

  $("#rec-all").addEventListener("change", (e) => {
    state.lastRecords.forEach((r) => {
      if (e.target.checked) state.selectedKeys.add(r.biz_key);
      else state.selectedKeys.delete(r.biz_key);
    });
    renderRecords();
  });
  $("#record-filter").addEventListener("input", renderRecords);

  $("#write-key").addEventListener("input", () => {
    const key = $("#write-key").value.trim();
    if (state.status && state.status.records.find((r) => r.biz_key === key)) {
      state.traceKey = key;
    }
  });
}

async function loadScenarios() {
  const data = await api("/api/scenarios");
  const box = $("#scenario-list");
  box.innerHTML = "";
  data.scenarios.forEach((s) => {
    const card = el("div", "panel");
    card.style.background = "#10161d";
    const row = el("div", "row between");
    row.appendChild(el("b", "", s.label));
    const btn = el("button", "ghost", "布置");
    row.appendChild(btn);
    card.appendChild(row);
    card.appendChild(el("div", "hint mt8", s.blurb));
    btn.addEventListener("click", async () => {
      try {
        const res = await api("/api/scenario", {
          method: "POST", body: { source_id: state.sourceId, name: s.id },
        });
        toast(`${res.scenario}：${res.arranged}`, "ok");
        await refreshAll();
      } catch (e) { toast(e.message, "err"); }
    });
    box.appendChild(card);
  });
}

async function boot() {
  wire();
  try {
    await loadSources();
    await loadScenarios();
    await refreshAll();
  } catch (e) {
    toast(e.message, "err");
  }
}

boot();
