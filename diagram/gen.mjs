import { writeFileSync } from "node:fs";
import { Resvg } from "@resvg/resvg-js";

// ── Light theme matching Frontier aesthetic ─────────────────────
const T = {
  bg:       "#F5F5F5",
  fill:     "#FFF0EA",
  fillSoft: "#FFF5F0",
  border:   "#E8B8A0",
  borderLt: "#DDDAD7",
  text:     "#1A1A1A",
  sub:      "#6B6B6B",
  accent:   "#E8590C",
  r:        12,
  font:     "Helvetica, Arial, sans-serif",
};

const W = 960, H = 780;

// ── Helpers ────────────────────────────────────────────────────
const esc = s => s.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");

const box = ({ x, y, w, h, fill = T.fill, stroke = T.border, sw = 1.5, r = T.r }) =>
  `<rect x="${x}" y="${y}" width="${w}" height="${h}" rx="${r}" fill="${fill}" stroke="${stroke}" stroke-width="${sw}"/>`;

const txt = ({ x, y, s, size = 14, fill = T.text, bold = true, anchor = "middle" }) =>
  `<text x="${x}" y="${y}" fill="${fill}" font-family="${T.font}" font-size="${size}" font-weight="${bold ? 700 : 400}" text-anchor="${anchor}" dominant-baseline="central">${esc(s)}</text>`;

// ── Grid ───────────────────────────────────────────────────────
const L  = 100;
const R  = 860;
const CW = R - L;           // 760
const g  = 16;               // gap between columns
const bw = (CW - 2*g) / 3;  // ~242
const bh = 52;

const colX = i => L + i * (bw + g);

// bar dimensions
const barH = 64;
const barG = 12;

// ── Y positions ────────────────────────────────────────────────
const titleY = 40;
const r1 = 80;               // row 1
const r2 = r1 + bh + 16;     // row 2
const s0 = r2 + bh + 28;     // first stack bar

const sY = i => s0 + i * (barH + barG);

const evalY  = sY(0);
const execY  = sY(1);
const ctxY   = sY(2);
const eventY = sY(3);

const dashY  = eventY + barH + 20;
const beY    = dashY + 24;
const sorY   = beY + 64 + 16;

// ── Compose ────────────────────────────────────────────────────
const o = [];

o.push(`<?xml version="1.0" encoding="UTF-8"?>`);
o.push(`<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}">`);
o.push(`<rect width="100%" height="100%" fill="${T.bg}"/>`);

// Title
o.push(txt({ x: W/2, y: titleY, s: "Horizons", size: 32, bold: true }));
o.push(txt({ x: W/2, y: titleY + 26, s: "The Open Source Agent Platform", size: 12, fill: T.sub, bold: false }));

// Left labels
const arrow = (y) => {
  o.push(txt({ x: L - 40, y, s: "", size: 10, fill: T.sub, bold: false, anchor: "end" }));
};

// Row 1: Interfaces
o.push(txt({ x: L - 8, y: r1 + bh/2, s: "Interfaces  \u2192", size: 10, fill: T.sub, bold: false, anchor: "end" }));

["Rust SDK", "Python SDK", "TypeScript SDK"].forEach((s, i) => {
  const x = colX(i);
  o.push(box({ x, y: r1, w: bw, h: bh, fill: T.fill, stroke: T.accent, sw: 1.5 }));
  o.push(txt({ x: x + bw/2, y: r1 + bh/2, s, size: 13 }));
});

// Row 2: Agents
o.push(txt({ x: L - 8, y: r2 + bh/2, s: "Agents  \u2192", size: 10, fill: T.sub, bold: false, anchor: "end" }));

const agents = ["Your Agents", "Codex \u00B7 Claude \u00B7 OpenCode", "Third-Party Agents"];
agents.forEach((s, i) => {
  const x = colX(i);
  o.push(box({ x, y: r2, w: bw, h: bh, fill: "#EFEFEF", stroke: T.borderLt }));
  o.push(txt({ x: x + bw/2, y: r2 + bh/2, s, size: 12 }));
});

// Stack bars — reordered: Exec, Biz Context, Eval, Event Sync
// Eval between Business Context and Event Sync, in Synth red
const bars = [
  { title: "Agent Execution",
    sub: "Sandbox runtime  \u00B7  MCP tools  \u00B7  Voyager memory  \u00B7  action approvals",
    red: false },
  { title: "Business Context",
    sub: "Context Refresh  \u00B7  graph engine  \u00B7  on-board storage",
    red: false },
  { title: "Evaluation, Optimization & Continual Learning",
    sub: "RLM scoring  \u00B7  MIPRO prompt optimization  \u00B7  GEPA reflective evolution",
    red: true },
  { title: "Event Sync",
    sub: "Bidirectional pub/sub  \u00B7  routing  \u00B7  retries  \u00B7  replay  \u00B7  DLQ",
    red: false },
];

bars.forEach((bar, i) => {
  const y = sY(i);
  if (bar.red) {
    o.push(box({ x: L, y, w: CW, h: barH, fill: T.accent, stroke: T.accent, sw: 0 }));
    o.push(txt({ x: L + CW/2, y: y + barH * 0.36, s: bar.title, size: 14, fill: "#FFFFFF" }));
    o.push(txt({ x: L + CW/2, y: y + barH * 0.68, s: bar.sub, size: 10, fill: "rgba(255,255,255,0.75)", bold: false }));
  } else {
    o.push(box({ x: L, y, w: CW, h: barH, fill: T.fillSoft, stroke: T.border }));
    o.push(txt({ x: L + CW/2, y: y + barH * 0.36, s: bar.title, size: 14 }));
    o.push(txt({ x: L + CW/2, y: y + barH * 0.68, s: bar.sub, size: 10, fill: T.sub, bold: false }));
  }
});

// Dashed separator
o.push(`<line x1="${L}" y1="${dashY}" x2="${R}" y2="${dashY}" stroke="#D0D0D0" stroke-width="1" stroke-dasharray="6 4"/>`);
o.push(txt({ x: L + CW/2, y: dashY + 12, s: "Horizons sits as a sidecar to your backend", size: 9, fill: T.sub, bold: false }));

// Your Backend
o.push(box({ x: L, y: beY, w: CW, h: 56, fill: "#EFEFEF", stroke: "#D0D0D0", sw: 1 }));
o.push(txt({ x: L + CW/2, y: beY + 22, s: "Your Backend", size: 14 }));
o.push(txt({ x: L + CW/2, y: beY + 42, s: "Business logic  \u00B7  APIs  \u00B7  databases  \u00B7  auth", size: 10, fill: T.sub, bold: false }));

// Systems of Record
const sw = 340, sx = L + (CW - sw)/2;
o.push(box({ x: sx, y: sorY, w: sw, h: 48, fill: "#EFEFEF", stroke: T.borderLt }));
o.push(txt({ x: sx + sw/2, y: sorY + 18, s: "Your Systems of Record", size: 12 }));
o.push(txt({ x: sx + sw/2, y: sorY + 36, s: "Gmail  \u00B7  CRM  \u00B7  GitHub  \u00B7  Jira  \u00B7  S3", size: 9, fill: T.sub, bold: false }));

// Right governance annotation
const gx = R + 16;
const gTop = r2;
const gBot = sY(2) + barH;  // bottom of eval bar (now at index 2)
o.push(`<path d="M ${gx-4} ${gTop} L ${gx} ${gTop} L ${gx} ${gBot} L ${gx-4} ${gBot}" fill="none" stroke="#C0C0C0" stroke-width="1"/>`);

const gov = [
  "Tenant isolation,",
  "approval gates,",
  "audit trail, ACLs",
  "",
  "designed for",
  "governed and",
  "regulated work",
];
gov.forEach((s, i) => {
  if (!s) return;
  o.push(txt({ x: gx + 10, y: gTop + 12 + i * 15, s, size: 9, fill: T.sub, bold: i >= 4, anchor: "start" }));
});

o.push(`</svg>`);

// ── Write ──────────────────────────────────────────────────────
const svg = o.join("\n");
writeFileSync("../horizons_diagram.svg", svg, "utf8");

const resvg = new Resvg(svg, {
  fitTo: { mode: "width", value: 2400 },
  font: { defaultFontFamily: "Helvetica" },
});
writeFileSync("../horizons_diagram.png", resvg.render().asPng());

console.log("done");
