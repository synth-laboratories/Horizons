import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
from matplotlib.lines import Line2D
import os

# Palette
BG      = "#FAFAFA"
FG      = "#1A1A1A"
MUTED   = "#6B6B6B"
ACCENT  = "#F05020"
ACCENT2 = "#FF8A66"
FILL    = "#FFF4F0"
FILL2   = "#FFFFFF"
BORDER  = "#E0E0E0"
DASH_C  = "#BBBBBB"

def rr(ax, x, y, w, h, fc, ec, lw=1.4, r=14, z=1):
    ax.add_patch(FancyBboxPatch(
        (x, y), w, h,
        boxstyle=f"round,pad=0,rounding_size={r}",
        lw=lw, ec=ec, fc=fc, zorder=z
    ))

def tx(ax, x, y, s, size=13, color=FG, weight="regular", ha="center", va="center", z=5):
    ax.text(x, y, s, fontsize=size, color=color, fontweight=weight,
            ha=ha, va=va, zorder=z, family="Helvetica Neue")

W, H = 1400, 1000
dpi = 180
fig = plt.figure(figsize=(W/dpi, H/dpi), dpi=dpi)
ax = fig.add_axes([0, 0, 1, 1])
ax.set_xlim(0, W)
ax.set_ylim(0, H)
ax.axis("off")
fig.patch.set_facecolor(BG)
ax.set_facecolor(BG)

# Title
tx(ax, 100, 960, "Horizons", size=22, weight="bold", ha="left")

# Layout
L    = 120
R    = 1100
MW   = R - L   # 980
gap  = 24
bw   = (MW - 2*gap) / 3   # ~310
bh   = 56

# --- Row 1: Interfaces ---
y = 890
labels = ["Rust SDK", "Python SDK", "TypeScript SDK"]
for i, lab in enumerate(labels):
    x = L + i * (bw + gap)
    rr(ax, x, y, bw, bh, fc=FILL, ec=ACCENT, lw=1.6)
    tx(ax, x + bw/2, y + bh/2, lab, size=12, weight="bold")

tx(ax, L - 60, y + bh/2, "Interfaces", size=10, color=MUTED, ha="right")
ax.annotate("", xy=(L - 8, y + bh/2), xytext=(L - 48, y + bh/2),
            arrowprops=dict(arrowstyle="->", color=MUTED, lw=1.2))

# --- Row 2: Agents ---
y = 810
labels = ["Your Agents", "Codex · Claude · OpenCode", "Third-Party Agents"]
for i, lab in enumerate(labels):
    x = L + i * (bw + gap)
    rr(ax, x, y, bw, bh, fc=FILL2, ec=BORDER)
    tx(ax, x + bw/2, y + bh/2, lab, size=12, weight="bold")

tx(ax, L - 60, y + bh/2, "Agents", size=10, color=MUTED, ha="right")
ax.annotate("", xy=(L - 8, y + bh/2), xytext=(L - 48, y + bh/2),
            arrowprops=dict(arrowstyle="->", color=MUTED, lw=1.2))

# --- Wide bars ---
bar_h = 80
bar_gap = 18

def wide_bar(y, title, subtitle, fc=FILL, ec=ACCENT):
    rr(ax, L, y, MW, bar_h, fc=fc, ec=ec, lw=1.6)
    tx(ax, L + MW/2, y + bar_h * 0.6, title, size=14, weight="bold")
    tx(ax, L + MW/2, y + bar_h * 0.28, subtitle, size=10, color=MUTED)
    return y

y = 700
wide_bar(y,
    "Evaluation, Optimization, and Continual Learning",
    "RLM scoring · MIPRO prompt optimization · GEPA reflective evolution")

y -= (bar_h + bar_gap)
wide_bar(y,
    "Agent Execution",
    "Sandbox runtime · MCP tools · Voyager memory · action approvals")

y -= (bar_h + bar_gap)
wide_bar(y,
    "Business Context",
    "Context Refresh · graph engine · on-board storage")

y -= (bar_h + bar_gap)
wide_bar(y,
    "Event Sync",
    "Bidirectional pub/sub with routing, retries, replay, and DLQ")

# --- Dashed line ---
sep_y = y - 30
ax.add_line(Line2D([L, R], [sep_y, sep_y], color=DASH_C, lw=1.2, ls=(0, (6, 6))))
tx(ax, L + MW/2, sep_y - 16, "Horizons sits as a sidecar to your backend", size=9, color=MUTED, weight="bold")

# --- Your Backend ---
y_be = sep_y - 80
rr(ax, L, y_be, MW, 60, fc=BG, ec=DASH_C, lw=1.2)
tx(ax, L + MW/2, y_be + 36, "Your Backend", size=14, weight="bold")
tx(ax, L + MW/2, y_be + 14, "Business logic · APIs · databases · auth", size=10, color=MUTED)

# --- Systems of Record ---
sor_w = 400
sor_x = L + (MW - sor_w) / 2
y_sr = y_be - 72
rr(ax, sor_x, y_sr, sor_w, 50, fc=FILL2, ec=BORDER, lw=1.2)
tx(ax, sor_x + sor_w/2, y_sr + 30, "Your Systems of Record", size=12, weight="bold")
tx(ax, sor_x + sor_w/2, y_sr + 12, "Gmail · CRM · GitHub · Jira · S3", size=9, color=MUTED)

# --- Right-side governance text ---
gx = R + 40
gy_top = 860
lines = [
    "Tenant isolation,",
    "approval gates,",
    "audit trail, ACLs",
    "",
    "designed for",
    "governed and",
    "regulated work"
]
# bracket
ax.add_line(Line2D([gx, gx], [700, gy_top], color=MUTED, lw=1.2))
ax.add_line(Line2D([gx - 12, gx], [gy_top, gy_top], color=MUTED, lw=1.2))
ax.add_line(Line2D([gx - 12, gx], [700, 700], color=MUTED, lw=1.2))
for i, ln in enumerate(lines):
    tx(ax, gx + 14, gy_top - 8 - 18*i, ln, size=9.5, color=MUTED, ha="left", weight="bold" if i >= 4 else "regular")

# Save
out = os.path.dirname(os.path.abspath(__file__))
fig.savefig(os.path.join(out, "horizons_diagram_synth.png"), dpi=dpi, facecolor=BG, bbox_inches="tight", pad_inches=0.3)
fig.savefig(os.path.join(out, "horizons_diagram_synth.svg"), facecolor=BG, bbox_inches="tight", pad_inches=0.3)
plt.close(fig)
print("Done")
