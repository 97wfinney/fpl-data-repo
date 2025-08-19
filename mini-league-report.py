#!/usr/bin/env python3
"""
mini-league-report.py

Generates a weekly Gameweek (GW) report for your mini-league from the data collected by
mini-league-analysis.py, and writes:
  - reports/gw{GW}.md    : human-readable weekly report
  - reports/rolling.csv  : per-manager per-GW metrics (appended)

Adds richer rolling metrics:
- points_vs_mean, rank_delta, squad_value_m, bank_m
- cap_raw_pts, cap_effect_pts, cap_vs_field
- transfer_count, transfer_in_pts, transfer_out_pts, net_transfer_gain
- autosubs_pts, bench_waste
- template_overlap, template_overlap_pct, differentials_started, leverage_points
- flagged_starters

Markdown now includes a detailed **Manager breakdown** table and an optional
**Your manager spotlight** section when --me is provided.
"""

import os
import csv
import json
import argparse
from collections import Counter, defaultdict
from statistics import mean, median
from datetime import datetime

import requests

# Paths
LEAGUE_DIR = "mini_league"
ENTRIES_INDEX = os.path.join(LEAGUE_DIR, "entries_index.json")
OUTPUT_DIR = "reports"
BOOTSTRAP_CACHE = os.path.join(LEAGUE_DIR, "bootstrap_cache.json")
BASE_URL = "https://fantasy.premierleague.com/api/"

# Rolling CSV schema (keep in one place)
CSV_FIELDNAMES = [
    "gw", "entry", "entry_name", "manager",
    "points", "points_vs_mean", "rank", "overall_rank", "overall_rank_prev", "rank_delta",
    "captain_id", "captain_name", "cap_raw_pts", "cap_effect_pts", "cap_vs_field",
    "bench_points", "autosubs", "autosubs_pts", "bench_waste",
    "hit_cost", "chip", "transfer_count", "transfer_in_pts", "transfer_out_pts", "net_transfer_gain",
    "formation", "premium_count",
    "template_overlap", "template_overlap_pct", "differentials_started", "leverage_points",
    "flagged_starters", "squad_value_m", "bank_m",
]

# ---------- Helpers ----------

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def load_json(path, default=None):
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return default


def write_text(path, text: str):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def fetch_json(url, timeout=15):
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "mini-league-report"})
        if r.status_code == 200:
            return r.json()
        return None
    except Exception:
        return None


# ---------- Bootstrap & Live ----------

def load_bootstrap():
    data = load_json(BOOTSTRAP_CACHE)
    if not data:
        data = fetch_json(BASE_URL + "bootstrap-static/") or {}
    elements = {e.get("id"): e for e in data.get("elements", [])}
    teams = {t.get("id"): t for t in data.get("teams", [])}
    types = {et.get("id"): et for et in data.get("element_types", [])}

    player_name = {}
    player_team = {}
    player_pos_short = {}
    player_cost_now = {}
    player_status = {}

    for pid, e in elements.items():
        name = f"{e.get('first_name','')} {e.get('second_name','')}".strip()
        player_name[pid] = name or e.get("web_name", str(pid))
        player_team[pid] = teams.get(e.get("team"), {}).get("short_name", "")
        player_pos_short[pid] = types.get(e.get("element_type"), {}).get("singular_name_short", "")
        player_cost_now[pid] = e.get("now_cost")  # e.g., 101 == £10.1m
        player_status[pid] = e.get("status")  # 'a','d','i','s','u'

    return {
        "elements": elements,
        "teams": teams,
        "types": types,
        "player_name": player_name,
        "player_team": player_team,
        "player_pos": player_pos_short,
        "player_cost": player_cost_now,
        "player_status": player_status,
    }


def load_live_points(gw: int):
    live = fetch_json(BASE_URL + f"event/{gw}/live/") or {}
    pts = {}
    mins_map = {}
    if live and isinstance(live.get("elements"), list):
        for e in live["elements"]:
            pid = e.get("id")
            stats = e.get("stats", {})
            pts[pid] = stats.get("total_points", 0)
            mins_map[pid] = stats.get("minutes", 0)
    return pts, mins_map


# ---------- Core data loading ----------

def load_entries_index():
    idx = load_json(ENTRIES_INDEX, default={"entries": []})
    entries = idx.get("entries", [])
    return entries


def load_picks(entry_id: int, gw: int):
    path = os.path.join(LEAGUE_DIR, "entries", str(entry_id), "picks", f"gw{gw}.json")
    return load_json(path, default=None)


# ---------- Calculations ----------

def calc_formation(picks):
    # element_type: 1 GKP, 2 DEF, 3 MID, 4 FWD
    g = d = m = f = 0
    for p in picks:
        mult = p.get("multiplier", 0)
        if mult > 0:
            et = p.get("element_type")
            if et == 1:
                g += 1
            elif et == 2:
                d += 1
            elif et == 3:
                m += 1
            elif et == 4:
                f += 1
    return g, d, m, f


def count_premiums(picks, player_cost_map, threshold_m = 10.0):
    cnt = 0
    for p in picks:
        pid = p.get("element")
        cost_tenths = player_cost_map.get(pid)
        if isinstance(cost_tenths, int) and (cost_tenths >= int(threshold_m * 10)):
            cnt += 1
    return cnt


def compute_eo(picks_by_manager, n_entries):
    eo = defaultdict(float)  # pid -> sum multipliers / n
    owners = defaultdict(int)  # pid -> count of managers who own (in 15)
    starters = defaultdict(int)  # pid -> count of managers who started (mult>0)

    for picks in picks_by_manager:
        owned_this_manager = set()
        for p in picks:
            pid = p.get("element")
            mult = p.get("multiplier", 0)
            if pid is None:
                continue
            eo[pid] += float(mult)
            owned_this_manager.add(pid)
            if mult > 0:
                starters[pid] += 1
        for pid in owned_this_manager:
            owners[pid] += 1

    eo = {pid: v / float(max(n_entries, 1)) for pid, v in eo.items()}
    own_rate = {pid: owners[pid] / float(max(n_entries, 1)) for pid in owners}
    start_rate = {pid: starters[pid] / float(max(n_entries, 1)) for pid in starters}
    return eo, own_rate, start_rate


def md_table(rows, headers):
    if not rows:
        return "\n"
    line = "| " + " | ".join(headers) + " |\n"
    sep = "|" + "|".join([" --- "] * len(headers)) + "|\n"
    body = "\n".join(["| " + " | ".join(map(str, r)) + " |" for r in rows]) + "\n"
    return line + sep + body


# ---------- Report generation ----------

def generate_report(gw: int, me_entry_id: int = None, push: bool = False):
    ensure_dir(OUTPUT_DIR)

    bootstrap = load_bootstrap()
    player_name = bootstrap["player_name"]
    player_team = bootstrap["player_team"]
    player_pos = bootstrap["player_pos"]
    player_cost = bootstrap["player_cost"]
    player_status = bootstrap["player_status"]

    live_points, live_mins = load_live_points(gw)

    entries = load_entries_index()
    n_entries = len(entries)

    # per-manager aggregates
    points = []
    ranks = []
    capt_counter = Counter()
    vc_counter = Counter()
    chips_counter = Counter()
    hit_costs = []
    bench_points = []
    autosubs_count = 0
    formations = Counter()
    premium_counts = []

    picks_by_manager = []

    # per-entry caches for second pass enrichments
    entry_to_picks = {}
    entry_to_autosubs_list = {}
    entry_to_eh = {}

    per_manager_rows = []  # for CSV

    # For top-11 ownership check later
    all_pids_seen = set()

    for e in entries:
        entry_id = e.get("entry")
        entry_name = e.get("entry_name", "")
        manager = e.get("player_name", "")

        pdata = load_picks(entry_id, gw)
        if not pdata:
            # skip managers without data for this GW
            continue

        picks = pdata.get("picks", [])
        picks_by_manager.append(picks)
        entry_to_picks[entry_id] = picks

        # points/rank from entry_history in picks file (single GW view)
        eh = pdata.get("entry_history", {})
        entry_to_eh[entry_id] = eh
        gw_points = eh.get("points")
        gw_rank = eh.get("rank")
        points.append(gw_points if gw_points is not None else 0)
        if gw_rank is not None:
            ranks.append(gw_rank)

        bench_points.append(eh.get("points_on_bench", 0))
        hit = eh.get("event_transfers_cost", 0)
        if hit and hit > 0:
            hit_costs.append(hit)

        chips_counter.update([pdata.get("active_chip") or "none"])

        cap_id = None
        vc_id = None
        cap_mult = 0
        for p in picks:
            pid = p.get("element")
            mult = p.get("multiplier", 0)
            all_pids_seen.add(pid)
            if p.get("is_captain"):
                cap_id = pid
                cap_mult = mult or cap_mult
            if p.get("is_vice_captain"):
                vc_id = pid

        if cap_id:
            capt_counter.update([cap_id])
        if vc_id:
            vc_counter.update([vc_id])

        # formation and premiums
        g, d, m, f = calc_formation(picks)
        formations.update([f"{g}-{d}-{m}-{f}"])
        premium_counts.append(count_premiums(picks, player_cost))

        autosubs_list = pdata.get("automatic_subs", [])
        entry_to_autosubs_list[entry_id] = autosubs_list
        autosubs_count += len(autosubs_list)

        # captain points
        cap_raw_pts = live_points.get(cap_id, 0) if cap_id else 0
        cap_effect_pts = cap_raw_pts * (cap_mult or (3 if (pdata.get("active_chip") == "triple_captain") else 2)) if cap_id else 0

        # value/bank in millions
        squad_value_m = (eh.get("value", 0) or 0) / 10.0
        bank_m = (eh.get("bank", 0) or 0) / 10.0

        per_manager_rows.append({
            "gw": gw,
            "entry": entry_id,
            "entry_name": entry_name,
            "manager": manager,
            "points": gw_points,
            "rank": gw_rank,
            "overall_rank": eh.get("overall_rank"),
            "captain_id": cap_id,
            "captain_name": player_name.get(cap_id, "-") if cap_id else "-",
            "cap_raw_pts": cap_raw_pts,
            "cap_effect_pts": cap_effect_pts,
            "bench_points": eh.get("points_on_bench", 0),
            "hit_cost": hit or 0,
            "chip": pdata.get("active_chip") or "none",
            "formation": f"{g}-{d}-{m}-{f}",
            "premium_count": premium_counts[-1],
            "autosubs": len(autosubs_list),
            "squad_value_m": round(squad_value_m, 2),
            "bank_m": round(bank_m, 2),
        })

    # League summary
    if points:
        league_min = min(points)
        league_max = max(points)
        league_mean = round(mean(points), 2)
        league_median = median(points)
        spread = league_max - league_min
    else:
        league_min = league_max = league_mean = league_median = spread = 0

    # EO calculations
    eo, own_rate, start_rate = compute_eo(picks_by_manager, n_entries)

    # Captaincy breakdown
    cap_rows = []
    total_cap = sum(capt_counter.values()) or 1
    for pid, cnt in capt_counter.most_common(10):
        cap_rows.append([
            player_name.get(pid, pid),
            player_team.get(pid, ""),
            cnt,
            f"{(cnt/total_cap)*100:.1f}%",
            live_points.get(pid, 0),
        ])

    avg_cap_points = 0.0
    if per_manager_rows:
        cap_pts_list = [r["cap_raw_pts"] for r in per_manager_rows]
        avg_cap_points = round(mean(cap_pts_list), 2)

    # Second pass: per-manager enrichments using league aggregates
    # Build template XI by starters' start rate (tie-break by EO)
    template_sorted = sorted(start_rate.items(), key=lambda kv: (kv[1], eo.get(kv[0], 0.0)), reverse=True)
    template_xi = set([pid for pid, _ in template_sorted[:11]])

    league_mean_pts = league_mean

    # helper to get previous overall rank
    def prev_overall_rank(entry_id: int):
        prev = load_picks(entry_id, gw-1) if gw and gw > 1 else None
        if prev:
            peh = prev.get("entry_history", {})
            return peh.get("overall_rank")
        return None

    # helper to load transfers
    def load_transfers(entry_id: int):
        path = os.path.join(LEAGUE_DIR, "entries", str(entry_id), "transfers.json")
        return load_json(path, default=[])

    # starters list for leverage/differentials
    def starter_pids(picks):
        return [p.get("element") for p in picks if p.get("multiplier", 0) > 0]

    for row in per_manager_rows:
        entry_id = row["entry"]
        picks = entry_to_picks.get(entry_id, [])
        starters = starter_pids(picks)
        starters_set = set(starters)

        # Points vs mean
        row["points_vs_mean"] = (row.get("points") or 0) - league_mean_pts

        # Rank delta
        cur_or = row.get("overall_rank")
        prev_or = prev_overall_rank(entry_id)
        row["rank_delta"] = (prev_or - cur_or) if (isinstance(prev_or, int) and isinstance(cur_or, int)) else None
        row["overall_rank_prev"] = prev_or

        # Captain vs field
        row["cap_vs_field"] = (row.get("cap_raw_pts") or 0) - avg_cap_points

        # Autosubs points & bench waste
        auto_list = entry_to_autosubs_list.get(entry_id, [])
        autosubs_pts = sum(live_points.get(a.get("element_in"), 0) for a in auto_list)
        row["autosubs_pts"] = autosubs_pts
        row["bench_waste"] = (row.get("bench_points") or 0) - autosubs_pts

        # Transfers
        transfers = [t for t in load_transfers(entry_id) if t.get("event") == gw]
        row["transfer_count"] = len(transfers)
        tin = sum(live_points.get(t.get("element_in"), 0) for t in transfers)
        tout = sum(live_points.get(t.get("element_out"), 0) for t in transfers)
        row["transfer_in_pts"] = tin
        row["transfer_out_pts"] = tout
        row["net_transfer_gain"] = (tin - tout) - (row.get("hit_cost") or 0)

        # Template & leverage
        overlap = len(starters_set & template_xi)
        row["template_overlap"] = overlap
        row["template_overlap_pct"] = round(overlap / 11.0, 3)
        diffs_started = sum(1 for pid in starters if (start_rate.get(pid, 0.0) < 0.20))
        row["differentials_started"] = diffs_started
        leverage_points = sum(live_points.get(pid, 0) * (1.0 - start_rate.get(pid, 0.0)) for pid in starters)
        row["leverage_points"] = round(leverage_points, 2)

        # Flagged starters
        row["flagged_starters"] = sum(1 for pid in starters if player_status.get(pid) in {"d", "i", "s"})

    # Differentials: starters <20% who returned
    diff_rows = []
    for pid, sr in start_rate.items():
        if sr < 0.20 and live_points.get(pid, 0) > 0:
            diff_rows.append([
                player_name.get(pid, pid),
                player_team.get(pid, ""),
                f"{sr*100:.1f}%",
                live_points.get(pid, 0),
            ])
    diff_rows.sort(key=lambda r: r[-1], reverse=True)
    diff_rows = diff_rows[:15]

    # Transfers & hits (league-level)
    pct_hits = (len(hit_costs) / float(n_entries)) * 100 if n_entries else 0.0
    avg_hit_cost = round(mean(hit_costs), 2) if hit_costs else 0.0

    # Chips usage
    chip_rows = []
    for chip, cnt in chips_counter.most_common():
        chip_rows.append([chip, cnt, f"{(cnt/float(n_entries or 1))*100:.1f}%"]) 

    # Bench waste & autosubs (league-level)
    avg_bench = round(mean(bench_points), 2) if bench_points else 0.0

    # Formation & structure
    formation_rows = [[form, cnt, f"{(cnt/float(n_entries or 1))*100:.1f}%"] for form, cnt in formations.most_common()]
    avg_premiums = round(mean(premium_counts), 2) if premium_counts else 0.0

    # Top 11 by points vs ownership (only among seen pids for speed)
    top_players = sorted(((pid, live_points.get(pid, 0)) for pid in all_pids_seen), key=lambda x: x[1], reverse=True)[:11]
    top_rows = []
    for pid, pts in top_players:
        top_rows.append([
            player_name.get(pid, pid),
            player_team.get(pid, ""),
            pts,
            f"own {own_rate.get(pid,0)*100:.1f}%",
            f"start {start_rate.get(pid,0)*100:.1f}%",
        ])

    # Flagged starters exposure (league view)
    flagged_rows = []
    flagged_codes = {"d": "doubt", "i": "inj", "s": "susp"}
    for pid, sr in start_rate.items():
        status = player_status.get(pid)
        if sr > 0 and status in flagged_codes:
            flagged_rows.append([
                player_name.get(pid, pid),
                player_team.get(pid, ""),
                flagged_codes[status],
                f"start {sr*100:.1f}%",
                live_points.get(pid, 0),
            ])
    flagged_rows.sort(key=lambda r: r[-1], reverse=True)

    # Markdown report
    md = []
    md.append(f"# Mini-League Report — GW{gw}\n")
    md.append(f"_Generated: {datetime.utcnow().isoformat()}Z — Entries: {n_entries}_\n\n")

    md.append("## League Summary\n")
    md.append(f"- **Points:** min {league_min}, mean {league_mean}, median {league_median}, max {league_max}, spread {spread}\n")
    if me_entry_id is not None:
        me_row = next((r for r in per_manager_rows if r["entry"] == me_entry_id), None)
        if me_row:
            md.append(f"- **Your GW points:** {me_row['points']} — formation {me_row['formation']} — captain {me_row['captain_name']} ({me_row['cap_raw_pts']} pts)\n")
    md.append("\n")

    md.append("## Captaincy\n")
    md.append(md_table(cap_rows, ["Captain", "Team", "Count", "%", "GW pts"]))
    md.append(f"**Average captain raw points:** {avg_cap_points}\n\n")

    md.append("## Effective Ownership (EO) — Top 20 by starters EO\n")
    eo_rows = []
    top_eo = sorted(start_rate.items(), key=lambda kv: kv[1], reverse=True)[:20]
    for pid, sr in top_eo:
        eo_rows.append([
            player_name.get(pid, pid), player_team.get(pid, ""), f"{eo.get(pid,0):.2f}", f"{sr*100:.1f}%", live_points.get(pid, 0)
        ])
    md.append(md_table(eo_rows, ["Player", "Team", "EO (mult)", "Start %", "GW pts"]))
    md.append("\n")

    md.append("## Differentials who returned (starters <20%)\n")
    md.append(md_table(diff_rows, ["Player", "Team", "Start %", "GW pts"]))
    md.append("\n")

    md.append("## Transfers & Hits\n")
    md.append(f"- **% with hits:** {pct_hits:.1f}%  —  **Avg hit cost:** {avg_hit_cost}\n\n")

    md.append("## Bench & Autosubs\n")
    md.append(f"- **Avg bench points:** {avg_bench}  —  **Total autosubs:** {autosubs_count}\n\n")

    md.append("## Formation & Structure\n")
    md.append(md_table(formation_rows, ["Formation", "Count", "%"]))
    md.append(f"- **Avg premium count (>£10.0m):** {avg_premiums}\n\n")

    md.append("## Top 11 by GW points (vs league ownership)\n")
    md.append(md_table(top_rows, ["Player", "Team", "GW pts", "Own %", "Start %"]))
    md.append("\n")

    # Transfers details per manager (player names + points)
    transfer_lines = []
    for r in sorted(per_manager_rows, key=lambda x: x.get("transfer_count", 0), reverse=True):
        entry_id = r.get("entry")
        # reuse helper defined above
        transfers = [t for t in load_transfers(entry_id) if t.get("event") == gw]
        if not transfers:
            continue
        ins = [f"{player_name.get(t.get('element_in'), t.get('element_in'))} ({live_points.get(t.get('element_in'), 0)})" for t in transfers]
        outs = [f"{player_name.get(t.get('element_out'), t.get('element_out'))} ({live_points.get(t.get('element_out'), 0)})" for t in transfers]
        transfer_lines.append(
            f"- {r.get('manager','')} — IN: {', '.join(ins)} | OUT: {', '.join(outs)} | net {r.get('net_transfer_gain',0)} (hit {r.get('hit_cost',0)})"
        )

    if transfer_lines:
        md.append("## Transfers details (GW)\n")
        md.extend(transfer_lines)
        md.append("\n")

    # New: Manager breakdown table (per-manager specifics)
    md.append("## Manager breakdown (GW)\n")
    mgr_rows = []
    for r in sorted(per_manager_rows, key=lambda x: (x.get("points") or 0), reverse=True):
        mgr_rows.append([
            r.get("manager", ""),
            r.get("entry_name", ""),
            r.get("points", 0),
            f"{(r.get('points_vs_mean') or 0):+.1f}",
            r.get("rank", ""),
            r.get("overall_rank", ""),
            r.get("rank_delta", ""),
            r.get("captain_name", "-"),
            r.get("cap_raw_pts", 0),
            f"{(r.get('cap_vs_field') or 0):+.1f}",
            r.get("chip", "none"),
            f"{r.get('transfer_count',0)}/{r.get('hit_cost',0)}/{r.get('net_transfer_gain',0)}",
            f"{r.get('bench_points',0)}/{r.get('bench_waste',0)}",
            r.get("formation", ""),
            r.get("premium_count", 0),
            r.get("template_overlap", 0),
            r.get("differentials_started", 0),
            r.get("leverage_points", 0),
            r.get("flagged_starters", 0),
            r.get("squad_value_m", 0.0),
            r.get("bank_m", 0.0),
        ])
    md.append(md_table(mgr_rows, [
        "Manager", "Team", "Pts", "±Mean", "GW Rank", "OR", "ΔOR",
        "Captain", "Cap pts", "Cap vs fld", "Chip", "Transfers (cnt/hit/net)",
        "Bench (pts/waste)", "Form", "Prem", "Tpl XI", "Diffs", "Leverage",
        "Flags", "Value", "Bank"
    ]))

    # Optional: Your manager spotlight
    if me_entry_id is not None:
        me = next((r for r in per_manager_rows if r["entry"] == me_entry_id), None)
        if me:
            # Build leverage contributions list
            starters = [p for p in entry_to_picks.get(me_entry_id, []) if p.get("multiplier",0) > 0]
            contribs = []
            for p in starters:
                pid = p.get("element")
                pts = live_points.get(pid, 0)
                sr = start_rate.get(pid, 0.0)
                contribs.append((player_name.get(pid, pid), player_team.get(pid, ""), pts, round((1-sr)*pts,2)))
            contribs.sort(key=lambda x: x[3], reverse=True)
            top5 = contribs[:5]

            md.append("\n## Your manager spotlight\n")
            md.append(f"- **Points vs mean:** {me.get('points_vs_mean',0):+.1f}  |  **Cap vs field:** {me.get('cap_vs_field',0):+.1f}  |  **Net transfer gain:** {me.get('net_transfer_gain',0)}\n")
            md.append(f"- **Template overlap:** {me.get('template_overlap',0)}/11  |  **Differentials started:** {me.get('differentials_started',0)}  |  **Leverage points:** {me.get('leverage_points',0)}\n")
            md.append(f"- **Bench waste:** {me.get('bench_waste',0)}  |  **Flagged starters:** {me.get('flagged_starters',0)}  |  **Value/Bank:** £{me.get('squad_value_m',0):.1f}m / £{me.get('bank_m',0):.1f}m\n")
            if top5:
                md.append("\n**Top leverage contributions this GW** (player, team, GW pts, leverage pts):\n")
                md.append("\n".join([f"- {n} ({t}) — {p} pts, leverage {l}" for n,t,p,l in top5]))
            md.append("\n")

    md.append("## Flagged starters exposure\n")
    md.append(md_table(flagged_rows, ["Player", "Team", "Flag", "Start %", "GW pts"]))

    out_md = os.path.join(OUTPUT_DIR, f"gw{gw}.md")
    write_text(out_md, "\n".join(md))

    # Rolling CSV per manager — overwrite mode with schema handling & de-dup by (gw,entry)
    out_csv = os.path.join(OUTPUT_DIR, "rolling.csv")

    # Prepare new rows for this run
    new_clean = []
    new_keys = set()
    for row in per_manager_rows:
        clean = {k: row.get(k, None) for k in CSV_FIELDNAMES}
        # normalise numeric
        try: clean["gw"] = int(clean.get("gw"))
        except Exception: pass
        try: clean["entry"] = int(clean.get("entry"))
        except Exception: pass
        new_keys.add((clean.get("gw"), clean.get("entry")))
        new_clean.append(clean)

    combined = []
    header_matches = False
    if os.path.exists(out_csv):
        try:
            with open(out_csv, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                existing_fieldnames = reader.fieldnames
                header_matches = (existing_fieldnames == CSV_FIELDNAMES)
                if header_matches:
                    for r in reader:
                        try:
                            gwi = int(r.get("gw"))
                            ent = int(r.get("entry"))
                        except Exception:
                            continue
                        if (gwi, ent) in new_keys:
                            continue  # drop old row for this gw/entry; we will replace with new
                        keep = {k: r.get(k, None) for k in CSV_FIELDNAMES}
                        try: keep["gw"] = int(keep.get("gw"))
                        except Exception: pass
                        try: keep["entry"] = int(keep.get("entry"))
                        except Exception: pass
                        combined.append(keep)
                else:
                    # Backup legacy schema and rebuild afresh
                    backup = out_csv.replace(".csv", f".legacy_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv")
                    try:
                        os.replace(out_csv, backup)
                        print(f"ℹ️ Detected legacy rolling.csv schema. Backed up to {backup} and rebuilt with new schema.")
                    except Exception:
                        pass
        except Exception:
            pass

    combined.extend(new_clean)

    # De-duplicate by key, prefer latest
    dedup = {}
    for r in combined:
        key = (r.get("gw"), r.get("entry"))
        dedup[key] = r
    final_rows = list(dedup.values())
    final_rows.sort(key=lambda r: (int(r.get("gw") or 0), int(r.get("entry") or 0)))

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        w.writeheader()
        for r in final_rows:
            w.writerow({k: r.get(k, None) for k in CSV_FIELDNAMES})

    print(f"✅ Report written: {out_md}")
    print(f"📈 Rolling CSV updated: {out_csv}")


def push_to_github(commit_message: str):
    os.system("git add .")
    os.system(f"git commit -m \"{commit_message}\" || true")
    os.system("git push origin main")


def main():
    ap = argparse.ArgumentParser(description="Generate a weekly mini-league GW report")
    ap.add_argument("--gw", type=int, required=True, help="Gameweek number")
    ap.add_argument("--me", type=int, default=None, help="Your entry id (optional)")
    ap.add_argument("--push", action="store_true", help="git add/commit/push after writing")
    args = ap.parse_args()

    generate_report(args.gw, me_entry_id=args.me, push=args.push)
    if args.push:
        push_to_github(f"Mini-league report for GW{args.gw}")


if __name__ == "__main__":
    main()