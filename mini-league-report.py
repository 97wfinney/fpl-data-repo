#!/usr/bin/env python3
"""
mini-league-report.py

Generates a weekly Gameweek (GW) report for your mini-league from the data collected by
mini-league-analysis.py, and writes:
  - reports/gw{GW}.md    : human-readable league report
  - reports/rolling.csv  : per-manager per-GW metrics (appended)

Key metrics included (single GW):
- League summary (min/mean/median/max, spread), optional rank change for --me
- Captaincy distribution and average captain points
- Effective Ownership (EO) within the league
- Differentials (<20% starters) who returned points
- Transfers & hits (percent with hits, avg hit cost)
- Chip usage counts
- Bench waste & autosubs frequency
- Formation prevalence and premium count (>£10.0m current price)
- Top 11 by GW points vs league ownership
- Red/flagged starters exposure

Usage examples:
    python mini-league-report.py --gw 1
    python mini-league-report.py --gw 2 --me 25029 --push

Assumptions:
- Your saved files live under mini_league/entries/{entry_id}/picks/gw{GW}.json
- Those picks files may be enriched with name/team/pos fields (recommended)
- bootstrap cache exists (mini_league/bootstrap_cache.json) or we fetch live
"""

import os
import csv
import json
import math
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
    # entries: list of {entry, entry_name, player_name}
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

    per_manager_rows = []  # for CSV

    # For differentials
    player_returns = defaultdict(int)  # pid -> points this GW

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

        # points/rank from entry_history in picks file (single GW view)
        eh = pdata.get("entry_history", {})
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
        for p in picks:
            pid = p.get("element")
            mult = p.get("multiplier", 0)
            all_pids_seen.add(pid)
            if p.get("is_captain"):
                cap_id = pid
            if p.get("is_vice_captain"):
                vc_id = pid
            # collect player returns for differentials (raw points)
            player_returns[pid] = live_points.get(pid, 0)

        if cap_id:
            capt_counter.update([cap_id])
        if vc_id:
            vc_counter.update([vc_id])

        # formation and premiums
        g, d, m, f = calc_formation(picks)
        formations.update([f"{g}-{d}-{m}-{f}"])
        premium_counts.append(count_premiums(picks, player_cost))

        autosubs = len(pdata.get("automatic_subs", []))
        autosubs_count += autosubs

        # captain points (raw total_points of captain)
        cap_pts = live_points.get(cap_id, 0) if cap_id else 0

        per_manager_rows.append({
            "gw": gw,
            "entry": entry_id,
            "entry_name": entry_name,
            "manager": manager,
            "points": gw_points,
            "rank": gw_rank,
            "captain_id": cap_id,
            "captain_name": player_name.get(cap_id, "-") if cap_id else "-",
            "captain_points": cap_pts,
            "bench_points": eh.get("points_on_bench", 0),
            "hit_cost": hit or 0,
            "chip": pdata.get("active_chip") or "none",
            "formation": f"{g}-{d}-{m}-{f}",
            "premium_count": premium_counts[-1],
            "autosubs": autosubs,
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
        cap_pts_list = [r["captain_points"] for r in per_manager_rows]
        avg_cap_points = round(mean(cap_pts_list), 2)

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

    # Transfers & hits
    pct_hits = (len(hit_costs) / float(n_entries)) * 100 if n_entries else 0.0
    avg_hit_cost = round(mean(hit_costs), 2) if hit_costs else 0.0

    # Chips usage
    chip_rows = []
    for chip, cnt in chips_counter.most_common():
        chip_rows.append([chip, cnt, f"{(cnt/float(n_entries or 1))*100:.1f}%"]) 

    # Bench waste & autosubs
    avg_bench = round(mean(bench_points), 2) if bench_points else 0.0

    # Formation & structure
    formation_rows = [[form, cnt, f"{(cnt/float(n_entries or 1))*100:.1f}%"] for form, cnt in formations.most_common()]
    avg_premiums = round(mean(premium_counts), 2) if premium_counts else 0.0

    # Top 11 by points vs ownership
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

    # Flagged starters exposure
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
            md.append(f"- **Your GW points:** {me_row['points']} — formation {me_row['formation']} — captain {me_row['captain_name']} ({me_row['captain_points']} pts)\n")
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

    md.append("## Chip Usage\n")
    md.append(md_table(chip_rows, ["Chip", "Count", "% of league"]))
    md.append("\n")

    md.append("## Bench & Autosubs\n")
    md.append(f"- **Avg bench points:** {avg_bench}  —  **Total autosubs:** {autosubs_count}\n\n")

    md.append("## Formation & Structure\n")
    md.append(md_table(formation_rows, ["Formation", "Count", "%"]))
    md.append(f"- **Avg premium count (>£10.0m):** {avg_premiums}\n\n")

    md.append("## Top 11 by GW points (vs league ownership)\n")
    md.append(md_table(top_rows, ["Player", "Team", "GW pts", "Own %", "Start %"]))
    md.append("\n")

    md.append("## Flagged starters exposure\n")
    md.append(md_table(flagged_rows, ["Player", "Team", "Flag", "Start %", "GW pts"]))

    out_md = os.path.join(OUTPUT_DIR, f"gw{gw}.md")
    write_text(out_md, "\n".join(md))

    # Rolling CSV per manager
    out_csv = os.path.join(OUTPUT_DIR, "rolling.csv")
    write_header = not os.path.exists(out_csv)
    with open(out_csv, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "gw", "entry", "entry_name", "manager", "points", "rank",
            "captain_id", "captain_name", "captain_points", "bench_points",
            "hit_cost", "chip", "formation", "premium_count", "autosubs",
        ])
        if write_header:
            w.writeheader()
        for row in per_manager_rows:
            w.writerow(row)

    print(f"✅ Report written: {out_md}")
    print(f"📈 Rolling CSV updated: {out_csv}")


def push_to_github(commit_message: str):
    # Lightweight git wrapper; repository root assumed as cwd
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
