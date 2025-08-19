#!/usr/bin/env python3
"""
Top-20 Insights — top-analysis.py

Reads the data collected by top100.py (the top ~20 overall managers for a given GW)
and produces:
  • top_overall/reports/gw{N}.md          → field-wide insights (template XI, captaincy, diffs, structure)
  • top_overall/reports/gw{N}_will.md     → personalised analysis vs field for your entry
  • top_overall/reports/rolling_player_eo.csv  → per-player EO among top-20 by GW (dedup, overwrite style)
  • top_overall/reports/rolling_manager.csv    → per-manager summary by GW (dedup, overwrite style)

Assumptions about folder layout written by top100.py (robust to minor differences):
  top_overall/
    Gameweek_{N}/
      summary.json            (optional; if missing we glob team_*.json)
      team_{ENTRY}.json       (must include a GW picks object; see parsing below)

We also use your local bootstrap cache if available:
  mini_league/bootstrap_cache.json
…or we fetch fresh from FPL if missing.

Usage:
  python top-analysis.py --gw 2 --my-entry 25029 --base-dir top_overall --push

Notes:
- EO definition here is *league EO among top-20*: sum of multipliers (starter=1, C=2, TC=3, bench=0)/20.
- Template XI is the top 11 by start_rate (ties → higher EO).
- Premium is defined by now_cost ≥ £10.0m (>= 100 in tenths).
"""

import os
import asyncio
import re
import csv
import json
import glob
import argparse
from datetime import datetime
from collections import defaultdict, Counter
from statistics import mean

import requests

BASE_URL = "https://fantasy.premierleague.com/api/"
BOOTSTRAP_CACHE = os.path.join("mini_league", "bootstrap_cache.json")

# Try to load DISCORD_TOKEN / DISCORD_CHANNEL_ID from .env if available
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

# --------------------- IO helpers ---------------------

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def load_json(path, default=None):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default


def write_text(path, text: str):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def fetch_json(url, timeout=20):
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "top-analysis/1.0"})
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None

# --------------------- FPL helpers ---------------------

def load_bootstrap():
    data = load_json(BOOTSTRAP_CACHE)
    if not data:
        data = fetch_json(BASE_URL + "bootstrap-static/") or {}
    elements = {e.get("id"): e for e in data.get("elements", [])}
    teams = {t.get("id"): t for t in data.get("teams", [])}
    types = {et.get("id"): et for et in data.get("element_types", [])}

    player_name = {}
    player_team = {}
    player_pos = {}
    player_cost = {}
    for pid, e in elements.items():
        nm = f"{e.get('first_name','')} {e.get('second_name','')}".strip() or e.get("web_name", str(pid))
        player_name[pid] = nm
        player_team[pid] = teams.get(e.get("team"), {}).get("short_name", "")
        player_pos[pid] = types.get(e.get("element_type"), {}).get("singular_name_short", "")
        player_cost[pid] = e.get("now_cost")
    return {
        "elements": elements,
        "teams": teams,
        "types": types,
        "player_name": player_name,
        "player_team": player_team,
        "player_pos": player_pos,
        "player_cost": player_cost,
    }


def load_live_points(gw: int):
    live = fetch_json(BASE_URL + f"event/{gw}/live/") or {}
    pts = {}
    if isinstance(live.get("elements"), list):
        for e in live["elements"]:
            pid = e.get("id")
            stats = e.get("stats", {})
            pts[pid] = stats.get("total_points", 0)
    return pts

# --------------------- top100 parsing ---------------------

def discover_entry_files(base_dir: str, gw: int):
    gw_dir = os.path.join(base_dir, f"Gameweek_{gw}")
    files = sorted(glob.glob(os.path.join(gw_dir, "team_*.json")))
    entries = []
    for path in files:
        m = re.search(r"team_(\d+)\.json$", path)
        if m:
            entries.append({"entry": int(m.group(1)), "path": path})
    # If a summary exists, use its order/meta where possible
    summary = load_json(os.path.join(gw_dir, "summary.json"), default=None)
    return gw_dir, entries, summary


def extract_manager_record(team_json: dict):
    """Return dict with (entry, manager, team_name, gw_picks dict). Robust to layout variations."""
    info = team_json.get("info") or {}
    entry = info.get("entry") or team_json.get("entry")
    manager = info.get("player_first_name", "").strip()
    if info.get("player_last_name"):
        manager = (manager + " " + info.get("player_last_name")).strip()
    team_name = info.get("name") or team_json.get("team_name") or ""

    gw_picks = team_json.get("gw_picks") or team_json.get("picks") or {}
    # if gw_picks is a list, wrap into dict
    if isinstance(gw_picks, list):
        gw_picks = {"picks": gw_picks}

    return {
        "entry": entry,
        "manager": manager,
        "team_name": team_name,
        "gw": {
            "picks": gw_picks.get("picks", []),
            "entry_history": gw_picks.get("entry_history", {}),
            "active_chip": gw_picks.get("active_chip"),
            "automatic_subs": gw_picks.get("automatic_subs", []),
        }
    }

# --------------------- metrics ---------------------

def calc_formation(picks):
    g=d=m=f=0
    for p in picks:
        if (p.get("multiplier") or 0) > 0:
            et = p.get("element_type")
            if et == 1: g += 1
            elif et == 2: d += 1
            elif et == 3: m += 1
            elif et == 4: f += 1
    return f"{g}-{d}-{m}-{f}"


def count_premiums(picks, player_cost, threshold=100):
    cnt = 0
    for p in picks:
        pid = p.get("element")
        if isinstance(player_cost.get(pid), int) and player_cost[pid] >= threshold:
            cnt += 1
    return cnt


def compute_eo_and_rates(picks_by_manager, n):
    eo = defaultdict(float)
    start_cnt = Counter()
    cap_cnt = Counter()
    seen = set()
    for picks in picks_by_manager:
        owned = set()
        for p in picks:
            pid = p.get("element")
            mult = int(p.get("multiplier") or 0)
            eo[pid] += mult
            owned.add(pid)
            if mult > 0:
                start_cnt[pid] += 1
            if p.get("is_captain"):
                cap_cnt[pid] += 1
        seen |= owned
    n = max(n, 1)
    eo = {pid: v/float(n) for pid, v in eo.items()}
    start_rate = {pid: start_cnt[pid]/float(n) for pid in seen}
    cap_rate = {pid: cap_cnt[pid]/float(n) for pid in seen}
    return eo, start_rate, cap_rate

# --------------------- markdown utils ---------------------

def md_table(rows, headers):
    if not rows:
        return "\n"
    line = "| " + " | ".join(headers) + " |\n"
    sep = "|" + "|".join([" --- "] * len(headers)) + "|\n"
    body = "\n".join(["| " + " | ".join(map(str, r)) + " |" for r in rows]) + "\n"
    return line + sep + body

# --------------------- main generation ---------------------

def generate(base_dir: str, gw: int, my_entry: int = None, push: bool = False, discord: bool = False, discord_channel: int | None = None):
    bs = load_bootstrap()
    pname = bs["player_name"]; pteam = bs["player_team"]; ppos = bs["player_pos"]; pcost = bs["player_cost"]
    live_pts = load_live_points(gw)

    gw_dir, entry_files, summary = discover_entry_files(base_dir, gw)
    if not entry_files:
        print(f"❌ No team_*.json files found in {gw_dir}")
        return

    # Load records
    managers = []
    for rec in entry_files:
        data = load_json(rec["path"], default=None)
        if not data:
            continue
        mrec = extract_manager_record(data)
        mrec["path"] = rec["path"]
        # Fallback: if entry id is missing in JSON, derive it from the filename discovery
        if not mrec.get("entry"):
            mrec["entry"] = rec.get("entry")
        managers.append(mrec)

    # Build picks list per manager
    picks_list = []
    manager_rows = []
    cap_counter = Counter()
    for m in managers:
        picks = m["gw"]["picks"] or []
        picks_list.append(picks)
        # captain id & points
        cap_id = next((p.get("element") for p in picks if p.get("is_captain")), None)
        if cap_id: cap_counter.update([cap_id])
        formation = calc_formation(picks)
        prem = count_premiums(picks, pcost)
        eh = m["gw"].get("entry_history", {})
        manager_rows.append({
            "entry": m.get("entry"),
            "manager": m.get("manager") or "",
            "team_name": m.get("team_name") or "",
            "points": eh.get("points"),
            "rank": eh.get("rank"),
            "formation": formation,
            "premium_count": prem,
            "captain_id": cap_id,
            "captain_name": pname.get(cap_id, "-") if cap_id else "-",
            "chip": m["gw"].get("active_chip") or "none",
        })

    n = len(picks_list)
    eo, start_rate, cap_rate = compute_eo_and_rates(picks_list, n)

    # Template XI
    template_sorted = sorted(start_rate.items(), key=lambda kv: (kv[1], eo.get(kv[0],0)), reverse=True)
    template_xi = [pid for pid,_ in template_sorted[:11]]

    # Differentials (start < 20%) ranked by live points
    diffs = [(pid, sr, live_pts.get(pid, 0)) for pid, sr in start_rate.items() if sr < 0.20]
    diffs.sort(key=lambda x: x[2], reverse=True)

    # Captaincy map
    cap_rows = []
    total_caps = sum(cap_counter.values()) or 1
    for pid, cnt in cap_counter.most_common():
        cap_rows.append([pname.get(pid, pid), pteam.get(pid, ""), cnt, f"{cnt/total_caps*100:.1f}%", live_pts.get(pid,0)])

    # Prepare Discord summary (we'll fill personalised parts later if available)
    _discord_summary = None
    _my_exposure_for_summary = None
    _attackers_for_summary = None

    # -------- Outputs (markdown) --------
    reports_dir = os.path.join(base_dir, "reports")
    ensure_dir(reports_dir)

    # General field report
    rows_tpl = [[pname.get(pid,pid), pteam.get(pid,""), ppos.get(pid,""), f"{start_rate.get(pid,0)*100:.1f}%", f"{eo.get(pid,0):.2f}", live_pts.get(pid,0)] for pid in template_xi]
    rows_tpl = [[i+1]+r for i,r in enumerate(rows_tpl)]

    diff_rows = [[pname.get(pid,pid), pteam.get(pid,""), ppos.get(pid,""), f"{sr*100:.1f}%", live_pts.get(pid,0)] for pid,sr,_ in diffs[:15]]

    form_dist = Counter(r["formation"] for r in manager_rows)
    form_rows = [[form, cnt, f"{cnt/n*100:.1f}%"] for form,cnt in form_dist.most_common()]
    avg_prem = round(mean([r["premium_count"] for r in manager_rows]),2) if manager_rows else 0.0

    md = []
    md.append(f"# Top-20 Insights — GW{gw}\n")
    md.append(f"_Generated: {datetime.utcnow().isoformat()}Z — Managers: {n}_\n\n")

    md.append("## Template XI (Top-20)\n")
    md.append(md_table(rows_tpl, ["#","Player","Team","Pos","Start %","EO (mult)","GW pts"]))
    md.append("\n")

    md.append("## Captaincy Map\n")
    md.append(md_table(cap_rows, ["Captain","Team","Count","Share","GW pts"]))
    md.append("\n")

    md.append("## Differentials who returned (Start <20%)\n")
    md.append(md_table(diff_rows, ["Player","Team","Pos","Start %","GW pts"]))
    md.append("\n")

    md.append("## Formation & Structure\n")
    md.append(md_table(form_rows, ["Formation","Count","% managers"]))
    md.append(f"- **Avg premium count (≥£10.0m):** {avg_prem}\n")

    out_md = os.path.join(reports_dir, f"gw{gw}.md")
    write_text(out_md, "\n".join(md))

    # -------- Personalised (Will vs Field) --------
    if my_entry:
        # Try local picks first
        my_picks = load_json(os.path.join("mini_league","entries",str(my_entry),"picks",f"gw{gw}.json"), default=None)
        picks = []
        if my_picks and isinstance(my_picks.get("picks"), list):
            picks = my_picks["picks"]
        else:
            # Fallback to API
            j = fetch_json(BASE_URL + f"entry/{my_entry}/event/{gw}/picks/") or {}
            picks = j.get("picks", [])
        # Build exposure map for me
        my_exposure = defaultdict(int)  # 0,1,2,3
        my_cap = None
        for p in picks:
            pid = p.get("element")
            mult = int(p.get("multiplier") or 0)
            my_exposure[pid] = max(my_exposure[pid], mult)
            if p.get("is_captain"): my_cap = pid
        # Exposure gap & swing scenarios
        scenarios = [2,6,10,15]
        gap_rows = []
        for pid, field_eo in sorted(eo.items(), key=lambda kv: kv[1], reverse=True)[:40]:
            my_mult = my_exposure.get(pid, 0)
            gap = max(field_eo - my_mult, 0)
            swings = ", ".join([f"{int(s)}→{gap*int(s):.1f}" for s in scenarios])
            gap_rows.append([pname.get(pid,pid), pteam.get(pid,""), f"{field_eo:.2f}", my_mult, swings])
        # Blockers (high EO you don't start)
        blockers = [[pname.get(pid,pid), pteam.get(pid,""), f"EO {eo.get(pid,0):.2f}"] for pid in template_xi if my_exposure.get(pid,0) == 0]
        # Attackers (your low-field picks among starters)
        my_starters = [p.get("element") for p in picks if (p.get("multiplier") or 0) > 0]
        attackers = []
        for pid in my_starters:
            sr = start_rate.get(pid, 0.0)
            if sr < 0.20:
                attackers.append([pname.get(pid,pid), pteam.get(pid,""), f"start {sr*100:.1f}%", live_pts.get(pid,0)])
        attackers.sort(key=lambda r: r[-1], reverse=True)
        # Captain risk note
        field_cap = cap_counter.most_common(1)[0][0] if cap_counter else None
        field_cap_name = pname.get(field_cap, "-") if field_cap else "-"
        my_cap_name = pname.get(my_cap, "-") if my_cap else "-"

        _my_exposure_for_summary = dict(my_exposure)
        _attackers_for_summary = list(attackers)

        md_me = []
        md_me.append(f"# Your vs Field — GW{gw}\n")
        md_me.append(f"_Entry {my_entry}. Field likely captain: **{field_cap_name}**. Your captain: **{my_cap_name}**._\n\n")
        md_me.append("## Biggest EO gaps (risk if they haul)\n")
        md_me.append(md_table(gap_rows[:20], ["Player","Team","Field EO","Your mult","Swings (pts→swing)"]))
        md_me.append("\n")
        md_me.append("## Your attackers (low-field starters you own)\n")
        md_me.append(md_table(attackers[:15], ["Player","Team","Field start%","GW pts"]))
        md_me.append("\n")
        if blockers:
            md_me.append("## Blockers you don't own from Template XI\n")
            md_me.append("\n".join([f"- {n} ({t}) — {eo_str}" for n,t,eo_str in blockers]))
            md_me.append("\n")

        out_me = os.path.join(reports_dir, f"gw{gw}_will.md")
        write_text(out_me, "\n".join(md_me))

    # -------- Rolling CSVs --------
    ensure_dir(reports_dir)
    # Player EO rolling
    player_rows = []
    for pid in set(list(eo.keys()) + list(start_rate.keys())):
        player_rows.append({
            "gw": gw,
            "player_id": pid,
            "player_name": pname.get(pid, pid),
            "team": pteam.get(pid, ""),
            "pos": ppos.get(pid, ""),
            "eo": round(eo.get(pid,0), 4),
            "start_rate": round(start_rate.get(pid,0), 4),
            "cap_rate": round(cap_rate.get(pid,0), 4),
        })

    write_dedup_csv(
        os.path.join(reports_dir, "rolling_player_eo.csv"),
        ["gw","player_id","player_name","team","pos","eo","start_rate","cap_rate"],
        player_rows,
        key=lambda r: (int(r["gw"]), int(r["player_id"]))
    )

    # Manager rolling
    mgr_rows = []
    for r in manager_rows:
        mgr_rows.append({
            "gw": gw,
            "entry": r["entry"],
            "manager": r["manager"],
            "team_name": r["team_name"],
            "points": r.get("points"),
            "rank": r.get("rank"),
            "formation": r.get("formation"),
            "premium_count": r.get("premium_count"),
            "captain_id": r.get("captain_id"),
            "captain_name": r.get("captain_name"),
            "chip": r.get("chip"),
        })

    write_dedup_csv(
        os.path.join(reports_dir, "rolling_manager.csv"),
        ["gw","entry","manager","team_name","points","rank","formation","premium_count","captain_id","captain_name","chip"],
        mgr_rows,
        key=lambda r: (int(r["gw"]), int(r["entry"]))
    )

    print(f"✅ Wrote {out_md}")
    if my_entry:
        print(f"✅ Wrote {os.path.join(reports_dir, f'gw{gw}_will.md')}")
    print(f"📈 Updated rolling CSVs in {reports_dir}")

    # Optional Discord post
    if discord:
        ch_id = discord_channel or os.getenv("DISCORD_CHANNEL_ID")
        if ch_id:
            try:
                ch_id_int = int(ch_id)
            except Exception:
                print(f"❌ Invalid DISCORD_CHANNEL_ID: {ch_id}")
                ch_id_int = None
        else:
            print("ℹ️ No Discord channel id provided; set --discord-channel or DISCORD_CHANNEL_ID.")
            ch_id_int = None

        if ch_id_int is not None:
            _discord_summary = build_discord_summary(
                gw, n, template_xi, pname, pteam, cap_counter, eo, start_rate,
                my_entry=my_entry,
                my_exposure=_my_exposure_for_summary,
                attackers=_attackers_for_summary,
            )
            post_to_discord(_discord_summary, ch_id_int)
# --------------------- Discord helpers ---------------------

def build_discord_summary(gw:int, n:int, template_xi, pname, pteam, cap_counter, eo:dict, start_rate:dict,
                          my_entry:int=None, my_exposure:dict=None, attackers:list=None):
    # Template XI names
    tpl_names = ", ".join([pname.get(pid, str(pid)) for pid in template_xi])
    # Captaincy top 3
    caps = []
    total = sum(cap_counter.values()) or 1
    for pid, cnt in cap_counter.most_common(3):
        caps.append(f"{pname.get(pid, pid)} {cnt/total*100:.0f}%")
    caps_str = ", ".join(caps) if caps else "-"

    lines = [
        f"**Top-20 Insights — GW{gw}**",
        f"Managers: {n}",
        f"Template XI: {tpl_names}",
        f"Captaincy: {caps_str}",
    ]

    # Personalised bits if available
    if my_entry and my_exposure is not None:
        gaps = []
        for pid, field_eo in eo.items():
            my_mult = int(my_exposure.get(pid, 0))
            gap = field_eo - my_mult
            if gap > 0:
                gaps.append((gap, pid, field_eo, my_mult))
        gaps.sort(reverse=True)
        topg = gaps[:3]
        if topg:
            gparts = []
            for gap, pid, field_eo, my_mult in topg:
                gparts.append(f"{pname.get(pid, pid)} (EO {field_eo:.2f} vs you {my_mult}; 10pt swing {gap*10:.1f})")
            lines.append("Your biggest EO gaps: " + "; ".join(gparts))
        if attackers:
            atk = [f"{a[0]} ({a[2]})" for a in attackers[:3]]
            if atk:
                lines.append("Your attackers: " + ", ".join(atk))

    msg = "\n".join(lines)
    if len(msg) > 1900:
        msg = msg[:1890] + "…"
    return msg


def post_to_discord(message: str, channel_id: int):
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        print("ℹ️ DISCORD_TOKEN not set; skipping Discord post.")
        return
    try:
        import discord  # type: ignore
    except Exception:
        print("ℹ️ discord.py not installed; skipping Discord post.")
        return

    intents = discord.Intents.default()
    intents.messages = True
    intents.guilds = True

    async def _run():
        client = discord.Client(intents=intents)
        @client.event
        async def on_ready():
            try:
                ch = client.get_channel(int(channel_id))
                if ch is None:
                    print(f"❌ Discord channel {channel_id} not found.")
                else:
                    await ch.send(message)
            finally:
                await client.close()
        await client.start(token)

    try:
        asyncio.run(_run())
    except RuntimeError:
        # In case there's already an event loop (rare in CLI), nest
        loop = asyncio.get_event_loop()
        loop.create_task(_run())


def write_dedup_csv(path: str, fieldnames, new_rows, key):
    """Overwrite CSV with de-duplication by key; skip rows with missing key parts."""
    existing = []
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    existing.append(r)
        except Exception:
            pass

    # Build set of keys for new rows, skipping invalid keys (e.g., entry=None)
    valid_new_rows = []
    new_keys = set()
    for r in new_rows:
        try:
            k = key(r)
            if isinstance(k, tuple) and any(v is None for v in k):
                continue
            new_keys.add(k)
            valid_new_rows.append(r)
        except Exception:
            continue

    # Keep only old rows whose key isn't replaced by a new row
    kept = []
    for r in existing:
        try:
            k = key(r)
            if isinstance(k, tuple) and any(v is None for v in k):
                continue
            if k in new_keys:
                continue
        except Exception:
            # if old schema or broken row, drop it
            continue
        kept.append(r)

    final = kept + valid_new_rows

    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in final:
            out = {k: r.get(k, None) for k in fieldnames}
            w.writerow(out)


def push_to_github(commit_message: str):
    os.system("git add .")
    os.system(f"git commit -m \"{commit_message}\" || true")
    os.system("git push origin main")


def main():
    ap = argparse.ArgumentParser(description="Top-20 insights from top100.py outputs")
    ap.add_argument("--gw", type=int, required=True, help="Gameweek number")
    ap.add_argument("--base-dir", default="top_overall", help="Base directory of top100.py outputs")
    ap.add_argument("--my-entry", type=int, default=25029, help="Your entry id for personalised report")
    ap.add_argument("--push", action="store_true", help="git add/commit/push after writing")
    ap.add_argument("--discord", action="store_true", help="Post a short summary to Discord")
    ap.add_argument("--discord-channel", type=int, default=None, help="Discord channel id (else use DISCORD_CHANNEL_ID env)")
    args = ap.parse_args()

    generate(args.base_dir, args.gw, my_entry=args.my_entry, push=args.push, discord=args.discord, discord_channel=args.discord_channel)
    if args.push:
        push_to_github(f"Top-20 insights for GW{args.gw}")


if __name__ == "__main__":
    main()
