#!/usr/bin/env python3
"""
Mini-League Season Diary

Produces a week-by-week account of each manager's decisions:
- Starting XI with points
- Bench with points
- Captain/Vice Captain
- Transfers in/out
- Chip used
- Rank movement

Optional: Use OpenAI API to generate entertaining analysis per GW.

Usage:
    python mini_league_diary.py                     # All teams, all GWs
    python mini_league_diary.py --team 25029        # Single team diary
    python mini_league_diary.py --gw 22             # All teams, single GW
    python mini_league_diary.py --team 25029 --gw 22  # Single team, single GW

AI:
    python mini_league_diary.py --team 25029 --gw 22 --ai
    python mini_league_diary.py --all --gw 22 --ai
"""

import os
import json
import argparse
from typing import Optional, Dict, List, Any

# ============================================================================
# CONFIGURATION
# ============================================================================

CONFIG = {
    "league_dir": "/home/wfinney/Desktop/fpl-data-repo/mini_league",
    "entries_index": "/home/wfinney/Desktop/fpl-data-repo/mini_league/entries_index.json",
    "bootstrap_cache": "/home/wfinney/Desktop/fpl-data-repo/mini_league/bootstrap_cache.json",
    "bootstrap_folder": "/home/wfinney/Desktop/fpl-data-repo/25",
    "my_entry_id": 25029,
}
# Terminal formatting
class Colors:
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    DIM = '\033[2m'
    END = '\033[0m'

def c(text, color):
    return f"{color}{text}{Colors.END}"

# ============================================================================
# DATA LOADING
# ============================================================================

GW_POINTS_CACHE: Dict[int, Dict[int, int]] = {}

def load_json(path, default=None):
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return default

def load_gw_points(gw: int) -> Dict[int, int]:
    """Load player points for a gameweek from saved bootstrap snapshot."""
    global GW_POINTS_CACHE

    if gw in GW_POINTS_CACHE:
        return GW_POINTS_CACHE[gw]

    snapshot_path = os.path.join(CONFIG["bootstrap_folder"], f"Gameweek_{gw}.json")
    if not os.path.exists(snapshot_path):
        print(c(f"    Warning: No snapshot for GW{gw} at {snapshot_path}", Colors.YELLOW))
        GW_POINTS_CACHE[gw] = {}
        return {}

    data = load_json(snapshot_path, {})
    pts: Dict[int, int] = {}
    for element in data.get("elements", []):
        pid = element.get("id")
        pts[pid] = element.get("event_points", 0)

    GW_POINTS_CACHE[gw] = pts
    return pts

def load_bootstrap():
    """Load player lookup from bootstrap cache."""
    data = load_json(CONFIG["bootstrap_cache"], {})

    elements = {e.get("id"): e for e in data.get("elements", [])}
    teams = {t.get("id"): t for t in data.get("teams", [])}
    types = {et.get("id"): et for et in data.get("element_types", [])}

    player_lookup = {}
    for pid, e in elements.items():
        player_lookup[pid] = {
            "name": e.get("web_name", f"Player {pid}"),
            "team": teams.get(e.get("team"), {}).get("short_name", ""),
            "position": types.get(e.get("element_type"), {}).get("singular_name_short", ""),
            "price": e.get("now_cost", 0) / 10,
        }

    return player_lookup

def load_entries_index():
    idx = load_json(CONFIG["entries_index"], {"entries": []})
    return idx.get("entries", [])

def load_entry_data(entry_id: int) -> dict:
    base = os.path.join(CONFIG["league_dir"], "entries", str(entry_id))
    return {
        "entry": load_json(os.path.join(base, "entry.json"), {}),
        "history": load_json(os.path.join(base, "history.json"), {}),
        "transfers": load_json(os.path.join(base, "transfers.json"), []),
    }

def load_picks(entry_id: int, gw: int) -> dict:
    path = os.path.join(CONFIG["league_dir"], "entries", str(entry_id), "picks", f"gw{gw}.json")
    return load_json(path, {})

def get_available_gameweeks(entry_id: int) -> List[int]:
    picks_dir = os.path.join(CONFIG["league_dir"], "entries", str(entry_id), "picks")
    if not os.path.exists(picks_dir):
        return []

    gws = []
    for f in os.listdir(picks_dir):
        if f.startswith("gw") and f.endswith(".json"):
            try:
                gw = int(f[2:-5])
                gws.append(gw)
            except:
                pass
    return sorted(gws)

# ============================================================================
# OPENAI (OPTIONAL)
# ============================================================================

def safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def _ai_cache_path(cache_dir: str, entry_id: int, gw: int) -> str:
    return os.path.join(cache_dir, f"entry_{entry_id}_gw_{gw}.txt")

def build_ai_payload(
    entry_id: int,
    gw: int,
    entry_meta: dict,
    points: int,
    total_points: int,
    overall_rank: int,
    gw_rank: int,
    team_value: float,
    bank: float,
    bench_pts: int,
    hits: int,
    chip: Optional[str],
    starters: List[dict],
    bench: List[dict],
    transfers: List[dict],
    auto_subs: List[dict],
) -> dict:
    """Keep the payload compact so token/cost stays sane."""
    team_name = entry_meta.get("entry_name", "Unknown")
    manager = entry_meta.get("player_name", "Unknown")

    def slim_player(p: dict) -> dict:
        return {
            "pos": p.get("position"),
            "name": p.get("name"),
            "team": p.get("team"),
            "pts": p.get("points"),
            "mult": p.get("multiplier", 1),
            "C": bool(p.get("is_captain")),
            "V": bool(p.get("is_vice")),
        }

    def slim_transfer(t: dict) -> dict:
        return {
            "out": t.get("out_name"),
            "in": t.get("in_name"),
            "out_pts": t.get("out_pts"),
            "in_pts": t.get("in_pts"),
        }

    def slim_autosub(s: dict) -> dict:
        return {
            "out": s.get("out_name"),
            "in": s.get("in_name"),
            "in_pts": s.get("in_pts"),
        }

    return {
        "entry_id": entry_id,
        "team_name": team_name,
        "manager": manager,
        "gw": gw,
        "summary": {
            "points": points,
            "total_points": total_points,
            "overall_rank": overall_rank,
            "gw_rank": gw_rank,
            "value_m": round(team_value, 1),
            "bank_m": round(bank, 1),
            "bench_points": bench_pts,
            "hits": hits,
            "chip": chip,
        },
        "starting_xi": [slim_player(p) for p in starters],
        "bench": [slim_player(p) for p in bench],
        "transfers": [slim_transfer(t) for t in transfers],
        "auto_subs": [slim_autosub(s) for s in auto_subs],
    }

def get_ai_analysis(
    client: Any,
    model: str,
    payload: dict,
    cache_dir: str,
    use_cache: bool = True,
) -> str:
    safe_mkdir(cache_dir)
    cache_path = _ai_cache_path(cache_dir, payload["entry_id"], payload["gw"])

    if use_cache and os.path.exists(cache_path):
        with open(cache_path, "r") as f:
            return f.read().strip()

    system_style = (
        "You are an FPL mini-league analyst. "
        "Write a punchy, funny-but-not-cruel analysis. "
        "Focus on: captaincy, bench pain, transfers, chip impact, and rank movement. "
        "Keep it concise: 90–140 words. UK English. No tables."
    )

    user_msg = (
        "Analyse this one manager's gameweek and write a short paragraph:\n"
        f"{json.dumps(payload, ensure_ascii=False)}"
    )

    resp = client.responses.create(
        model=model,
        input=[
            {"role": "system", "content": system_style},
            {"role": "user", "content": user_msg},
        ],
    )
    text = (getattr(resp, "output_text", "") or "").strip()

    if use_cache and text:
        with open(cache_path, "w") as f:
            f.write(text)

    return text or "(No analysis returned.)"

# ============================================================================
# GAMEWEEK DIARY FOR ONE TEAM
# ============================================================================

def print_gw_diary(
    entry_id: int,
    gw: int,
    entry_meta: dict,
    entry_data: dict,
    players: dict,
    prev_gw_picks: Optional[dict] = None,
    ai_client: Any = None,
    ai_model: str = "gpt-5.2",
    ai_cache_dir: str = "mini_league/ai_cache",
    use_ai_cache: bool = True,
):
    """Print detailed diary entry for one team's gameweek."""

    picks_data = load_picks(entry_id, gw)
    if not picks_data:
        print(c(f"    No data for GW{gw}", Colors.DIM))
        return picks_data

    picks = picks_data.get("picks", [])
    entry_history = picks_data.get("entry_history", {})
    chip = picks_data.get("active_chip")
    auto_subs = picks_data.get("automatic_subs", [])

    # Get history data for this GW
    history = entry_data.get("history", {}).get("current", [])
    gw_history = next((h for h in history if h.get("event") == gw), {})

    # Get transfers for this GW
    all_transfers = entry_data.get("transfers", [])
    gw_transfers = [t for t in all_transfers if t.get("event") == gw]

    # Live points for this GW (from snapshot)
    gw_pts = load_gw_points(gw)

    # Points and rank
    points = gw_history.get("points", entry_history.get("points", 0))
    total_points = gw_history.get("total_points", entry_history.get("total_points", 0))
    overall_rank = gw_history.get("overall_rank", entry_history.get("overall_rank", 0))
    gw_rank = gw_history.get("rank", entry_history.get("rank", 0))
    bench_pts = gw_history.get("points_on_bench", entry_history.get("points_on_bench", 0))
    hits = gw_history.get("event_transfers_cost", entry_history.get("event_transfers_cost", 0))
    team_value = entry_history.get("value", 0) / 10
    bank = entry_history.get("bank", 0) / 10

    # Header
    chip_str = f" [{chip.upper()}]" if chip else ""
    hit_str = f" {c(f'(-{hits} hit)', Colors.RED)}" if hits > 0 else ""

    print(f"\n  {c(f'GAMEWEEK {gw}', Colors.BOLD + Colors.CYAN)}{chip_str}{hit_str}")
    print(f"  {c('─' * 50, Colors.DIM)}")
    print(f"  Points: {c(str(points), Colors.GREEN if points >= 50 else Colors.YELLOW if points >= 40 else Colors.RED)}  │  Total: {total_points}  │  Rank: {overall_rank:,}  │  GW Rank: {gw_rank:,}")
    print(f"  Value: £{team_value:.1f}m  │  Bank: £{bank:.1f}m")

    # Transfers (always show)
    transfer_summaries: List[dict] = []
    if gw_transfers:
        print(f"\n  {c('Transfers:', Colors.YELLOW)}")
        for t in gw_transfers:
            p_in = t.get("element_in")
            p_out = t.get("element_out")
            in_name = players.get(p_in, {}).get("name", f"ID:{p_in}")
            out_name = players.get(p_out, {}).get("name", f"ID:{p_out}")
            in_cost = t.get("element_in_cost", 0) / 10
            out_cost = t.get("element_out_cost", 0) / 10
            in_pts = gw_pts.get(p_in, 0)
            out_pts = gw_pts.get(p_out, 0)
            print(f"    {c('OUT:', Colors.RED)} {out_name} (£{out_cost:.1f}m, {out_pts}pts)  →  {c('IN:', Colors.GREEN)} {in_name} (£{in_cost:.1f}m, {in_pts}pts)")
            transfer_summaries.append({
                "out_name": out_name,
                "in_name": in_name,
                "out_pts": out_pts,
                "in_pts": in_pts,
            })
    else:
        print(f"\n  {c('Transfers: None', Colors.DIM)}")

    # Build squad display
    starters: List[dict] = []
    bench_players: List[dict] = []

    for p in picks:
        pid = p.get("element")
        pos = p.get("position", 0)
        mult = p.get("multiplier", 0)
        is_cap = p.get("is_captain", False)
        is_vice = p.get("is_vice_captain", False)

        player_info = players.get(pid, {})
        name = p.get("web_name") or p.get("name") or player_info.get("name", f"ID:{pid}")
        team = p.get("team") or player_info.get("team", "")
        position = p.get("pos") or player_info.get("position", "")

        pts = gw_pts.get(pid, 0)

        player_entry = {
            "pid": pid,
            "name": name,
            "team": team,
            "position": position,
            "points": pts,
            "multiplier": mult,
            "is_captain": is_cap,
            "is_vice": is_vice,
            "bench_pos": pos if mult == 0 else 0,
        }

        if mult > 0:
            starters.append(player_entry)
        else:
            bench_players.append(player_entry)

    pos_order = {"GKP": 1, "DEF": 2, "MID": 3, "FWD": 4}
    starters.sort(key=lambda x: pos_order.get(x["position"], 5))
    bench_players.sort(key=lambda x: x["bench_pos"])

    # Print Starting XI
    print(f"\n  {c('Starting XI:', Colors.GREEN)}")
    for p in starters:
        cap_str = " (C)" if p["is_captain"] else " (V)" if p["is_vice"] else ""
        raw_pts = p["points"]
        actual_pts = raw_pts * p["multiplier"] if p["is_captain"] else raw_pts

        pts_color = Colors.GREEN if raw_pts >= 6 else Colors.YELLOW if raw_pts >= 3 else Colors.RED if raw_pts <= 1 else Colors.END
        mult_str = f" (×{p['multiplier']}={actual_pts})" if p['multiplier'] > 1 else ""
        print(f"    {p['position']:3} {p['name']:15} {p['team']:4}{cap_str:4}  {c(str(raw_pts), pts_color):>3}pts{mult_str}")

    # Print Bench
    print(f"\n  {c('Bench:', Colors.DIM)} ({bench_pts}pts)")
    for p in bench_players:
        print(f"    {p['position']:3} {p['name']:15} {p['team']:4}       {p['points']:>2}pts")

    # Auto subs
    autosub_summaries: List[dict] = []
    if auto_subs:
        print(f"\n  {c('Auto-subs:', Colors.YELLOW)}")
        for sub in auto_subs:
            p_in = sub.get("element_in")
            p_out = sub.get("element_out")
            in_name = players.get(p_in, {}).get("name", f"ID:{p_in}")
            out_name = players.get(p_out, {}).get("name", f"ID:{p_out}")
            in_pts = gw_pts.get(p_in, 0)
            print(f"    {out_name} → {in_name} (+{in_pts}pts)")
            autosub_summaries.append({
                "out_name": out_name,
                "in_name": in_name,
                "in_pts": in_pts,
            })

    # Note for Free Hit
    if chip == "freehit" and prev_gw_picks:
        print(f"\n  {c('Note:', Colors.DIM)} Free Hit used - team reverted after this GW")

    # AI analysis (optional)
    if ai_client is not None:
        try:
            payload = build_ai_payload(
                entry_id=entry_id,
                gw=gw,
                entry_meta=entry_meta,
                points=points,
                total_points=total_points,
                overall_rank=overall_rank,
                gw_rank=gw_rank,
                team_value=team_value,
                bank=bank,
                bench_pts=bench_pts,
                hits=hits,
                chip=chip,
                starters=starters,
                bench=bench_players,
                transfers=transfer_summaries,
                auto_subs=autosub_summaries,
            )
            analysis = get_ai_analysis(
                client=ai_client,
                model=ai_model,
                payload=payload,
                cache_dir=ai_cache_dir,
                use_cache=use_ai_cache,
            )
            print(f"\n  {c('AI analysis:', Colors.CYAN)}")
            print(f"    {analysis}")
        except Exception as e:
            print(c(f"\n  AI analysis failed: {e}", Colors.RED))

    return picks_data

def print_team_diary(
    entry_id: int,
    entry_meta: dict,
    players: dict,
    gw_filter: Optional[int] = None,
    ai_client: Any = None,
    ai_model: str = "gpt-5.2",
    ai_cache_dir: str = "mini_league/ai_cache",
    use_ai_cache: bool = True,
):
    """Print full season diary for one team."""

    team_name = entry_meta.get("entry_name", "Unknown")
    manager = entry_meta.get("player_name", "Unknown")

    print(f"\n{c('=' * 60, Colors.CYAN)}")
    print(c(f" {team_name.upper()} ", Colors.BOLD + Colors.CYAN).center(70))
    print(c(f" Manager: {manager} │ Entry: {entry_id} ", Colors.DIM).center(60))
    print(c('=' * 60, Colors.CYAN))

    entry_data = load_entry_data(entry_id)
    available_gws = get_available_gameweeks(entry_id)

    if not available_gws:
        print(c("  No gameweek data available", Colors.RED))
        return

    if gw_filter:
        gws_to_show = [gw_filter] if gw_filter in available_gws else []
    else:
        gws_to_show = available_gws

    prev_picks = None
    for gw in gws_to_show:
        picks = print_gw_diary(
            entry_id=entry_id,
            gw=gw,
            entry_meta=entry_meta,
            entry_data=entry_data,
            players=players,
            prev_gw_picks=prev_picks,
            ai_client=ai_client,
            ai_model=ai_model,
            ai_cache_dir=ai_cache_dir,
            use_ai_cache=use_ai_cache,
        )
        prev_picks = picks
        print()

# ============================================================================
# MAIN
# ============================================================================

def interactive_menu(entries: list, players: dict):
    """Show interactive menu to select a manager."""
    print(f"\n{c('Select a manager:', Colors.BOLD)}\n")

    sorted_entries = sorted(entries, key=lambda x: x.get("entry_name", "").lower())

    for i, entry in enumerate(sorted_entries, 1):
        name = entry.get("entry_name", "Unknown")
        manager = entry.get("player_name", "Unknown")
        entry_id = entry.get("entry")

        if entry_id == CONFIG["my_entry_id"]:
            print(f"  {c(f'{i:2}', Colors.CYAN)}. {c(name[:25], Colors.CYAN + Colors.BOLD):25} ({manager})")
        else:
            print(f"  {i:2}. {name[:25]:25} ({manager})")

    print(f"\n  {c(' 0', Colors.DIM)}. Exit")
    print()

    while True:
        try:
            choice = input(c("Enter number: ", Colors.YELLOW))

            if choice.strip() == "0" or choice.strip().lower() == "q":
                print(c("Goodbye!", Colors.DIM))
                return None

            num = int(choice)
            if 1 <= num <= len(sorted_entries):
                return sorted_entries[num - 1]
            else:
                print(c(f"Please enter 1-{len(sorted_entries)} or 0 to exit", Colors.RED))
        except ValueError:
            print(c("Please enter a number", Colors.RED))
        except KeyboardInterrupt:
            print(c("\nGoodbye!", Colors.DIM))
            return None

def main():
    parser = argparse.ArgumentParser(description="Mini-League Season Diary")
    parser.add_argument("--team", type=int, help="Single team entry ID (skip menu)")
    parser.add_argument("--gw", type=int, help="Single gameweek only")
    parser.add_argument("--all", action="store_true", help="Show all teams (no menu)")
    parser.add_argument("--me", type=int, help="Override your entry ID")

    # AI flags
    parser.add_argument("--ai", action="store_true", help="Generate AI analysis per GW (OpenAI API)")
    parser.add_argument("--model", type=str, default="gpt-5.2", help="OpenAI model name (default: gpt-5.2)")
    parser.add_argument("--ai-cache-dir", type=str, default="mini_league/ai_cache", help="Cache folder for AI output")
    parser.add_argument("--no-ai-cache", action="store_true", help="Disable AI cache (always call API)")

    args = parser.parse_args()

    if args.me:
        CONFIG["my_entry_id"] = args.me

    # Load data
    entries = load_entries_index()
    if not entries:
        print(c("No entries found. Run mini-league-analysis.py first.", Colors.RED))
        return

    players = load_bootstrap()
    if not players:
        print(c("No bootstrap cache found. Run mini-league-analysis.py first.", Colors.RED))
        return

    ai_client = None
    if args.ai:
        try:
            from openai import OpenAI
            ai_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
            if not os.environ.get("OPENAI_API_KEY"):
                print(c("OPENAI_API_KEY not set. Export it first.", Colors.RED))
                return
        except ImportError:
            print(c("openai package not installed. Run: pip install openai", Colors.RED))
            return

    print(c(f"\n📔 Mini-League Season Diary", Colors.BOLD))
    print(c(f"   {len(entries)} teams in league", Colors.DIM))
    if args.ai:
        print(c(f"   AI: ON  (model={args.model})", Colors.DIM))

    use_cache = not args.no_ai_cache

    if args.team:
        entry_meta = next((e for e in entries if e.get("entry") == args.team), None)
        if not entry_meta:
            print(c(f"Team {args.team} not found in league", Colors.RED))
            return
        print_team_diary(
            args.team, entry_meta, players, args.gw,
            ai_client=ai_client, ai_model=args.model,
            ai_cache_dir=args.ai_cache_dir, use_ai_cache=use_cache
        )

    elif args.all:
        for entry in entries:
            entry_id = entry.get("entry")
            print_team_diary(
                entry_id, entry, players, args.gw,
                ai_client=ai_client, ai_model=args.model,
                ai_cache_dir=args.ai_cache_dir, use_ai_cache=use_cache
            )

    else:
        selected = interactive_menu(entries, players)
        if selected:
            entry_id = selected.get("entry")
            print_team_diary(
                entry_id, selected, players, args.gw,
                ai_client=ai_client, ai_model=args.model,
                ai_cache_dir=args.ai_cache_dir, use_ai_cache=use_cache
            )

if __name__ == "__main__":
    main()
