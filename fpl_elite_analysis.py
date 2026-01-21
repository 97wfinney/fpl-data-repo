#!/usr/bin/env python3
"""
FPL Elite Analysis Tool
Analyses top 20 overall managers vs your team with single GW and trend analysis.

Usage:
    python fpl_elite_analysis.py                    # Latest GW analysis
    python fpl_elite_analysis.py --gw 15            # Specific GW
    python fpl_elite_analysis.py --trends           # Multi-GW trend analysis
    python fpl_elite_analysis.py --all              # Everything
"""

import os
import json
import argparse
from collections import Counter, defaultdict
from datetime import datetime
from typing import Optional
import requests

# ============================================================================
# CONFIGURATION
# ============================================================================

CONFIG = {
    "user_entry_id": 25029,  # Your FPL entry ID
    "repo_path": "/home/wfinney/Desktop/fpl-data-repo",  # Path to your data repo
    "top_managers_folder": "top_overall",  # Folder with top manager data
    "bootstrap_folder": "25",  # Folder with bootstrap snapshots (season 24/25)
    "api_base": "https://fantasy.premierleague.com/api/",
}

# Terminal formatting
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    DIM = '\033[2m'
    END = '\033[0m'

def c(text, color):
    return f"{color}{text}{Colors.END}"

def header(text):
    width = 70
    print("\n" + c("=" * width, Colors.CYAN))
    print(c(f" {text.upper()} ".center(width), Colors.BOLD + Colors.CYAN))
    print(c("=" * width, Colors.CYAN))

def subheader(text):
    print(f"\n{c('▸ ' + text, Colors.YELLOW + Colors.BOLD)}")
    print(c("─" * 50, Colors.DIM))

# ============================================================================
# DATA LOADING
# ============================================================================

def get_available_gameweeks():
    """Find all gameweeks with top manager data."""
    top_path = os.path.join(CONFIG["repo_path"], CONFIG["top_managers_folder"])
    if not os.path.exists(top_path):
        return []
    
    gws = []
    for folder in os.listdir(top_path):
        if folder.startswith("Gameweek_"):
            try:
                gw = int(folder.split("_")[1])
                gws.append(gw)
            except ValueError:
                continue
    return sorted(gws)

def load_bootstrap(gw: int) -> Optional[dict]:
    """Load bootstrap-static data for a gameweek."""
    path = os.path.join(CONFIG["repo_path"], CONFIG["bootstrap_folder"], f"Gameweek_{gw}.json")
    if not os.path.exists(path):
        # Try fetching live if not archived
        print(c(f"  Bootstrap GW{gw} not found locally, fetching live...", Colors.DIM))
        try:
            resp = requests.get(CONFIG["api_base"] + "bootstrap-static/", timeout=10)
            if resp.status_code == 200:
                return resp.json()
        except:
            pass
        return None
    
    with open(path) as f:
        return json.load(f)

def load_top_managers(gw: int) -> tuple[list, dict]:
    """Load summary and individual team files for a gameweek."""
    folder = os.path.join(CONFIG["repo_path"], CONFIG["top_managers_folder"], f"Gameweek_{gw}")
    
    summary_path = os.path.join(folder, "summary.json")
    if not os.path.exists(summary_path):
        return [], {}
    
    with open(summary_path) as f:
        summary = json.load(f)
    
    teams = {}
    for manager in summary:
        entry_id = manager.get("entry")
        team_path = os.path.join(folder, f"team_{entry_id}.json")
        if os.path.exists(team_path):
            with open(team_path) as f:
                teams[entry_id] = json.load(f)
    
    return summary, teams

def fetch_user_team(entry_id: int, gw: int) -> Optional[dict]:
    """Fetch user's team data from API."""
    try:
        info = requests.get(f"{CONFIG['api_base']}entry/{entry_id}/", timeout=10).json()
        history = requests.get(f"{CONFIG['api_base']}entry/{entry_id}/history/", timeout=10).json()
        picks = requests.get(f"{CONFIG['api_base']}entry/{entry_id}/event/{gw}/picks/", timeout=10).json()
        return {"info": info, "history": history, "gw_picks": picks}
    except Exception as e:
        print(c(f"  Failed to fetch user team: {e}", Colors.RED))
        return None

def build_player_lookup(bootstrap: dict) -> dict:
    """Build element_id -> player info lookup."""
    lookup = {}
    for player in bootstrap.get("elements", []):
        lookup[player["id"]] = {
            "name": player["web_name"],
            "full_name": f"{player['first_name']} {player['second_name']}",
            "team": player["team"],
            "position": player["element_type"],
            "price": player["now_cost"] / 10,
            "total_points": player["total_points"],
            "form": float(player.get("form", 0)),
            "selected_by": float(player.get("selected_by_percent", 0)),
            "minutes": player.get("minutes", 0),
            "goals": player.get("goals_scored", 0),
            "assists": player.get("assists", 0),
            "xG": float(player.get("expected_goals", 0)),
            "xA": float(player.get("expected_assists", 0)),
            "ict": float(player.get("ict_index", 0)),
            "points_per_game": player.get("points_per_game", "0"),
        }
    return lookup

def build_team_lookup(bootstrap: dict) -> dict:
    """Build team_id -> team name lookup."""
    return {t["id"]: t["short_name"].upper() for t in bootstrap.get("teams", [])}

def get_position_name(pos_id: int) -> str:
    return {1: "GKP", 2: "DEF", 3: "MID", 4: "FWD"}.get(pos_id, "???")

# ============================================================================
# SINGLE GAMEWEEK ANALYSIS
# ============================================================================

def analyse_elite_template(gw: int, summary: list, teams: dict, players: dict, team_names: dict):
    """Analyse ownership and template among top 20."""
    header(f"Elite Template Report - GW{gw}")
    
    # Count ownership across top 20
    ownership = Counter()
    captain_counts = Counter()
    formations = Counter()
    
    for entry_id, team_data in teams.items():
        picks = team_data.get("gw_picks", {}).get("picks", [])
        
        # Count formation
        playing = [p for p in picks if p["multiplier"] > 0]
        pos_counts = Counter(p["element_type"] for p in playing)
        formation = f"{pos_counts.get(2,0)}-{pos_counts.get(3,0)}-{pos_counts.get(4,0)}"
        formations[formation] += 1
        
        for pick in picks:
            element_id = pick["element"]
            if pick["multiplier"] > 0:  # Playing, not benched
                ownership[element_id] += 1
            if pick["is_captain"]:
                captain_counts[element_id] += 1
    
    # Essential picks (15+/20)
    subheader("Essential Picks (≥15/20 ownership)")
    essentials = [(eid, count) for eid, count in ownership.most_common() if count >= 15]
    if essentials:
        for eid, count in essentials:
            p = players.get(eid, {})
            pct = count / 20 * 100
            general_own = p.get("selected_by", 0)
            diff = pct - general_own
            diff_str = c(f"+{diff:.0f}%", Colors.GREEN) if diff > 0 else c(f"{diff:.0f}%", Colors.RED)
            print(f"  {p.get('name', '???'):15} {count:2}/20 ({pct:5.1f}%)  │  General: {general_own:5.1f}%  │  Δ {diff_str}")
    else:
        print(c("  No players owned by 15+ managers", Colors.DIM))
    
    # Top 15 owned
    subheader("Top 15 Most Owned by Elite")
    for i, (eid, count) in enumerate(ownership.most_common(15), 1):
        p = players.get(eid, {})
        pos = get_position_name(p.get("position", 0))
        team = team_names.get(p.get("team", 0), "???")
        print(f"  {i:2}. {p.get('name', '???'):15} ({pos} {team:3})  {count:2}/20  £{p.get('price', 0):.1f}m  {p.get('total_points', 0):3}pts")
    
    # Captain choices
    subheader("Captain Choices")
    for eid, count in captain_counts.most_common(5):
        p = players.get(eid, {})
        pct = count / 20 * 100
        bar = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
        print(f"  {p.get('name', '???'):15} {count:2}/20 ({pct:5.1f}%)  {bar}")
    
    # Formations
    subheader("Formation Distribution")
    for formation, count in formations.most_common():
        pct = count / 20 * 100
        bar = "█" * count
        print(f"  {formation:7} {count:2}/20 ({pct:5.1f}%)  {bar}")
    
    return ownership, captain_counts

def analyse_differentials(gw: int, ownership: Counter, players: dict, team_names: dict):
    """Find differentials owned by elite but low general ownership."""
    header("Differential Finder")
    
    subheader("Elite Differentials (<10% general ownership)")
    diffs = []
    for eid, count in ownership.items():
        p = players.get(eid, {})
        general_own = p.get("selected_by", 0)
        if general_own < 10 and count >= 3:  # At least 3/20 elite own
            diffs.append((eid, count, general_own))
    
    diffs.sort(key=lambda x: (-x[1], x[2]))  # Sort by elite ownership desc, then general asc
    
    if diffs:
        for eid, count, general in diffs[:10]:
            p = players.get(eid, {})
            pos = get_position_name(p.get("position", 0))
            team = team_names.get(p.get("team", 0), "???")
            elite_pct = count / 20 * 100
            print(f"  {p.get('name', '???'):15} ({pos} {team:3})  Elite: {count:2}/20 ({elite_pct:4.1f}%)  │  General: {general:5.1f}%  │  {p.get('total_points', 0):3}pts")
    else:
        print(c("  No significant differentials found", Colors.DIM))

def analyse_captain_hindsight(gw: int, teams: dict, players: dict, bootstrap: dict):
    """Analyse captain choices vs optimal."""
    header("Captain Hindsight")
    
    # Get GW points for each player (would need live data or element_summary)
    # For now, use total_points as proxy or fetch from picks data
    
    subheader("Captain Choices Analysis")
    
    captain_results = []
    for entry_id, team_data in teams.items():
        picks = team_data.get("gw_picks", {}).get("picks", [])
        entry_history = team_data.get("gw_picks", {}).get("entry_history", {})
        
        captain = None
        vice = None
        squad_players = []
        
        for pick in picks:
            if pick["is_captain"]:
                captain = pick["element"]
            if pick["is_vice_captain"]:
                vice = pick["element"]
            if pick["multiplier"] > 0:
                squad_players.append(pick["element"])
        
        if captain:
            captain_results.append({
                "entry_id": entry_id,
                "captain": captain,
                "squad": squad_players,
                "points": entry_history.get("points", 0)
            })
    
    # Count captain popularity
    captain_pop = Counter(r["captain"] for r in captain_results)
    
    print("  Captain popularity among top 20:")
    for eid, count in captain_pop.most_common(5):
        p = players.get(eid, {})
        pct = count / len(captain_results) * 100
        print(f"    {p.get('name', '???'):15} {count:2} managers ({pct:.0f}%)")
    
    # Effective ownership calculation
    subheader("Effective Ownership (EO) - Playing XI")
    
    eo = defaultdict(float)
    for entry_id, team_data in teams.items():
        picks = team_data.get("gw_picks", {}).get("picks", [])
        for pick in picks:
            if pick["multiplier"] > 0:
                eo[pick["element"]] += pick["multiplier"]  # Captain = 2, TC = 3
    
    # Normalize to percentage
    total_managers = len(teams)
    eo_pct = {eid: (val / total_managers) * 100 for eid, val in eo.items()}
    
    # Top EO players
    for eid, pct in sorted(eo_pct.items(), key=lambda x: -x[1])[:10]:
        p = players.get(eid, {})
        owned = sum(1 for t in teams.values() 
                   for pick in t.get("gw_picks", {}).get("picks", [])
                   if pick["element"] == eid and pick["multiplier"] > 0)
        cap_count = sum(1 for t in teams.values()
                       for pick in t.get("gw_picks", {}).get("picks", [])
                       if pick["element"] == eid and pick["is_captain"])
        print(f"  {p.get('name', '???'):15}  EO: {pct:6.1f}%  │  Owned: {owned:2}/20  │  Captained: {cap_count:2}/20")

def analyse_chips(gw: int, teams: dict):
    """Analyse chip usage among top 20."""
    header("Chip Strategy Analysis")
    
    chip_usage = defaultdict(list)
    
    for entry_id, team_data in teams.items():
        history = team_data.get("history", {})
        chips = history.get("chips", [])
        manager_name = team_data.get("info", {}).get("player_first_name", "Unknown")
        
        for chip in chips:
            chip_name = chip.get("name", "unknown")
            chip_gw = chip.get("event", 0)
            chip_usage[chip_name].append((chip_gw, entry_id, manager_name))
    
    subheader("Chip Usage by Type")
    chip_names = {"wildcard": "Wildcard", "freehit": "Free Hit", "bboost": "Bench Boost", "3xc": "Triple Captain"}
    
    for chip_key, display_name in chip_names.items():
        usages = chip_usage.get(chip_key, [])
        if usages:
            gw_counts = Counter(u[0] for u in usages)
            print(f"\n  {c(display_name, Colors.BOLD)} ({len(usages)} used):")
            for chip_gw, count in sorted(gw_counts.items()):
                bar = "█" * count
                print(f"    GW{chip_gw:2}: {count:2} managers  {bar}")
        else:
            print(f"\n  {c(display_name, Colors.BOLD)}: None used yet")
    
    # Active chip this GW
    subheader(f"Active Chips in GW{gw}")
    active_chips = []
    for entry_id, team_data in teams.items():
        active = team_data.get("gw_picks", {}).get("active_chip")
        if active:
            name = team_data.get("info", {}).get("player_first_name", "Unknown")
            active_chips.append((active, name, entry_id))
    
    if active_chips:
        for chip, name, eid in active_chips:
            print(f"  {chip_names.get(chip, chip):15} - {name}")
    else:
        print(c("  No chips active this gameweek", Colors.DIM))

def analyse_transfer_recommendations(user_team: dict, teams: dict, ownership: Counter,
                                     players: dict, team_names: dict):
    """Generate actionable transfer recommendations based on elite template."""
    header("Transfer Recommendations")
    
    user_picks = user_team.get("gw_picks", {}).get("picks", [])
    user_squad = {p["element"]: p for p in user_picks}
    user_playing = [p["element"] for p in user_picks if p["multiplier"] > 0]
    
    # Get user's bank and team value
    entry_history = user_team.get("gw_picks", {}).get("entry_history", {})
    bank = entry_history.get("bank", 0) / 10  # Convert to millions
    team_value = entry_history.get("value", 0) / 10
    
    print(f"  Bank: £{bank:.1f}m  │  Team Value: £{team_value:.1f}m")
    
    # === SELL CANDIDATES ===
    subheader("Sell Candidates (You own, elite don't)")
    
    sell_candidates = []
    for eid in user_squad:
        elite_count = ownership.get(eid, 0)
        p = players.get(eid, {})
        if elite_count <= 3 and p.get("total_points", 0) > 0:  # Low elite ownership
            sell_candidates.append({
                "id": eid,
                "name": p.get("name", "???"),
                "position": p.get("position", 0),
                "team": p.get("team", 0),
                "price": p.get("price", 0),
                "points": p.get("total_points", 0),
                "elite_own": elite_count,
                "form": p.get("form", 0),
                "ppg": float(p.get("points_per_game", 0)) if p.get("points_per_game") else p.get("total_points", 0) / max(p.get("minutes", 1) / 90, 1),
            })
    
    # Sort by elite ownership (lowest first), then by points (lowest first)
    sell_candidates.sort(key=lambda x: (x["elite_own"], x["points"]))
    
    if sell_candidates:
        for s in sell_candidates[:5]:
            pos = get_position_name(s["position"])
            team = team_names.get(s["team"], "???")
            color = Colors.RED if s["elite_own"] == 0 else Colors.YELLOW
            print(f"  {c('✗', color)} {s['name']:15} ({pos} {team:3})  {s['elite_own']}/20 elite  │  £{s['price']:.1f}m  │  {s['points']}pts  │  Form: {s['form']:.1f}")
    else:
        print(c("  No obvious sell candidates", Colors.GREEN))
    
    # === BUY TARGETS ===
    subheader("Buy Targets (Elite own, you don't)")
    
    buy_targets = []
    for eid, elite_count in ownership.items():
        if eid not in user_squad and elite_count >= 6:  # At least 6/20 elite own
            p = players.get(eid, {})
            buy_targets.append({
                "id": eid,
                "name": p.get("name", "???"),
                "position": p.get("position", 0),
                "team": p.get("team", 0),
                "price": p.get("price", 0),
                "points": p.get("total_points", 0),
                "elite_own": elite_count,
                "form": p.get("form", 0),
                "selected_by": p.get("selected_by", 0),
            })
    
    # Sort by elite ownership (highest first)
    buy_targets.sort(key=lambda x: (-x["elite_own"], -x["points"]))
    
    if buy_targets:
        for b in buy_targets[:8]:
            pos = get_position_name(b["position"])
            team = team_names.get(b["team"], "???")
            affordable = "✓" if b["price"] <= bank + 5 else "£"  # Rough affordability check
            color = Colors.GREEN if b["elite_own"] >= 10 else Colors.CYAN
            print(f"  {c('→', color)} {b['name']:15} ({pos} {team:3})  {b['elite_own']}/20 elite  │  £{b['price']:.1f}m  │  {b['points']}pts  │  Form: {b['form']:.1f}")
    else:
        print(c("  You already have the elite template!", Colors.GREEN))
    
    # === SPECIFIC TRANSFER SUGGESTIONS ===
    subheader("Suggested Transfers")
    
    # Match sell candidates to buy targets by position
    suggestions = []
    used_sells = set()
    used_buys = set()
    
    for sell in sell_candidates:
        for buy in buy_targets:
            if buy["id"] in used_buys:
                continue
            if sell["id"] in used_sells:
                continue
            
            # Same position or flexible (MID/FWD can sometimes swap)
            same_pos = sell["position"] == buy["position"]
            flex_pos = (sell["position"] in [3, 4] and buy["position"] in [3, 4])
            
            if same_pos or flex_pos:
                price_diff = buy["price"] - sell["price"]
                can_afford = price_diff <= bank
                
                if can_afford or price_diff <= bank + 1:  # Within £1m of affording
                    gain = buy["elite_own"] - sell["elite_own"]
                    pts_diff = buy["points"] - sell["points"]
                    
                    suggestions.append({
                        "sell": sell,
                        "buy": buy,
                        "price_diff": price_diff,
                        "can_afford": can_afford,
                        "elite_gain": gain,
                        "pts_diff": pts_diff,
                    })
                    used_sells.add(sell["id"])
                    used_buys.add(buy["id"])
                    break
    
    # Sort by elite ownership gain
    suggestions.sort(key=lambda x: (-x["elite_gain"], -x["pts_diff"]))
    
    if suggestions:
        for i, s in enumerate(suggestions[:4], 1):
            sell_name = s["sell"]["name"]
            buy_name = s["buy"]["name"]
            price_diff = s["price_diff"]
            afford_str = c("✓", Colors.GREEN) if s["can_afford"] else c(f"need £{price_diff-bank:.1f}m", Colors.YELLOW)
            elite_gain = s["elite_gain"]
            pts_diff = s["pts_diff"]
            
            pts_color = Colors.GREEN if pts_diff > 0 else Colors.RED if pts_diff < 0 else Colors.DIM
            
            print(f"\n  {c(f'#{i}', Colors.BOLD)} {c(sell_name, Colors.RED)} → {c(buy_name, Colors.GREEN)}")
            print(f"      Cost: {'+' if price_diff > 0 else ''}{price_diff:.1f}m  {afford_str}")
            print(f"      Elite ownership: +{elite_gain} managers")
            print(f"      Points difference: {c(f'{pts_diff:+}pts', pts_color)}")
    else:
        print(c("  No clear transfer pairs found. Consider a broader restructure.", Colors.DIM))
    
    # === PRIORITY RATING ===
    subheader("Transfer Priority")
    
    # Calculate how far from template
    template_match = sum(1 for eid in user_playing if ownership.get(eid, 0) >= 8)
    template_pct = template_match / 11 * 100
    
    if template_pct >= 70:
        priority = c("LOW", Colors.GREEN)
        advice = "Your team aligns well with elite template. Focus on captaincy and differentials."
    elif template_pct >= 50:
        priority = c("MEDIUM", Colors.YELLOW)
        advice = "Some divergence from elite. 1-2 transfers could close the gap."
    else:
        priority = c("HIGH", Colors.RED)
        advice = "Significant divergence. Consider multiple transfers or a wildcard."
    
    print(f"  Template match: {template_match}/11 ({template_pct:.0f}%)")
    print(f"  Priority: {priority}")
    print(f"  {advice}")


def analyse_user_comparison(gw: int, user_team: dict, teams: dict, ownership: Counter, 
                           captain_counts: Counter, players: dict, team_names: dict, summary: list):
    """Compare user's team to top 20."""
    header(f"Your Team vs Top 20 - GW{gw}")
    
    user_info = user_team.get("info", {})
    user_picks_data = user_team.get("gw_picks", {})
    user_picks = user_picks_data.get("picks", [])
    user_history = user_team.get("history", {}).get("current", [])
    
    # Basic info
    subheader("Your Status")
    print(f"  Team: {user_info.get('name', 'Unknown')}")
    print(f"  Manager: {user_info.get('player_first_name', '')} {user_info.get('player_last_name', '')}")
    print(f"  Overall Rank: {user_info.get('summary_overall_rank', 'N/A'):,}")
    print(f"  Total Points: {user_info.get('summary_overall_points', 0)}")
    
    # Compare to top 20 range
    if summary:
        top_points = summary[0].get("total", 0)
        bottom_points = summary[-1].get("total", 0)
        user_points = user_info.get('summary_overall_points', 0)
        gap = top_points - user_points
        print(f"\n  Top 20 range: {bottom_points} - {top_points} pts")
        print(f"  Gap to #1: {c(f'{gap} pts', Colors.YELLOW)}")
    
    # Squad comparison
    subheader("Squad vs Elite Template")
    
    user_squad = {p["element"]: p for p in user_picks}
    user_playing = [p["element"] for p in user_picks if p["multiplier"] > 0]
    user_captain = next((p["element"] for p in user_picks if p["is_captain"]), None)
    
    # Players you have that elite also have
    print("\n  " + c("Players you share with elite:", Colors.GREEN))
    shared = []
    for eid in user_playing:
        if eid in ownership:
            p = players.get(eid, {})
            elite_count = ownership[eid]
            shared.append((eid, elite_count))
    
    shared.sort(key=lambda x: -x[1])
    for eid, count in shared:
        p = players.get(eid, {})
        is_cap = " (C)" if eid == user_captain else ""
        elite_cap = captain_counts.get(eid, 0)
        cap_str = f" │ {elite_cap}/20 cap" if elite_cap > 0 else ""
        print(f"    {p.get('name', '???'):15}{is_cap:4}  {count:2}/20 elite own{cap_str}")
    
    # Players elite have that you don't
    print("\n  " + c("Elite picks you're missing:", Colors.RED))
    missing = []
    for eid, count in ownership.most_common():
        if eid not in user_squad and count >= 8:  # At least 8/20 own
            missing.append((eid, count))
    
    for eid, count in missing[:8]:
        p = players.get(eid, {})
        pos = get_position_name(p.get("position", 0))
        team = team_names.get(p.get("team", 0), "???")
        print(f"    {p.get('name', '???'):15} ({pos} {team:3})  {count:2}/20 own  │  £{p.get('price', 0):.1f}m  │  {p.get('total_points', 0)}pts")
    
    # Your differentials vs elite
    print("\n  " + c("Your differentials (elite <5/20 own):", Colors.CYAN))
    diffs = []
    for eid in user_playing:
        elite_count = ownership.get(eid, 0)
        if elite_count < 5:
            p = players.get(eid, {})
            diffs.append((eid, elite_count, p.get("total_points", 0)))
    
    diffs.sort(key=lambda x: -x[2])  # Sort by points
    for eid, count, pts in diffs:
        p = players.get(eid, {})
        pos = get_position_name(p.get("position", 0))
        print(f"    {p.get('name', '???'):15} ({pos})  {count}/20 elite own  │  {pts}pts")
    
    # Captain comparison
    subheader("Captain Comparison")
    if user_captain:
        p = players.get(user_captain, {})
        elite_cap_count = captain_counts.get(user_captain, 0)
        elite_own_count = ownership.get(user_captain, 0)
        
        print(f"  Your captain: {c(p.get('name', '???'), Colors.BOLD)}")
        print(f"  Elite captaincy: {elite_cap_count}/20 ({elite_cap_count/20*100:.0f}%)")
        print(f"  Elite ownership: {elite_own_count}/20")
        
        # Most popular elite captain
        if captain_counts:
            top_cap_id, top_cap_count = captain_counts.most_common(1)[0]
            if top_cap_id != user_captain:
                top_cap = players.get(top_cap_id, {})
                print(f"\n  Most popular elite captain: {c(top_cap.get('name', '???'), Colors.YELLOW)} ({top_cap_count}/20)")

def analyse_bench(gw: int, teams: dict, players: dict):
    """Analyse bench decisions and points left on bench."""
    header("Bench Analysis")
    
    subheader("Points on Bench - Top 20")
    
    bench_data = []
    for entry_id, team_data in teams.items():
        history = team_data.get("history", {}).get("current", [])
        gw_history = next((h for h in history if h.get("event") == gw), None)
        
        if gw_history:
            bench_pts = gw_history.get("points_on_bench", 0)
            total_pts = gw_history.get("points", 0)
            name = team_data.get("info", {}).get("player_first_name", "Unknown")
            bench_data.append((name, bench_pts, total_pts, entry_id))
    
    bench_data.sort(key=lambda x: -x[1])  # Sort by bench points desc
    
    avg_bench = sum(b[1] for b in bench_data) / len(bench_data) if bench_data else 0
    print(f"  Average bench points: {c(f'{avg_bench:.1f}', Colors.YELLOW)}")
    print()
    
    for name, bench_pts, total_pts, eid in bench_data[:10]:
        bar = "█" * min(bench_pts // 2, 20) if bench_pts > 0 else ""
        color = Colors.RED if bench_pts > 15 else Colors.YELLOW if bench_pts > 8 else Colors.GREEN
        print(f"  {name:15} {c(f'{bench_pts:2}pts', color)} on bench  │  {total_pts}pts scored  {bar}")

# ============================================================================
# MULTI-GAMEWEEK TREND ANALYSIS  
# ============================================================================

def analyse_trends(available_gws: list, players: dict, team_names: dict):
    """Analyse trends across multiple gameweeks."""
    header("Multi-Gameweek Trend Analysis")
    
    if len(available_gws) < 2:
        print(c("  Need at least 2 gameweeks of data for trend analysis", Colors.RED))
        return
    
    print(f"  Analysing GWs: {min(available_gws)} - {max(available_gws)} ({len(available_gws)} weeks)")
    
    # Track ownership over time
    ownership_history = defaultdict(list)  # player_id -> [(gw, count), ...]
    captain_history = defaultdict(list)
    
    for gw in available_gws:
        _, teams = load_top_managers(gw)
        
        gw_ownership = Counter()
        gw_captains = Counter()
        
        for entry_id, team_data in teams.items():
            picks = team_data.get("gw_picks", {}).get("picks", [])
            for pick in picks:
                if pick["multiplier"] > 0:
                    gw_ownership[pick["element"]] += 1
                if pick["is_captain"]:
                    gw_captains[pick["element"]] += 1
        
        for eid, count in gw_ownership.items():
            ownership_history[eid].append((gw, count))
        for eid, count in gw_captains.items():
            captain_history[eid].append((gw, count))
    
    # Rising ownership
    subheader("Rising Stars (Increasing Elite Ownership)")
    
    risers = []
    for eid, history in ownership_history.items():
        if len(history) >= 2:
            history.sort(key=lambda x: x[0])
            recent = [h for h in history if h[0] >= max(available_gws) - 3]
            older = [h for h in history if h[0] <= min(available_gws) + 2]
            
            if recent and older:
                recent_avg = sum(h[1] for h in recent) / len(recent)
                older_avg = sum(h[1] for h in older) / len(older)
                change = recent_avg - older_avg
                
                if change >= 3:  # Gained 3+ owners
                    current = history[-1][1]
                    risers.append((eid, change, current, older_avg))
    
    risers.sort(key=lambda x: -x[1])
    for eid, change, current, old in risers[:8]:
        p = players.get(eid, {})
        pos = get_position_name(p.get("position", 0))
        team = team_names.get(p.get("team", 0), "???")
        print(f"  {p.get('name', '???'):15} ({pos} {team:3})  {old:.0f} → {current}/20  {c(f'+{change:.0f}', Colors.GREEN)}")
    
    # Falling ownership
    subheader("Falling Out of Favour")
    
    fallers = []
    for eid, history in ownership_history.items():
        if len(history) >= 2:
            history.sort(key=lambda x: x[0])
            recent = [h for h in history if h[0] >= max(available_gws) - 3]
            older = [h for h in history if h[0] <= min(available_gws) + 2]
            
            if recent and older:
                recent_avg = sum(h[1] for h in recent) / len(recent)
                older_avg = sum(h[1] for h in older) / len(older)
                change = recent_avg - older_avg
                
                if change <= -3 and older_avg >= 8:  # Lost 3+ owners, was popular
                    current = history[-1][1]
                    fallers.append((eid, change, current, older_avg))
    
    fallers.sort(key=lambda x: x[1])
    for eid, change, current, old in fallers[:8]:
        p = players.get(eid, {})
        pos = get_position_name(p.get("position", 0))
        team = team_names.get(p.get("team", 0), "???")
        print(f"  {p.get('name', '???'):15} ({pos} {team:3})  {old:.0f} → {current}/20  {c(f'{change:.0f}', Colors.RED)}")
    
    # Consistent essentials
    subheader("Consistent Essentials (High Ownership Throughout)")
    
    consistents = []
    for eid, history in ownership_history.items():
        if len(history) >= len(available_gws) * 0.7:  # Present in 70%+ of weeks
            avg_ownership = sum(h[1] for h in history) / len(history)
            min_ownership = min(h[1] for h in history)
            if avg_ownership >= 12 and min_ownership >= 8:
                consistents.append((eid, avg_ownership, min_ownership))
    
    consistents.sort(key=lambda x: -x[1])
    for eid, avg, minimum in consistents[:8]:
        p = players.get(eid, {})
        pos = get_position_name(p.get("position", 0))
        team = team_names.get(p.get("team", 0), "???")
        print(f"  {p.get('name', '???'):15} ({pos} {team:3})  Avg: {avg:.1f}/20  │  Min: {minimum}/20  │  {p.get('total_points', 0)}pts")

def analyse_user_trajectory(available_gws: list, user_entry_id: int):
    """Analyse user's rank trajectory over time."""
    header("Your Rank Trajectory")
    
    try:
        history = requests.get(f"{CONFIG['api_base']}entry/{user_entry_id}/history/", timeout=10).json()
        current = history.get("current", [])
    except Exception as e:
        print(c(f"  Failed to fetch history: {e}", Colors.RED))
        return
    
    subheader("Rank Progression")
    
    # Filter to available GWs
    relevant = [h for h in current if h.get("event") in available_gws]
    
    if not relevant:
        print(c("  No matching gameweek data", Colors.DIM))
        return
    
    prev_rank = None
    for h in relevant:
        gw = h.get("event")
        rank = h.get("overall_rank", 0)
        pts = h.get("points", 0)
        total = h.get("total_points", 0)
        bench = h.get("points_on_bench", 0)
        
        if prev_rank:
            change = prev_rank - rank
            if change > 0:
                change_str = c(f"↑{change:,}", Colors.GREEN)
            elif change < 0:
                change_str = c(f"↓{abs(change):,}", Colors.RED)
            else:
                change_str = c("─", Colors.DIM)
        else:
            change_str = ""
        
        print(f"  GW{gw:2}  {pts:3}pts  │  Rank: {rank:>10,}  {change_str:15}  │  Bench: {bench}pts")
        prev_rank = rank
    
    # Summary stats
    subheader("Season Summary")
    if current:
        all_pts = [h.get("points", 0) for h in current]
        all_bench = [h.get("points_on_bench", 0) for h in current]
        
        print(f"  Average GW score: {sum(all_pts)/len(all_pts):.1f}")
        print(f"  Best GW: {max(all_pts)} pts")
        print(f"  Worst GW: {min(all_pts)} pts")
        print(f"  Total bench points: {sum(all_bench)}")

# ============================================================================
# MAIN
# ============================================================================

def run_single_gw_analysis(gw: int):
    """Run all single-gameweek analyses."""
    print(c(f"\n{'='*70}", Colors.CYAN))
    print(c(f" FPL ELITE ANALYSIS - GAMEWEEK {gw} ".center(70), Colors.BOLD + Colors.CYAN))
    print(c(f"{'='*70}", Colors.CYAN))
    
    # Load data
    print(c("\nLoading data...", Colors.DIM))
    bootstrap = load_bootstrap(gw)
    if not bootstrap:
        print(c("Failed to load bootstrap data", Colors.RED))
        return
    
    summary, teams = load_top_managers(gw)
    if not teams:
        print(c(f"No top manager data found for GW{gw}", Colors.RED))
        return
    
    players = build_player_lookup(bootstrap)
    team_names = build_team_lookup(bootstrap)
    
    print(c(f"Loaded {len(teams)} top managers, {len(players)} players", Colors.DIM))
    
    # Run analyses
    ownership, captain_counts = analyse_elite_template(gw, summary, teams, players, team_names)
    analyse_differentials(gw, ownership, players, team_names)
    analyse_captain_hindsight(gw, teams, players, bootstrap)
    analyse_chips(gw, teams)
    analyse_bench(gw, teams, players)
    
    # User comparison
    print(c("\nFetching your team data...", Colors.DIM))
    user_team = fetch_user_team(CONFIG["user_entry_id"], gw)
    if user_team:
        analyse_user_comparison(gw, user_team, teams, ownership, captain_counts, players, team_names, summary)
        analyse_transfer_recommendations(user_team, teams, ownership, players, team_names)
        
        # Quick summary at the end
        print_executive_summary(user_team, teams, ownership, captain_counts, players, summary)
    else:
        print(c("Could not fetch your team data", Colors.YELLOW))


def print_executive_summary(user_team: dict, teams: dict, ownership: Counter, 
                           captain_counts: Counter, players: dict, summary: list):
    """Print a quick actionable summary."""
    header("Executive Summary")
    
    user_picks = user_team.get("gw_picks", {}).get("picks", [])
    user_squad = {p["element"]: p for p in user_picks}
    user_playing = [p["element"] for p in user_picks if p["multiplier"] > 0]
    user_captain = next((p["element"] for p in user_picks if p["is_captain"]), None)
    
    # Key metrics
    user_info = user_team.get("info", {})
    rank = user_info.get("summary_overall_rank", 0)
    points = user_info.get("summary_overall_points", 0)
    top_points = summary[0].get("total", 0) if summary else 0
    gap = top_points - points
    
    # Template alignment
    high_owned = [eid for eid, count in ownership.items() if count >= 10]
    user_has_essential = sum(1 for eid in high_owned if eid in user_squad)
    
    # Most popular captain
    if captain_counts:
        top_cap, top_cap_count = captain_counts.most_common(1)[0]
        top_cap_name = players.get(top_cap, {}).get("name", "???")
        user_cap_name = players.get(user_captain, {}).get("name", "???") if user_captain else "???"
        cap_match = "✓" if user_captain == top_cap else "✗"
    
    # Biggest miss
    biggest_miss = None
    for eid, count in ownership.most_common():
        if eid not in user_squad and count >= 10:
            biggest_miss = players.get(eid, {})
            biggest_miss_count = count
            break
    
    print(f"""
  {c('YOUR POSITION', Colors.BOLD)}
  ────────────────────────────────────────
  Rank: {rank:,}  │  Points: {points}  │  Gap to #1: {gap}pts
  
  {c('KEY ACTIONS', Colors.BOLD)}
  ────────────────────────────────────────""")
    
    # Action 1: Template alignment
    if user_has_essential < len(high_owned):
        missing = len(high_owned) - user_has_essential
        print(f"  {c('1.', Colors.YELLOW)} Missing {missing} elite essentials (≥10/20 ownership)")
        if biggest_miss:
            print(f"     → Priority target: {c(biggest_miss.get('name', '???'), Colors.GREEN)} ({biggest_miss_count}/20 own)")
    else:
        print(f"  {c('1.', Colors.GREEN)} ✓ You have all elite essentials")
    
    # Action 2: Captain
    if captain_counts:
        print(f"  {c('2.', Colors.YELLOW if cap_match == '✗' else Colors.GREEN)} Captain: {user_cap_name} {cap_match}")
        if cap_match == "✗":
            print(f"     → Elite favourite: {top_cap_name} ({top_cap_count}/20)")
    
    # Action 3: Differentials assessment
    user_diffs = [eid for eid in user_playing if ownership.get(eid, 0) <= 3]
    if user_diffs:
        diff_names = [players.get(eid, {}).get("name", "???") for eid in user_diffs[:3]]
        print(f"  {c('3.', Colors.CYAN)} You have {len(user_diffs)} differentials: {', '.join(diff_names)}")
        print(f"     → Review if underperforming vs elite picks")
    
    print()

def run_trend_analysis():
    """Run multi-gameweek trend analysis."""
    available_gws = get_available_gameweeks()
    
    if not available_gws:
        print(c("No gameweek data found in repository", Colors.RED))
        return
    
    print(c(f"\nFound data for GWs: {available_gws}", Colors.DIM))
    
    # Load latest bootstrap for player names
    bootstrap = load_bootstrap(max(available_gws))
    if not bootstrap:
        bootstrap = load_bootstrap(available_gws[-1])
    
    if not bootstrap:
        print(c("Failed to load bootstrap data", Colors.RED))
        return
    
    players = build_player_lookup(bootstrap)
    team_names = build_team_lookup(bootstrap)
    
    analyse_trends(available_gws, players, team_names)
    analyse_user_trajectory(available_gws, CONFIG["user_entry_id"])

def main():
    parser = argparse.ArgumentParser(description="FPL Elite Analysis Tool")
    parser.add_argument("--gw", type=int, help="Specific gameweek to analyse")
    parser.add_argument("--trends", action="store_true", help="Run multi-GW trend analysis")
    parser.add_argument("--all", action="store_true", help="Run all analyses")
    parser.add_argument("--user", type=int, help="Override user entry ID")
    
    args = parser.parse_args()
    
    if args.user:
        CONFIG["user_entry_id"] = args.user
    
    available_gws = get_available_gameweeks()
    
    if args.all:
        # Run latest single GW + trends
        if available_gws:
            run_single_gw_analysis(max(available_gws))
        run_trend_analysis()
    elif args.trends:
        run_trend_analysis()
    elif args.gw:
        run_single_gw_analysis(args.gw)
    else:
        # Default: latest available GW
        if available_gws:
            run_single_gw_analysis(max(available_gws))
        else:
            print(c("No gameweek data found. Run your data collection scripts first.", Colors.RED))

if __name__ == "__main__":
    main()
