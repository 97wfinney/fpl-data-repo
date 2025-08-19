import os
import re
import json
import glob
import time
import argparse
import subprocess
from datetime import datetime
from typing import Dict, List, Set, Tuple

import requests
from dotenv import load_dotenv

# =====================
# Configuration
# =====================
LEAGUE_DATA_DIR = "mini_league"  # where mini_league_gw*.json are saved
OUTPUT_ROOT = os.path.join(LEAGUE_DATA_DIR, "entries")
REPO_PATH = "/home/wfinney/Desktop/fpl-data-repo"
BASE_URL = "https://fantasy.premierleague.com/api/"
LOG_FILE = os.path.join(LEAGUE_DATA_DIR, "entries_index.json")

# Optional Discord (only used if both env vars are present)
load_dotenv()
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
DISCORD_CHANNEL_ID = os.getenv("DISCORD_CHANNEL_ID", "1253342360222437419")

HEADERS = {
    "User-Agent": "fpl-data-repo/mini-league-analysis (+https://fantasy.premierleague.com)"
}

# =====================
# Helpers
# =====================

def debug(msg: str):
    print(msg, flush=True)


def fetch_json(url: str, max_retries: int = 4, backoff: float = 0.8):
    """GET a JSON endpoint with simple retry/backoff and sensible timeouts."""
    attempt = 0
    while True:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                return resp.json()
            # 404s on some endpoints (e.g., picks before team is created) should not hard-fail
            if resp.status_code in (403, 404):
                debug(f"⚠️  {resp.status_code} for {url}")
                return None
            debug(f"❌ {resp.status_code} for {url}")
        except requests.RequestException as e:
            debug(f"❌ Request error for {url}: {e}")
        attempt += 1
        if attempt > max_retries:
            return None
        sleep_for = backoff * (2 ** (attempt - 1))
        time.sleep(sleep_for)


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def write_json(path: str, data) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def read_json_if_exists(path: str):
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return None


def push_to_github(repo_path: str, commit_message: str) -> None:
    original_dir = os.getcwd()
    os.chdir(repo_path)
    try:
        subprocess.run(["git", "add", "."], check=False)
        subprocess.run(["git", "commit", "-m", commit_message], check=False)
        subprocess.run(["git", "push", "origin", "main"], check=False)
    finally:
        os.chdir(original_dir)


# =====================
# League parsing
# =====================

def discover_saved_league_files() -> List[str]:
    """Find all saved mini-league JSON files (mini_league_gw*.json)."""
    pattern = os.path.join(LEAGUE_DATA_DIR, "mini_league_gw*.json")
    files = sorted(glob.glob(pattern))
    return files


def parse_entry_ids_from_league(files: List[str]) -> Dict[int, Dict[str, str]]:
    """Return mapping of entry_id -> {entry_name, player_name} from saved league files.
    Aggregates across all files to capture anyone who joined later, etc.
    """
    entries: Dict[int, Dict[str, str]] = {}
    for fp in files:
        try:
            with open(fp, "r") as f:
                data = json.load(f)
        except Exception as e:
            debug(f"⚠️  Failed to read {fp}: {e}")
            continue
        standings = data.get("standings") or {}
        results = standings.get("results") or []
        for r in results:
            entry_id = r.get("entry")
            if isinstance(entry_id, int):
                entries[entry_id] = {
                    "entry_name": r.get("entry_name", ""),
                    "player_name": r.get("player_name", ""),
                    "rank": r.get("rank"),
                }
    return entries


# =====================
# FPL endpoints per entry
# =====================

def fetch_bootstrap():
    return fetch_json(BASE_URL + "bootstrap-static/")


def finished_or_current_gws(bootstrap) -> List[int]:
    events = bootstrap.get("events", []) if bootstrap else []
    gws = []
    for e in events:
        if e.get("finished") or e.get("is_current"):
            gws.append(e.get("id"))
    # Ensure uniqueness and sort
    return sorted(set([gw for gw in gws if isinstance(gw, int)]))


def gather_entry_data(entry_id: int, gws: List[int], refresh: bool = False) -> Tuple[int, int]:
    """Fetch and save all data for a single entry.
    Returns (files_written, requests_made).
    """
    files_written = 0
    requests_made = 0

    entry_dir = os.path.join(OUTPUT_ROOT, str(entry_id))
    picks_dir = os.path.join(entry_dir, "picks")
    ensure_dir(entry_dir)
    ensure_dir(picks_dir)

    # 1) Static entry info
    entry_path = os.path.join(entry_dir, "entry.json")
    if refresh or not os.path.exists(entry_path):
        data = fetch_json(BASE_URL + f"entry/{entry_id}/")
        requests_made += 1
        if data is not None:
            write_json(entry_path, data)
            files_written += 1

    # 2) History (current + past + chips + transfers summary)
    history_path = os.path.join(entry_dir, "history.json")
    if refresh or not os.path.exists(history_path):
        data = fetch_json(BASE_URL + f"entry/{entry_id}/history/")
        requests_made += 1
        if data is not None:
            write_json(history_path, data)
            files_written += 1

    # 3) Transfers (full transfer log)
    transfers_path = os.path.join(entry_dir, "transfers.json")
    if refresh or not os.path.exists(transfers_path):
        data = fetch_json(BASE_URL + f"entry/{entry_id}/transfers/")
        requests_made += 1
        if data is not None:
            write_json(transfers_path, data)
            files_written += 1

    # 4) Picks per GW (for all finished + current)
    for gw in gws:
        gw_path = os.path.join(picks_dir, f"gw{gw}.json")
        if refresh or not os.path.exists(gw_path):
            data = fetch_json(BASE_URL + f"entry/{entry_id}/event/{gw}/picks/")
            requests_made += 1
            if data is not None:
                write_json(gw_path, data)
                files_written += 1
            # Be a little gentle on the API
            time.sleep(0.15)

    return files_written, requests_made


# =====================
# Main
# =====================

def build_entries_index(entries_meta: Dict[int, Dict[str, str]]) -> None:
    """Persist an index of all entries with names for convenience."""
    index = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "count": len(entries_meta),
        "entries": [
            {"entry": eid, **meta} for eid, meta in sorted(entries_meta.items(), key=lambda kv: kv[0])
        ],
    }
    write_json(LOG_FILE, index)


def main():
    parser = argparse.ArgumentParser(description="Collect rich data for all mini-league entries.")
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-fetch and overwrite existing files instead of skipping",
    )
    parser.add_argument(
        "--push",
        action="store_true",
        help="git add/commit/push changes to the repository when finished",
    )
    args = parser.parse_args()

    ensure_dir(LEAGUE_DATA_DIR)
    ensure_dir(OUTPUT_ROOT)

    files = discover_saved_league_files()
    if not files:
        debug("❌ No mini_league_gw*.json files found. Run your collector first.")
        return

    entries_meta = parse_entry_ids_from_league(files)
    if not entries_meta:
        debug("❌ No entries discovered in saved league files.")
        return

    debug(f"🔎 Discovered {len(entries_meta)} entries from saved league files.")
    build_entries_index(entries_meta)

    bootstrap = fetch_bootstrap()
    gws = finished_or_current_gws(bootstrap)
    if not gws:
        debug("⚠️  Could not determine finished/current gameweeks; defaulting to 1..38")
        gws = list(range(1, 39))

    total_files = 0
    total_requests = 0

    for i, (entry_id, meta) in enumerate(entries_meta.items(), start=1):
        debug(f"➡️  ({i}/{len(entries_meta)}) Entry {entry_id}: {meta.get('entry_name')} / {meta.get('player_name')}")
        fw, rm = gather_entry_data(entry_id, gws, refresh=args.refresh)
        total_files += fw
        total_requests += rm

    debug(
        f"✅ Done. Files written: {total_files}. API calls made: {total_requests}. Entries: {len(entries_meta)}."
    )

    # Optional git push
    if args.push:
        commit_msg = (
            f"Collect entry data: {len(entries_meta)} entries, files {total_files}, calls {total_requests}"
        )
        push_to_github(REPO_PATH, commit_msg)

    # Optional Discord (only if token present)
    if DISCORD_TOKEN:
        try:
            import discord
            import asyncio

            intents = discord.Intents.default()
            intents.messages = True
            intents.guilds = True

            async def notify(message: str):
                client = discord.Client(intents=intents)

                @client.event
                async def on_ready():
                    channel = client.get_channel(int(DISCORD_CHANNEL_ID))
                    await channel.send(message)
                    await client.close()

                await client.start(DISCORD_TOKEN)

            msg = (
                f"📥 Mini-league analysis collected. Entries: {len(entries_meta)}. "
                f"Files written: {total_files}. Calls: {total_requests}."
            )
            asyncio.run(notify(msg))
        except Exception as e:
            debug(f"(Discord notify skipped) {e}")


if __name__ == "__main__":
    main()
