import os
import json
import requests
from datetime import datetime
import subprocess
import asyncio
import discord
from dotenv import load_dotenv
load_dotenv()

BASE_URL = "https://fantasy.premierleague.com/api/"
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
DISCORD_CHANNEL_ID = 1253342360222437419


def fetch_data(url):
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch data from {url}.")
        return None
    return response.json()


def get_latest_finished_gw():
    """Return the latest GW id that is both finished and data_checked."""
    data = fetch_data(BASE_URL + "bootstrap-static/")
    if not data:
        return None
    finished = [e for e in data.get("events", []) if e.get("finished") and e.get("data_checked")]
    if not finished:
        return None
    return max(finished, key=lambda e: e.get("id", 0)).get("id")


def fetch_top_managers(limit=20):
    standings = fetch_data(BASE_URL + "leagues-classic/314/standings/")
    if standings:
        return standings["standings"]["results"][:limit]
    return []


def fetch_team_details(entry_id, gw):
    details = {
        "info": fetch_data(BASE_URL + f"entry/{entry_id}/"),
        "history": fetch_data(BASE_URL + f"entry/{entry_id}/history/"),
        "gw_picks": fetch_data(BASE_URL + f"entry/{entry_id}/event/{gw}/picks/")
    }
    return details


def save_json(data, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=4)


def push_to_github(repo_path, commit_message="Update top manager data"):
    original_dir = os.getcwd()
    os.chdir(repo_path)
    subprocess.run(['git', 'add', '.'])
    subprocess.run(['git', 'commit', '-m', commit_message])
    subprocess.run(['git', 'push', 'origin', 'main'])
    os.chdir(original_dir)


async def notify_discord(message):
    if not DISCORD_TOKEN or not DISCORD_CHANNEL_ID:
        print("No Discord credentials set.")
        return

    intents = discord.Intents.default()
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        channel = client.get_channel(DISCORD_CHANNEL_ID)
        if channel:
            await channel.send(message)
        await client.close()

    await client.start(DISCORD_TOKEN)


def main():
    gameweek = get_latest_finished_gw()
    if gameweek is None:
        # No finished, data-checked GW yet — do nothing quietly
        print("ℹ️ No finished, data-checked gameweek available. Skipping.")
        return

    top_managers = fetch_top_managers(20)
    if not top_managers:
        msg = "⚠️ Failed to fetch top managers. No data collected."
        print(msg)
        asyncio.run(notify_discord(msg))
        return

    folder = f"top_overall/Gameweek_{gameweek}/"
    summary_path = os.path.join(folder, "summary.json")
    if os.path.exists(summary_path):
        print(f"ℹ️ Latest finished GW{gameweek} already saved. Nothing to do.")
        return

    save_json(top_managers, summary_path)

    collected = []
    failed = []
    for manager in top_managers:
        entry_id = manager.get("entry")
        details = fetch_team_details(entry_id, gameweek)
        if not details or not details.get("info") or not details.get("history") or not details.get("gw_picks"):
            failed.append(entry_id)
        collected.append((entry_id, details))

    if failed:
        print(f"❌ Aborting save for GW{gameweek}: failed to fetch {len(failed)} teams: {failed}")
        # Do not post to Discord on failure; try again on next run
        return

    # Only write to disk once we know all 20 succeeded
    for entry_id, details in collected:
        save_json(details, os.path.join(folder, f"team_{entry_id}.json"))

    push_to_github("/home/wfinney/Desktop/fpl-data-repo", f"Add top 20 manager data for Gameweek {gameweek}")
    message = f"📊 Top 20 manager data for GW{gameweek} collected and pushed to GitHub!"
    print(message)
    # Post only on actual new save
    asyncio.run(notify_discord(message))


if __name__ == "__main__":
    main()