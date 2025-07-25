

import os
import json
import requests
from datetime import datetime
import subprocess
import asyncio
import discord

BASE_URL = "https://fantasy.premierleague.com/api/"
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
DISCORD_CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", 0))


def fetch_data(url):
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch data from {url}.")
        return None
    return response.json()


def get_current_gameweek():
    data = fetch_data(BASE_URL + "bootstrap-static/")
    if not data:
        return None
    for event in data.get("events", []):
        if event.get("is_current"):
            return event.get("id")
    return None


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
    gameweek = get_current_gameweek()
    if gameweek is None:
        print("Could not determine the current gameweek.")
        return

    top_managers = fetch_top_managers(20)
    if not top_managers:
        print("Failed to fetch top managers.")
        return

    folder = f"top_overall/Gameweek_{gameweek}/"
    save_json(top_managers, os.path.join(folder, "summary.json"))

    for manager in top_managers:
        entry_id = manager["entry"]
        details = fetch_team_details(entry_id, gameweek)
        save_json(details, os.path.join(folder, f"team_{entry_id}.json"))

    push_to_github("/home/wfinney/Desktop/fpl-data-repo", f"Add top 20 manager data for Gameweek {gameweek}")
    print(f"Saved and pushed data for Gameweek {gameweek}.")

    message = f"📊 Top 20 manager data for GW{gameweek} collected and pushed to GitHub!"
    asyncio.run(notify_discord(message))


if __name__ == "__main__":
    main()