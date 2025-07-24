

import json
import os
import requests
from datetime import datetime
from dotenv import load_dotenv
import subprocess
import discord
import asyncio
import os

# --- Configuration ---
LEAGUE_ID = 106780
BASE_URL = "https://fantasy.premierleague.com/api/"
DATA_DIR = "mini_league"
LOG_FILE = "saved_gameweeks.txt"
REPO_PATH = "/home/wfinney/Desktop/fpl-data-repo"

# Discord configuration
load_dotenv()
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
DISCORD_CHANNEL_ID = 1253342360222437419
intents = discord.Intents.default()
intents.messages = True
intents.guilds = True

# --- Helper Functions ---

def fetch_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    print(f"❌ Failed to fetch: {url}")
    return None

def get_current_gameweek(bootstrap):
    for event in bootstrap.get("events", []):
        if event.get("is_current"):
            return event["id"]
    return None

def get_finished_gameweek_ids(bootstrap):
    return [e["id"] for e in bootstrap["events"] if e["finished"]]

def get_saved_gameweeks():
    if not os.path.exists(LOG_FILE):
        return set()
    with open(LOG_FILE, "r") as f:
        return set(line.strip() for line in f)

def log_gameweek(gw_id):
    with open(LOG_FILE, "a") as f:
        f.write(str(gw_id) + "\n")

def save_json(data, path):
    with open(path, "w") as f:
        json.dump(data, f, indent=4)

def push_to_github(repo_path, commit_message):
    original_dir = os.getcwd()
    os.chdir(repo_path)
    subprocess.run(["git", "add", "."])
    subprocess.run(["git", "commit", "-m", commit_message])
    subprocess.run(["git", "push", "origin", "main"])
    os.chdir(original_dir)

async def notify_discord(message):
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        channel = client.get_channel(DISCORD_CHANNEL_ID)
        await channel.send(message)
        await client.close()

    await client.start(DISCORD_TOKEN)

# --- Main Script ---

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    bootstrap = fetch_data(BASE_URL + "bootstrap-static/")
    if not bootstrap:
        return

    current_gw = get_current_gameweek(bootstrap)
    finished_gws = get_finished_gameweek_ids(bootstrap)
    saved_gws = get_saved_gameweeks()

    new_saved = False
    for gw in finished_gws:
        if str(gw) not in saved_gws:
            league_data = fetch_data(BASE_URL + f"leagues-classic/{LEAGUE_ID}/standings/?event={gw}")
            if league_data:
                filename = os.path.join(DATA_DIR, f"mini_league_gw{gw}.json")
                save_json(league_data, filename)
                log_gameweek(gw)
                new_saved = True
                print(f"✅ Saved mini-league data for GW{gw}.")

    push_to_github(REPO_PATH, "Update mini-league data")

    message = f"📊 Mini-league script ran. Current GW: {current_gw}. {'Saved new GW data.' if new_saved else 'No new data to save.'}"
    asyncio.run(notify_discord(message))

if __name__ == "__main__":
    main()