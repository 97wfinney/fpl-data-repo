import json
import requests
import subprocess
import os
from datetime import datetime

BASE_URL = "https://fantasy.premierleague.com/api/"

# Function to fetch data from the bootstrap-static endpoint
def fetch_bootstrap_data():
    url = BASE_URL + "bootstrap-static/"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch data from {url}.")
        return None
    return response.json()

# Function to determine the current gameweek
def get_current_gameweek(data):
    for event in data.get('events', []):
        if event.get('is_current'):
            return event.get('id')
    return None

# Function to save data to a JSON file
def save_to_json(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)

# Function to push changes to GitHub
def push_to_github(repo_path, commit_message="Add FPL data"):
    # Save current directory
    original_dir = os.getcwd()
    
    # Change to repo directory
    os.chdir(repo_path)
    
    # Add all files to the staging area
    subprocess.run(['git', 'add', '.'])
    
    # Commit the changes
    subprocess.run(['git', 'commit', '-m', commit_message])
    
    # Push to the remote repository
    subprocess.run(['git', 'push', 'origin', 'main'])
    
    # Return to original directory
    os.chdir(original_dir)

# Main function to fetch data, save it, and push to GitHub
def main():
    # Fetch the bootstrap data
    data = fetch_bootstrap_data()
    if data is None:
        return

    # Determine the current gameweek
    current_gameweek = get_current_gameweek(data)
    if current_gameweek is None:
        print("Could not determine the current gameweek.")
        return

    # Define the path to your local Git repository
    repo_path = "/home/wfinney/Desktop/fpl-data-repo"  # Change this to your actual repo path
    
    # Create the 25 folder if it doesn't exist
    folder_25_path = os.path.join(repo_path, "25")
    os.makedirs(folder_25_path, exist_ok=True)
    print(f"Created/verified folder: {folder_25_path}")

    # Create a filename based on the current gameweek
    filename = f"Gameweek_{current_gameweek}.json"
    
    # Full path including the 25 folder
    full_filepath = os.path.join(folder_25_path, filename)

    # Save the data to a JSON file in the 25 folder
    save_to_json(data, full_filepath)
    print(f"Data for Gameweek {current_gameweek} saved to {full_filepath}")

    # Create commit message with more detail
    commit_message = f"Add FPL data for Gameweek {current_gameweek} - Season 24/25"

    # Push the saved data to GitHub
    push_to_github(repo_path, commit_message)
    print(f"Successfully pushed Gameweek {current_gameweek} data to GitHub!")

if __name__ == "__main__":
    main()