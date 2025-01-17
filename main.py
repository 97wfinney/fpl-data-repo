import json

# Load the data from the JSON file
with open("Gameweek_20.json", "r") as file:
    data = json.load(file)

# Extract player data
players = data["elements"]
teams = {team["id"]: team["name"] for team in data["teams"]}

# Define position limits
POSITION_NAMES = {
    1: "Goalkeeper",
    2: "Defender",
    3: "Midfielder",
    4: "Forward",
}
TOP_N = 5  # Number of top players to find in each position

# Extract team strengths from JSON
team_strengths = {
    team["id"]: {
        "attack": (team["strength_attack_home"] + team["strength_attack_away"]) / 2,
        "defence": (team["strength_defence_home"] + team["strength_defence_away"]) / 2,
    }
    for team in data["teams"]
}

# Define function to rank players by a score with team strength
def calculate_player_score(player, team_strengths):
    """Calculate a score for each player based on form, points, cost efficiency, and team strength."""
    team_id = player["team"]
    team = team_strengths[team_id]

    # Adjust multiplier based on player position
    if player["element_type"] in [3, 4]:  # Midfielders and Forwards
        strength_multiplier = team["attack"]
    else:  # Goalkeepers and Defenders
        strength_multiplier = team["defence"]

    return (
        float(player["form"]) * 2
        + player["total_points"]
        + float(player["ep_next"])
        + strength_multiplier / 100  # Normalise team strength impact
    )

# Filter available players
available_players = [player for player in players if player["status"] == "a"]

# Find top players for each position
top_players_by_position = {}

for position, name in POSITION_NAMES.items():
    # Filter players by position
    position_players = [p for p in available_players if p["element_type"] == position]
    
    # Sort players by their calculated score
    position_players_sorted = sorted(
        position_players, key=lambda p: calculate_player_score(p, team_strengths), reverse=True
    )
    
    # Select top N players
    top_players_by_position[name] = position_players_sorted[:TOP_N]

# Display the top players for each position
for position, players in top_players_by_position.items():
    print(f"\nTop {TOP_N} {position}:\n")
    for player in players:
        print(
            f"{player['web_name']} (Team: {teams[player['team']]}, "
            f"Cost: £{player['now_cost'] / 10:.1f}m, "
            f"Points: {player['total_points']}, "
            f"Form: {player['form']})"
        )
