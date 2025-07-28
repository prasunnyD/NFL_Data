import requests
import polars as pl

def get_teams():
    action = requests.get("https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams")
    teams = action.json()['sports'][0]['leagues'][0]['teams']
    return teams

def get_roster(team: {dict}):
    """Even more efficient version that constructs DataFrame directly"""
    team_id = team['team']['id']
    team_name = team['team']['name']
    roster = requests.get(f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}/roster")
    active_players = roster.json()['athletes']
    
    # Extract all data in parallel using list comprehensions
    player_ids = [player['id'] for position in active_players for player in position['items']]
    player_names = [player['displayName'] for position in active_players for player in position['items']]
    positions = [player['position']['abbreviation'] for position in active_players for player in position['items']]
    team_names = [team_name] * len(player_ids)
    team_ids = [team_id] * len(player_ids)
    
    return pl.DataFrame({
        'player_id': player_ids,
        'player_name': player_names,
        'position': positions,
        'team_name': team_names,
        'team_id': team_ids
    })

if __name__ == "__main__":
    teams = get_teams()
    for team in teams:
        print(team['team']['name'])