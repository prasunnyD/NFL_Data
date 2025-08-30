import requests
import polars as pl
import database

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

def get_team_stats(team_id: int, season_year: int) -> pl.DataFrame:
    """
    Fetches team statistics for a given NFL team and season year.

    Args:
        team_id (int): The unique identifier for the NFL team.
        season_year (int): The season year for which to fetch statistics.

    Returns:
        pl.DataFrame: A DataFrame with columns: category, stat_name, stat_value
                    Each row represents a single statistic for a category.

    Example:
        ┌──────────┬────────────┬────────────┐
        │ category │ stat_name  │ stat_value │
        ├──────────┼────────────┼────────────┤
        │ passing  │ yards      │ 3500       │
        │ passing  │ touchdowns │ 25         │
        │ rushing  │ yards      │ 1800       │
        │ rushing  │ touchdowns │ 12         │
        └──────────┴────────────┴────────────┘
    """
    response = requests.get(f"https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season_year}/types/2/teams/{team_id}/statistics")
    response_json = response.json()
    categories = response_json['splits']['categories']
    
    # Pre-allocate lists for better memory efficiency
    category_names = []
    stat_names = []
    stat_values = []
    
    for category in categories:
        category_name = category['name']
        stats = category['stats']
        
        for stat in stats:
            category_names.append(category_name)
            stat_names.append(stat['name'])
            stat_values.append(stat['value'])
    
    # Construct DataFrame once
    return pl.DataFrame({
        'category': category_names,
        'stat_name': stat_names,
        'stat_value': stat_values
    })

def get_game_events(season_year: str) -> pl.DataFrame:
    response = requests.get(f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?limit=1000&dates={season_year}&seasontype=2")
    response_json = response.json()
    events = response_json['events']
    event_ids = []
    event_dates = []
    event_weeks = []
    for event in events:
        event_ids.append(event['id'])
        event_dates.append(event['date'])
        event_weeks.append(event['week']['number'])
    event_df = pl.DataFrame({
        'game_id': event_ids,
        'game_date': event_dates,
        'game_week': event_weeks
    })
    return event_df


if __name__ == "__main__":
    response = requests.get("https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?limit=1000&dates=2023&seasontype=2")
    response_json = response.json()
    events = response_json['events']
    for event in events:
        print(event['id'])
        print(event['date'])
        print(event['week']['number'])

