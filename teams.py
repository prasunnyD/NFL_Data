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

def add_team_defense_advanced_stats(df: pl.DataFrame) -> pl.DataFrame:

        columns_to_rank = ['epa_per_play_allowed', 'success_rate_allowed', 'rush_success_rate_allowed', 'dropback_success_rate_allowed', 'tacklesForLoss', 'sacks', 'stuffs', 'passesDefended']
        team_mapping = {
            'ARI': 'Cardinals',
            'ATL': 'Falcons',
            'BAL': 'Ravens',
            'BUF': 'Bills',
            'CAR': 'Panthers',
            'CHI': 'Bears',
            'CIN': 'Bengals',
            'CLE': 'Browns',
            'DAL': 'Cowboys',
            'DEN': 'Broncos',
            'DET': 'Lions',
            'GB': 'Packers',
            'HOU': 'Texans',
            'IND': 'Colts',
            'JAX': 'Jaguars',
            'KC': 'Chiefs',
            'LA': 'Rams',
            'LAC': 'Chargers',
            'LV': 'Raiders',
            'MIA': 'Dolphins',
            'MIN': 'Vikings',
            'NE': 'Patriots',
            'NO': 'Saints',
            'NYG': 'Giants',
            'NYJ': 'Jets',
            'PHI': 'Eagles',
            'PIT': 'Steelers',
            'SEA': 'Seahawks',
            'SF': '49ers',
            'TB': 'Buccaneers',
            'TEN': 'Titans',
            'WAS': 'Commanders'
        }
        adv_stats = pl.read_csv("nfl_adv_stats/NFL-2025-week4-defense-stats.csv")
        adv_stats = adv_stats.with_columns(pl.col("Abbr").map_elements(lambda x: team_mapping.get(x, x)).alias("team_name"))
        adv_stats = adv_stats.drop(["Abbr","Team", ""])
        adv_stats = adv_stats.rename({"EPA/play": "epa_per_play_allowed", "Success Rate (SR)": "success_rate_allowed", "Rush SR": "rush_success_rate_allowed", "Dropback SR": "dropback_success_rate_allowed"})
        df = df.join(adv_stats, on="team_name")
        # Create rank columns for the specified columns
        rank_expressions = []
        for col in columns_to_rank:
            if col in df.columns:
                # For defensive stats, lower is better for yardsAllowed and pointsAllowed
                # Higher is better for sacks, stuffs, and passesDefended
                if col in ['epa_per_play_allowed', 'success_rate_allowed', 'rush_success_rate_allowed', 'dropback_success_rate_allowed']:
                    # Lower values get better ranks (rank 1 = best)
                    rank_expressions.append(pl.col(col).rank(method='min', descending=False).alias(f'{col}_rank'))
                else:
                    # Higher values get better ranks (rank 1 = best)
                    rank_expressions.append(pl.col(col).rank(method='min', descending=True).alias(f'{col}_rank'))

        # Add all rank columns to the original DataFrame
        df_with_ranks = df.with_columns(rank_expressions)
        
        return df_with_ranks
    


if __name__ == "__main__":
    
