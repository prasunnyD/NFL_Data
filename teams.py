import requests
import polars as pl
import database
from scraping import PlaywrightScraper

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

def convert_string_columns_to_float(df: pl.DataFrame, exclude_columns: list = None) -> pl.DataFrame:
    """
    Convert string columns to float by removing percentage signs and commas.
    
    Args:
        df (pl.DataFrame): The DataFrame to convert
        exclude_columns (list): List of column names to exclude from conversion
        
    Returns:
        pl.DataFrame: DataFrame with string columns converted to float
    """
    if exclude_columns is None:
        exclude_columns = []
    
    # Find string columns that need conversion
    string_columns = []
    for col in df.columns:
        if col not in exclude_columns and not df[col].dtype.is_numeric():
            string_columns.append(col)
    
    # Convert string columns to float
    if string_columns:
        conversion_expressions = []
        for col in string_columns:
            # Try to convert to float, handling percentage signs and other common string formats
            conversion_expressions.append(
                pl.col(col).str.replace('%', '').str.replace(',', '').cast(pl.Float64, strict=False).alias(col)
            )
        df = df.with_columns(conversion_expressions)
    
    return df

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

def add_team_defense_advanced_stats(df: pl.DataFrame, csv_file : str) -> pl.DataFrame:

        columns_to_rank = ['epa_per_play_allowed', 'success_rate_allowed', 'rush_success_rate_allowed', 'dropback_success_rate_allowed', 'tacklesForLoss', 'sacks', 'stuffs', 'passesDefended',"rush_epa_allowed", "dropback_epa_allowed"]
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
        adv_stats = pl.read_csv(csv_file)
        adv_stats = adv_stats.with_columns(pl.col("Abbr").map_elements(lambda x: team_mapping.get(x, x)).alias("team_name"))
        adv_stats = adv_stats.drop(["Abbr","Team", ""])
        adv_stats = adv_stats.rename({"EPA/play": "epa_per_play_allowed", "Success Rate (SR)": "success_rate_allowed","Rush EPA" : "rush_epa_allowed", "Rush SR": "rush_success_rate_allowed", "Dropback EPA": "dropback_epa_allowed", "Dropback SR": "dropback_success_rate_allowed"})
        df = df.join(adv_stats, on="team_name")
        # Create rank columns for the specified columns
        rank_expressions = []
        for col in columns_to_rank:
            if col in df.columns:
                # For defensive stats, lower is better for yardsAllowed and pointsAllowed
                # Higher is better for sacks, stuffs, and passesDefended
                if col in ['epa_per_play_allowed', "rush_epa_allowed", "dropback_epa_allowed" ,'success_rate_allowed', 'rush_success_rate_allowed', 'dropback_success_rate_allowed']:
                    # Lower values get better ranks (rank 1 = best)
                    rank_expressions.append(pl.col(col).rank(method='min', descending=False).alias(f'{col}_rank'))
                else:
                    # Higher values get better ranks (rank 1 = best)
                    rank_expressions.append(pl.col(col).rank(method='min', descending=True).alias(f'{col}_rank'))

        # Add all rank columns to the original DataFrame
        df_with_ranks = df.with_columns(rank_expressions)
        
        return df_with_ranks
    

def add_team_offense_advanced_stats(csv_file : str) -> pl.DataFrame:

        columns_to_rank = ['epa_per_play', "rush_epa", "dropback_epa", 'success_rate', 'rush_success_rate', 'dropback_success_rate', 'tacklesForLoss', 'sacks', 'stuffs', 'passesDefended']
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
        adv_stats = pl.read_csv(csv_file)
        adv_stats = adv_stats.with_columns(pl.col("Abbr").map_elements(lambda x: team_mapping.get(x, x)).alias("team_name"))
        adv_stats = adv_stats.drop(["Abbr","Team", ""])
        adv_stats = adv_stats.rename({"EPA/play": "epa_per_play", "Success Rate (SR)": "success_rate", "Rush SR": "rush_success_rate", "Dropback SR": "dropback_success_rate", "Rush EPA": "rush_epa", "Dropback EPA": 'dropback_epa'})
        # Create rank columns for the specified columns
        rank_expressions = []
        for col in columns_to_rank:
            if col in adv_stats.columns:
                rank_expressions.append(pl.col(col).rank(method='min', descending=True).alias(f'{col}_rank'))

        # Add all rank columns to the original DataFrame
        adv_stats_with_ranks = adv_stats.with_columns(rank_expressions)
        
        return adv_stats_with_ranks

def sharp_defense_stats() -> pl.DataFrame:
    urls = [ "https://www.sharpfootballanalysis.com/stats-nfl/nfl-defensive-stats/",
            "https://www.sharpfootballanalysis.com/stats-nfl/nfl-defensive-line-stats/",
            "https://www.sharpfootballanalysis.com/stats-nfl/nfl-defensive-tendencies/",
            "https://www.sharpfootballanalysis.com/stats-nfl/nfl-coverage-schemes/",
            ]
    
    dataframes = []
    
    with PlaywrightScraper() as scraper:
        results = scraper.scrape_multiple_urls(urls, wait_time=2)
        for result in results:
            if result.success:
                df = pl.from_pandas(result.dataframe)
                print(f"Scraped columns from {result.url}: {df.columns}")
                dataframes.append(df)
    
    # Handle case where no data was successfully scraped
    if not dataframes:
        return pl.DataFrame()
    
    # Start with the first DataFrame and join subsequent ones
    final_df = dataframes[0]
    for df in dataframes[1:]:
        # Use outer join to keep all teams and all columns
        final_df = final_df.join(df, on="Team")
    
    # Create rank columns for numeric columns only
    rank_expressions = []
    for col in final_df.columns:
        # Skip non-numeric columns and the Team column
        if col == "Team" or not final_df[col].dtype.is_numeric():
            continue
            
        # For defensive stats, lower is better for yardsAllowed and pointsAllowed
        # Higher is better for sacks, stuffs, and passesDefended
        if col in ['Explosive Play Rate Allowed', 'Yards Before Contact Per Rb Rush', 'Yards Per Play Allowed', 'Down Conversion Rate Allowed']:
            # Lower values get better ranks (rank 1 = best)
            rank_expressions.append(pl.col(col).rank(method='min', descending=False).alias(f'{col}_rank'))
        else:
            # Higher values get better ranks (rank 1 = best)
            rank_expressions.append(pl.col(col).rank(method='min', descending=True).alias(f'{col}_rank'))

    # Add all rank columns to the original DataFrame
    if rank_expressions:
        final_df = final_df.with_columns(rank_expressions)
    
    # Clean up column names: replace spaces with underscores and convert to lowercase
    column_mapping = {col: col.replace(' ', '_').lower() for col in final_df.columns}
    final_df = final_df.rename(column_mapping)
    
    return final_df

def sumer_advanced_stats() -> pl.DataFrame:
    urls = [ "https://sumersports.com/teams/defensive/",
            "https://sumersports.com/teams/offensive/",]
    dataframes = []
    with PlaywrightScraper() as scraper:
        results = scraper.scrape_multiple_urls(urls, wait_time=2)
        for result in results:
            if result.success:
                df = pl.from_pandas(result.dataframe)
                print(f"Scraped columns from {result.url}: {df.columns}")
                dataframes.append(df)

    
    defense_df = dataframes[0]
    
    # Convert string columns to float before ranking using reusable function
    defense_df = convert_string_columns_to_float(defense_df, exclude_columns=["Team", "Season"])
    
    # Create rank columns for numeric columns only
    defense_rank_expressions = []
    for col in defense_df.columns:
        # Skip non-numeric columns and the Team column
        if col in ["Team", "Season"]:
            continue
        # For defensive stats, lower is better for yardsAllowed and pointsAllowed
        # Higher is better for sacks, stuffs, and passesDefended
        if col in ['Sack %', 'Int %']:
            # Higher values get better ranks (rank 1 = best)
            
            defense_rank_expressions.append(pl.col(col).rank(method='min', descending=True).alias(f'{col}_rank'))
        else:
            # Lower values get better ranks (rank 1 = best)
            defense_rank_expressions.append(pl.col(col).rank(method='min', descending=False).alias(f'{col}_rank'))
    offense_df = dataframes[1]
    
    # Convert string columns to float before ranking using reusable function
    offense_df = convert_string_columns_to_float(offense_df, exclude_columns=["Team", "Season"])
    
    offense_rank_expressions = []
    for col in offense_df.columns:
        # Skip non-numeric columns and the Team column
        if col in ["Team", "Season"]:
            continue
        if col in ['Sack %', 'Int %']:
            # Lower values get better ranks (rank 1 = best)
            offense_rank_expressions.append(pl.col(col).rank(method='min', descending=False).alias(f'{col}_rank'))
        else:
            # Higher values get better ranks (rank 1 = best)
            offense_rank_expressions.append(pl.col(col).rank(method='min', descending=True).alias(f'{col}_rank'))
            
    defense_df = defense_df.with_columns(defense_rank_expressions)
    column_mapping = {col: col.replace(' ', '_').lower() for col in defense_df.columns}
    defense_df = defense_df.rename(column_mapping)
    # Extract the last word from team names
    defense_df = defense_df.with_columns(
        pl.col('team').str.split(' ').list.last().alias('team')
    )
    offense_df = offense_df.with_columns(offense_rank_expressions)
    column_mapping = {col: col.replace(' ', '_').lower() for col in offense_df.columns}
    offense_df = offense_df.rename(column_mapping)
    # Extract the last word from team names
    offense_df = offense_df.with_columns(
        pl.col('team').str.split(' ').list.last().alias('team')
    )
    return defense_df, offense_df

