import requests
import polars as pl
import logging

def create_player_dict(team : dict) -> dict:
    """
    Creates a dictionary of player information for a given list of teams.

    Args:
        teams (list[dict]): A list of team dictionaries, where each dictionary contains team information.

    Returns:
        dict: A dictionary mapping player IDs to their information, including 'id', 'name', and 'position' for each offensive player on the first team in the list.
    """
    team_id = team['team']['id']
    players_info = {}
    roster = requests.get(f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}/roster")
    active_players = roster.json()['athletes']
    for position in active_players:
        if position['position'] == 'offense':
            players = position['items']
            for player in players:
                players_info[player['id']]= {'id': player['id'], 'name': player['displayName'], 'position': player['position']['abbreviation']}
    return players_info

def get_stats(stats : dict, player_id : str, player_name : str, player_position : str) -> pl.DataFrame:
    """
    Extracts and formats player statistics from a given stats dictionary.

    Args:
        stats (dict): A dictionary containing player statistics, typically from the ESPN API.
        player_id (str): The unique identifier for the player.
        player_name (str): The display name of the player.
        player_position (str): The position abbreviation of the player (e.g., 'RB', 'WR').

    Returns:
        pl.DataFrame: A Polars DataFrame containing the player's statistics, with additional columns for player name, position, and ID.
    """
    try:
        df = pl.DataFrame(stats['stats'])
        df = df.drop(['displayName', 'shortDisplayName', 'abbreviation', 'displayValue', 'description', 'rankDisplayValue', 'displayValue', 'perGameValue', 'perGameDisplayValue', 'rank'], strict=False)
        metrics_dict = dict(zip(df["name"], df["value"]))
        results = pl.DataFrame([metrics_dict])
        final = results.with_columns(
            pl.lit(player_name).alias("player_name"),
            pl.lit(player_position).alias("position"),
            pl.lit(player_id).alias("player_id")
            )
        return final
    except pl.exceptions.ColumnNotFoundError as e:
        logging.error(e)
        logging.error(df.columns)

def get_player_stats(player_info : dict, year : int) -> pl.DataFrame:
    """
    Fetches and aggregates rushing and receiving statistics for a list of NFL players for a given season.

    Args:
        player_ids (list[str]): A list of player IDs for which to retrieve statistics.
        player_info (dict): A dictionary mapping player IDs to their information, including 'name' and 'position'.
        year (int): The NFL season year for which to fetch player statistics.

    Returns:
        tuple[pl.DataFrame, pl.DataFrame]: 
            - The first DataFrame contains rushing statistics for the specified players.
            - The second DataFrame contains receiving statistics for the specified players.
    """
    rushing_df = pl.DataFrame()
    receiving_df = pl.DataFrame()
    player_ids = list(player_info.keys())
    for player_id in player_ids:
        season_stats = requests.get(f"http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{year}/types/2/athletes/{player_id}/statistics/0?lang=en&region=us")
        try:
            player_name = player_info[player_id]['name']
            player_position = player_info[player_id]['position']
            player_id = player_info[player_id]['id']
            for stats in season_stats.json().get('splits').get('categories'):
                if stats['displayName'] == 'Rushing' and (player_info[player_id]['position'] in ['RB', 'QB', 'WR', 'TE']):
                    final = get_stats(stats, player_id, player_name, player_position)
                    rushing_df = pl.concat([rushing_df, final])
                if stats['displayName'] == 'Receiving' and (player_info[player_id]['position'] in ['WR', 'TE', 'RB']):
                    final_receiving = get_stats(stats, player_id, player_name, player_position)
                    receiving_df = pl.concat([receiving_df, final_receiving])
        except AttributeError:
            logging.error(f"No stats for {player_name}")
    return rushing_df, receiving_df

# if __name__ == "__main__":
#     import teams
#     team_dict = teams.get_teams()
#     print(type(team_dict))
#     player_info = create_player_dict(team_dict)
#     player_ids = list(player_info.keys())
#     rushing_df, receiving_df = get_player_stats(player_ids, player_info, 2024)
#     print(rushing_df.head(10))
#     print(receiving_df.head(10))