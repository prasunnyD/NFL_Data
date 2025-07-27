import pandas as pd
import requests
import polars as pl


def create_player_dict(teams : list[dict]) -> dict:
    team_id = teams[0]['team']['id']
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
        print(e)
        print(df.columns)

def get_player_stats(player_ids : list[str], player_info : dict, year : int) -> pl.DataFrame:
    rushing_df = pl.DataFrame()
    receiving_df = pl.DataFrame()
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
            print(f"No stats for {player_name}")
    return rushing_df, receiving_df