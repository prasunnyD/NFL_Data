from prefect import flow, task
import players
import teams
import os
import database
from dotenv import load_dotenv
import polars as pl
import logging
import duckdb


@task
def populate_player_overall_stats():
    # Get all players at once - no conversion needed
    player_df = database.get_from_db("select * from nfl_roster_db")
    
    # Process all players at once with DataFrame directly
    rushing_df, receiving_df, passing_df = players.get_player_stats_async_sync(player_df, 2025)
    
    # Write all data to database
    if not rushing_df.is_empty():
        database.write_to_db(rushing_df, "nfl_rushing_db", "player_id")
    if not receiving_df.is_empty():
        database.write_to_db(receiving_df, "nfl_receiving_db", "player_id")
    if not passing_df.is_empty():
        database.write_to_db(passing_df, "nfl_passing_db", "player_id")

@task
def populate_player_gamelog():
    import nfl_data_py as nfl

    player_df = database.get_from_db("select DISTINCT player_id, player_name, position from nfl_roster_db")
    
    # Get all players at once
    all_players = list(player_df.select("player_id", "player_name", "position").iter_rows())
    season = 2024
    
    # Process all players with built-in concurrency
    gamelog_df = players.get_multiple_player_gamelogs_sync(all_players, season=season)
    games_df = database.get_from_db("select game_id, game_date::date as game_date, game_week::int as game_week from nfl_games")
    gamelog_df = gamelog_df.join(games_df, on=["game_id"])

    gamelog_df = gamelog_df.with_columns([
            pl.col("game_date").dt.year().alias("season")
        ])
    # gamelog_df = gamelog_df.with_columns(pl.col("player_id").cast(pl.Int64))

    snap_counts_df = players.snap_counts_to_df(season)
    snap_counts_df = snap_counts_df.drop("player_name")
    gamelog_df = gamelog_df.join(snap_counts_df, on=["player_id", "season", "game_week"], how="left")
    
    if not gamelog_df.is_empty():
        database.insert_into_db(gamelog_df, "nfl_player_gamelog")

@task
def populate_roster():
    team_dict = teams.get_teams()
    for team in team_dict:
        roster = teams.get_roster(team)
        database.write_to_db(roster, "nfl_roster", "playerid")

@task
def populate_game_events():
    for year in range(2021, 2026):
        event_df = teams.get_game_events(year)
        database.write_to_game_db(event_df, "nfl_games")

@task
def populate_team_stats():
    import duckdb
    team_df = database.get_from_db("select DISTINCT team_name, team_id from nfl_roster_db")
    
    # Collect all stats first (avoid database writes in loops)
    all_stats = []
    team_names = team_df['team_name'].to_list()
    team_ids = team_df['team_id'].to_list()
    
    for team_name, team_id in zip(team_names, team_ids):
        team_stats_df = teams.get_team_stats(team_id, 2025)
        
        # Add team identifiers to the DataFrame
        team_stats_df = team_stats_df.with_columns([
            pl.lit(team_name).alias("team_name"),
            pl.lit(team_id).alias("team_id")
        ])
        
        all_stats.append(team_stats_df)
    
    # Concatenate all DataFrames
    if all_stats:
        combined_stats = pl.concat(all_stats)
        
        # Group by category and write each category to its own table
        categories = combined_stats['category'].unique().to_list()
        
        for category in categories:
            category_stats = combined_stats.filter(pl.col("category") == category)
            
            # Pivot the data to have stat names as columns
            pivoted_stats = category_stats.pivot(
                values="stat_value",
                index=["team_name", "team_id"],
                columns="stat_name"
            )
            if category == "defensive":
                pivoted_stats = teams.add_team_defense_advanced_stats(pivoted_stats)
            
            # Write to database
            database.write_to_db(pivoted_stats, f"nfl_team_{category}_stats_db", "team_id")

@flow
def main():
    populate_team_stats()
    


if __name__ == "__main__":
    main()
    