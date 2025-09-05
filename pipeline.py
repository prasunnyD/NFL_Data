from prefect import flow, task
import players
import teams
import os
import database
from dotenv import load_dotenv
import polars as pl


@task
def populate_player_overall_stats():
    team_df = database.get_from_db( "select DISTINCT team_name from nfl_roster_db")
    for team in team_df:
        player_info = players.create_player_dict(team)
        rushing_df, receiving_df = players.get_player_stats_async_sync(player_info, 2024)
        database.write_to_db(rushing_df, "nfl_rushing_db")
        database.write_to_db(receiving_df, "nfl_receiving_db")

@task
def process_position_gamelog(position: str, player_ids: list, table_name: str, final_df: pl.DataFrame, season: int):
    """Process gamelog for a specific position"""
    if player_ids:
        gamelog_df = players.get_multiple_player_gamelogs_sync(player_ids, season=season)
        if gamelog_df is None or gamelog_df.is_empty():
            return f"No {position} players to process"
        if gamelog_df.shape[0] == 0:
            return f"No {position} players to process"
        return gamelog_df
    return f"No {position} players to process"

@task
def populate_player_gamelog():
    player_df = database.get_from_db("select DISTINCT player_id, player_name, position from nfl_roster_db")
    
    # Group players by position
    position_groups = ['QB', 'RB', 'WR', 'TE']
    season = 2024
    
    # Create tasks for each position that can run in parallel
    tasks = []
    for position in position_groups:
        position_df = player_df.filter(pl.col("position") == position)
        player_tuples = list(position_df.select("player_id", "player_name").iter_rows())
        task = process_position_gamelog.submit(position, player_tuples, "", pl.DataFrame(), season)
        tasks.append(task)
    
    # Collect only the DataFrames, filter out strings
    gamelogs_df = []
    for task in tasks:
        result = task.result()
        if isinstance(result, pl.DataFrame):  # Only add DataFrames
            gamelogs_df.append(result)
    
    # Only concatenate if we have DataFrames
    if gamelogs_df:
        final_df = pl.concat(gamelogs_df, how="diagonal")
        database.write_to_db(final_df, "nfl_player_gamelog")

@task
def populate_roster():
    team_dict = teams.get_teams()
    for team in team_dict:
        roster = teams.get_roster(team)
        database.write_to_db(roster, "nfl_roster")

@task
def populate_game_events():
    for year in range(2021, 2025):
        event_df = teams.get_game_events(year)
        database.write_to_game_db(event_df, "nfl_games")

@task
def populate_team_stats():
    team_df = database.get_from_db("select DISTINCT team_name, team_id from nfl_roster_db")
    
    # Collect all stats first (avoid database writes in loops)
    all_stats = []
    team_names = team_df['team_name'].to_list()
    team_ids = team_df['team_id'].to_list()
    
    for team_name, team_id in zip(team_names, team_ids):
        team_stats_df = teams.get_team_stats(team_id, 2024)
        
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
            
            # Write to database
            database.write_to_db(pivoted_stats, f"nfl_team_{category}_stats_db")

@flow
def main():
    populate_player_gamelog()


if __name__ == "__main__":
    main()
    