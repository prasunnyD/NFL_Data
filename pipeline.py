from prefect import flow, task, serve, get_run_logger
from prefect.schedules import Cron
import players
import teams
import os
import database
from dotenv import load_dotenv
import polars as pl
import duckdb
import requests

load_dotenv()
ODDS_API = os.getenv('ODDS_API')
MOTHERDUCK_TOKEN = os.getenv('MOTHERDUCK_TOKEN')


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

    player_df = database.get_from_db("select DISTINCT player_id, player_name, position from nfl_roster_db where position in ('WR','QB','RB','TE')")
    
    # Get all players at once
    all_players = list(player_df.select("player_id", "player_name", "position").iter_rows())
    season = 2025
    
    # Process all players with built-in concurrency
    gamelog_df, qb_gamelog_df = players.get_multiple_player_gamelogs_sync(all_players, season=season)
    games_df = database.get_from_db("select game_id, game_date::date as game_date, game_week::int as game_week from nfl_games")
    gamelog_df = gamelog_df.join(games_df, on=["game_id"])
    qb_gamelog_df = qb_gamelog_df.join(games_df, on=["game_id"])

    gamelog_df = gamelog_df.with_columns([
            pl.col("game_date").dt.year().alias("season")
        ])
    gamelog_df = gamelog_df.with_columns(pl.col("player_id").cast(pl.Int64))
    qb_gamelog_df = qb_gamelog_df.with_columns([
            pl.col("game_date").dt.year().alias("season")
        ])
    qb_gamelog_df = qb_gamelog_df.with_columns(pl.col("player_id").cast(pl.Int64))

    
    if not gamelog_df.is_empty():
        database.insert_into_db(gamelog_df, "nfl_player_gamelog")
    if not qb_gamelog_df.is_empty():
        database.insert_into_db(qb_gamelog_df, "nfl_qb_gamelog")

@task
def populate_player_snap_counts():
    snap_counts_df = players.snap_counts_to_df([2024, 2025])
    database.write_to_db(snap_counts_df, "nfl_player_snap_counts", "player_id")

@task
def populate_roster():
    team_dict = teams.get_teams()
    for team in team_dict:
        roster = teams.get_roster(team)
        database.write_to_db(roster, "nfl_roster", "player_id")

@task
def populate_game_events():
    for year in range(2021, 2026):
        event_df = teams.get_game_events(year)
        database.write_to_game_db(event_df, "nfl_games")

@task
def populate_team_stats(csv_path):
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
                pivoted_stats = teams.add_team_defense_advanced_stats(pivoted_stats, csv_path)
            elif category in ["passing", "rushing", "receiving"]:
                rank_expressions = []
                for col in pivoted_stats.columns:
                    rank_expressions.append(pl.col(col).rank(method='min', descending=True).alias(f'{col}_rank'))
                pivoted_stats = pivoted_stats.with_columns(rank_expressions)
            
            # Write to database
            database.write_to_db(pivoted_stats, f"nfl_team_{category}_stats_db", "team_id")

@task
def populate_team_advanced_offense_stats(csv_path):
    adv_stats = teams.add_team_offense_advanced_stats(csv_path)
    team_df = database.get_from_db("select DISTINCT team_name, team_id from nfl_roster_db")
    adv_stats = adv_stats.join(team_df, on="team_name", how="left")
    database.write_to_db(adv_stats, "nfl_team_offense_advanced_stats", "team_id")

@task
def populate_player_advanced_stats():
    passing_df = players.get_player_passing_advanced_stats("passing", [2025])
    rushing_df = players.get_player_passing_advanced_stats("rushing", [2025])
    receiving_df = players.get_player_passing_advanced_stats("receiving", [2025])
    database.write_to_db(passing_df, "nfl_player_passing_advanced_stats", "player_id")
    database.write_to_db(rushing_df, "nfl_player_rushing_advanced_stats", "player_id")
    database.write_to_db(receiving_df, "nfl_player_receiving_advanced_stats", "player_id")

@task
def sharp_defense_stats():
    df = teams.sharp_defense_stats()
    database.write_to_db(df, "nfl_sharp_defense_stats", "Team")

@task
def summer_advanced_stats():
    defense_df, offense_df = teams.sumer_advanced_stats()
    database.write_to_db(defense_df, "nfl_sumer_defense_stats", "Team")
    database.write_to_db(offense_df, "nfl_sumer_offense_stats", "Team")

@task
def populate_passing_heat_map():
    pbp_df = players.get_pbp_passing(2025)
    database.write_to_db(pbp_df, "nfl_pbp_qb_data", "game_id")

def get_events() -> list[int]:

    events_url = f"https://api.the-odds-api.com/v4/sports/americanfootball_nfl/events?apiKey={ODDS_API}"
    events = []
    matches = requests.get(events_url).json()
    for match in matches:
        events.append(match["id"])
    return events

@flow()
def get_prop_odds() -> None:
    from collections import defaultdict

    logger = get_run_logger()
    logger.info("Getting Prop Odds...")
    markets = [
        "player_pass_completions",
        "player_pass_attempts",
        "player_pass_yds",
        "player_rush_yds",
        "player_rush_attempts",
        "player_reception_yds",
        "player_receptions",
        "player_reception_longest",
    ]
    events = get_events()

    with duckdb.connect(f"md:nfl_data?motherduck_token={MOTHERDUCK_TOKEN}") as conn:
        table_exists = conn.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = 'nfl_prop_odds'
            )
            """).fetchone()[0]
        for game_id in events:
            for market in markets:
                game_url = f"https://api.the-odds-api.com/v4/sports/americanfootball_nfl/events/{game_id}/odds?apiKey={ODDS_API}&regions=us&markets={market}&oddsFormat=american"
                try:
                    games = requests.get(game_url).json()
                except Exception as e:
                    logger.error(f"Request failed for {game_id}: {e}")
                    continue

                list_of_players = []

                for bookmaker in games["bookmakers"]:
                    market_data = bookmaker["markets"][0]
                    timestamp = market_data["last_update"]
                    sport_book = bookmaker["title"]
                    market_key = market_data["key"]

                    # Group outcomes by (player_name, point_line) since same player can have multiple lines
                    player_lines = defaultdict(dict)

                    for outcome in market_data["outcomes"]:
                        player_name = outcome["description"]
                        logger.info(f"Getting data for {player_name}...")
                        point_line = outcome["point"]
                        over_under = outcome["name"]  # "Over" or "Under"
                        price = outcome["price"]

                        # Use (player, line) as key since some books offer multiple lines per player
                        key = (player_name, point_line)
                        player_lines[key][over_under] = price

                    # Now create a clean dict for each player/line combo
                    for (player_name, point_line), odds in player_lines.items():
                        player_dict = {
                            "timestamp": timestamp,
                            "sport_book": sport_book,
                            "market": market_key,
                            "player": player_name,
                            "line": point_line,
                            "over_odds": odds.get("Over"),
                            "under_odds": odds.get("Under"),
                        }
                        list_of_players.append(player_dict)

                # Convert to DataFrame for easy viewing
                if not list_of_players:
                    logger.warning(f"No odds data for game {game_id}, skipping...")
                    continue

                odds_df = pl.DataFrame(list_of_players)

                if not table_exists:
                    logger.info("Creating new odds table...")
                    conn.execute("""
                        CREATE TABLE nfl_prop_odds AS
                        SELECT * FROM odds_df
                    """)
                    table_exists = True
                else:
                    logger.info("Updating existing nfl_prop_odds table...")
                    # Register the new data as a view
                    conn.register("new_odds", odds_df)

                    # Insert only new records using LEFT JOIN approach instead of NOT EXISTS
                    conn.execute("""
                        INSERT INTO nfl_prop_odds
                        SELECT n.*
                        FROM new_odds n
                    """)

                conn.commit()





@flow
def player_data_pipeline(log_prints=True):
    populate_roster()
    populate_game_events()
    populate_player_snap_counts()
    populate_player_gamelog()
    populate_passing_heat_map()

@flow
def team_data_pipeline(log_prints=True):
    populate_team_stats("nfl_adv_stats/NFL-2025-week14-defense-stats.csv")
    populate_team_advanced_offense_stats("nfl_adv_stats/NFL-2025-week14-offense-stats.csv")
    sharp_defense_stats()
    summer_advanced_stats()



if __name__ == "__main__":
    player_deploy = player_data_pipeline.to_deployment(
        name="nfl-player-data-pipeline",
        schedule=Cron("0 23 * * 3", timezone="America/New_York"),
    )
    team_deploy = team_data_pipeline.to_deployment(
        name="nfl-team-data-pipeline",
        schedule=Cron("0 23 * * 3", timezone="America/New_York"),
    )
    odds_deploy = get_prop_odds.to_deployment(
        name="nfl-odds-pipeline",
        schedule=Cron("5 10-20 * * 1,3,4,6,0", timezone="America/New_York"),
    )
    serve(player_deploy, team_deploy, odds_deploy)

    