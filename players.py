import requests
import polars as pl
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
import asyncio
import aiohttp
from database import get_from_db
import nfl_data_py as nfl
import duckdb
from database import MOTHERDUCK_TOKEN

logging.basicConfig(level=logging.INFO) # Set logging level to INFO to see all messages

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


async def get_player_stats_async(player_df: pl.DataFrame, year: int, max_concurrent: int = 50) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Async version using DataFrame directly - most efficient approach.
    """
    rushing_data = []
    receiving_data = []
    passing_data = []
    
    # Define position groups for efficiency
    rushing_positions = {'RB', 'QB', 'WR', 'TE'}
    receiving_positions = {'WR', 'TE', 'RB'}
    
    async def fetch_player_stats(session: aiohttp.ClientSession, row: dict) -> tuple[list, list, list]:
        """Fetch stats for a single player asynchronously."""
        player_rushing = []
        player_receiving = []
        player_passing = []
        
        try:
            player_id = row['player_id']
            player_name = row['player_name']
            player_position = row['position']
            
            url = f"http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{year}/types/2/athletes/{player_id}/statistics/0?lang=en&region=us"
            
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                response.raise_for_status()
                data = await response.json()
            
            categories = data.get('splits', {}).get('categories', [])
            
            for stats in categories:
                display_name = stats.get('displayName', '')
                
                if (display_name == 'Rushing' and player_position in rushing_positions):
                    stats_df = get_stats(stats, player_id, player_name, player_position)
                    if stats_df is not None:
                        player_rushing.append(stats_df)
                
                elif (display_name == 'Receiving' and player_position in receiving_positions):
                    stats_df = get_stats(stats, player_id, player_name, player_position)
                    if stats_df is not None:
                        player_receiving.append(stats_df)
                
                elif (display_name == 'Passing' and player_position == 'QB'):
                    stats_df = get_stats(stats, player_id, player_name, player_position)
                    if stats_df is not None:
                        player_passing.append(stats_df)
                        
        except Exception as e:
            logging.error(f"Error fetching stats for {row.get('player_name', 'Unknown')} (ID: {row.get('player_id', 'Unknown')}): {e}")
        
        return player_rushing, player_receiving, player_passing
    
    # Create semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def fetch_with_semaphore(session: aiohttp.ClientSession, row: dict):
        async with semaphore:
            return await fetch_player_stats(session, row)
    
    # Process all players asynchronously using DataFrame rows directly
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_with_semaphore(session, row)
            for row in player_df.iter_rows(named=True)  # Direct DataFrame iteration
        ]
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Task failed with exception: {result}")
            else:
                player_rushing, player_receiving, player_passing = result
                rushing_data.extend(player_rushing)
                receiving_data.extend(player_receiving)
                passing_data.extend(player_passing)
    
    # Batch concatenate all data
    rushing_df = pl.concat(rushing_data) if rushing_data else pl.DataFrame()
    receiving_df = pl.concat(receiving_data) if receiving_data else pl.DataFrame()
    passing_df = pl.concat(passing_data, how="diagonal") if passing_data else pl.DataFrame()
    
    return rushing_df, receiving_df, passing_df


def get_player_stats_async_sync(player_df: pl.DataFrame, year: int, max_concurrent: int = 50) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Synchronous wrapper for the async version using DataFrame directly.
    Much more efficient than dict conversion.
    """
    return asyncio.run(get_player_stats_async(player_df, year, max_concurrent))


async def get_player_gamelog_async(player_id: str, player_name: str, player_position: str, season: int) -> pl.DataFrame:
    """
    Async version of get_player_gamelog using aiohttp.
    """
    async with aiohttp.ClientSession() as session:
        url = f"https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/{player_id}/gamelog?season={season}"
        
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                response.raise_for_status()
                data = await response.json()
            
            stat_names = data['names']
            
            # Navigate to events more safely
            seasonTypes = data.get('seasonTypes',[{}])
            for seasonType in seasonTypes:
                if seasonType.get('displayName') == f'{season} Regular Season':
                    events = (seasonType.get('categories', [{}])[0]
                            .get('events', []))
            
                    # Use list comprehension - efficient and readable for this use case
                    dict_list = [
                        {
                            **dict(zip(stat_names, boxscore['stats'])),
                            "game_id": boxscore['eventId']
                        }
                        for boxscore in events
                    ]
                    
                    boxscore_df = pl.DataFrame(dict_list)
                    boxscore_df = boxscore_df.with_columns(pl.lit(player_id).alias("player_id"))
                    boxscore_df = boxscore_df.with_columns(pl.lit(player_name).alias("player_name"))
                    boxscore_df = boxscore_df.with_columns(pl.lit(player_position).alias("position"))
                    return boxscore_df

            logging.debug(f"No {season} Regular Season found")
            return pl.DataFrame()
            
        except (KeyError, IndexError) as e:
            return pl.DataFrame()
        except aiohttp.ClientError as e:
            return pl.DataFrame()


async def get_multiple_player_gamelogs_async(player_ids: list[tuple[str, str, str]], max_concurrent: int = 50, season: int = 2024) -> pl.DataFrame:
    """
    Fetch gamelog data for multiple players concurrently using async.
    Much more efficient than fetching one by one.
    
    Args:
        player_ids (list[tuple[str, str]]): List of (player_id, player_name) tuples to fetch gamelog data for
        max_concurrent (int): Maximum number of concurrent requests
        
    Returns:
        pl.DataFrame: Combined gamelog data for all players
    """
    if not player_ids:
        return pl.DataFrame()
    
    # Create semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def fetch_with_semaphore(player_id: str, player_name: str, player_position: str, season: int) -> pl.DataFrame:
        async with semaphore:
            return await get_player_gamelog_async(player_id, player_name, player_position, season)
    
    # Create tasks for all players
    tasks = [fetch_with_semaphore(player_id, player_name, player_position, season) for player_id, player_name, player_position in player_ids]
    
    # Execute all tasks concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Combine results, filtering out errors
    valid_dataframes = []
    players = []
    columns = []
    count_19 = 0
    count_20 = 0
    count_18 = 0
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            pass
        elif not result.is_empty() or None:
            logging.info(f"Result: {result}")
            if result.width == 19:
                count_19 += 1
                valid_dataframes.append(result)
            elif result.width == 20:
                players.append(player_ids[i])
                columns.append(result.columns)
                valid_dataframes.append(result)
                count_20 += 1
            else:
                count_18 += 1
                valid_dataframes.append(result)
    
    # Concatenate all valid results
    if valid_dataframes:
        logging.info(f"Valid dataframes: {len(valid_dataframes)}")
        try:
            return pl.concat(valid_dataframes, how="diagonal")
        except pl.exceptions.ShapeError as e:
            print(f"Count 19: {count_19}")
            print(f"Count 20: {count_20}, {players}, {columns}")
            print(f"Count 18: {count_18}")
            logging.error(f"Error concatenating dataframes: {e}")
    else:
        return pl.DataFrame()


def get_multiple_player_gamelogs_sync(player_ids: list[tuple[str, str, str]], max_concurrent: int = 50, season: int = 2024) -> pl.DataFrame:
    """
    Synchronous wrapper for the async version.
    Use this if you want async performance but need a sync interface.
    """
    return asyncio.run(get_multiple_player_gamelogs_async(player_ids, max_concurrent, season))

def snap_counts_to_df(season: int) -> pl.DataFrame:

    df=nfl.import_ids()
    df = df.get(["espn_id", "merge_name"])
    df = df.dropna()
    df = df.astype({"espn_id": int})
    df = pl.from_pandas(df)
    df = df.rename({"merge_name": "player_name", "espn_id": "player_id"})

    snap_counts_df = nfl.import_snap_counts([season])
    polars_df = pl.from_pandas(snap_counts_df)
    polars_df = polars_df.select("season", "week", "team", "position", "player", "offense_snaps", "offense_pct", "defense_snaps", "defense_pct")
    snap_count_df = polars_df.rename({
        "offense_pct": "offense_snap_pct",
        "defense_pct": "defense_snap_pct",
        "player": "player_name",
        "week": "game_week",
    })
    snap_count_df = snap_count_df.with_columns(pl.col("player_name").str.replace_all(r"\.", "").alias("player_name"))
    snap_count_df = snap_count_df.with_columns(pl.col("player_name").str.to_lowercase().alias("player_name"))
    snap_count_df = snap_count_df.join(df, on="player_name", how="left")
    snap_count_df = snap_count_df.with_columns(pl.col("player_id").cast(pl.String))

    return snap_count_df


if __name__ == "__main__":
    import asyncio
    import pandas as pd


    async def test_gamelog():
        df = await get_multiple_player_gamelogs_async(player_ids=[('3916387', 'Lamar Jackson', 'QB'), ('4047650', 'DK Metcalf', 'WR')], season=2025)
        df = df.select("player_name", "position", "rushingYards", "rushingAttempts", "receivingYards")
        return df
    
    # Run the async function
    df = asyncio.run(test_gamelog())
    print(df)
