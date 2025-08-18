import requests
import polars as pl
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
import asyncio
import aiohttp
from database import get_from_db

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

def get_player_stats(player_info : dict, year : int) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Fetches and aggregates rushing and receiving statistics for a list of NFL players for a given season.

    Args:
        player_info (dict): A dictionary mapping player IDs to their information, including 'name' and 'position'.
        year (int): The NFL season year for which to fetch player statistics.

    Returns:
        tuple[pl.DataFrame, pl.DataFrame]: 
            - The first DataFrame contains rushing statistics for the specified players.
            - The second DataFrame contains receiving statistics for the specified players.
    """
    rushing_data = []
    receiving_data = []
    
    # Define position groups for efficiency
    rushing_positions = {'RB', 'QB', 'WR', 'TE'}
    receiving_positions = {'WR', 'TE', 'RB'}
    
    # Use session for connection pooling
    session = requests.Session()
    
    for player_id, player_data in player_info.items():
        try:
            # Use session for better performance
            response = session.get(
                f"http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{year}/types/2/athletes/{player_id}/statistics/0?lang=en&region=us",
                timeout=10
            )
            response.raise_for_status()
            
            player_name = player_data['name']
            player_position = player_data['position']
            
            # Get categories from response with safer navigation
            data = response.json()
            categories = data.get('splits', {}).get('categories', [])
            
            for stats in categories:
                display_name = stats.get('displayName', '')
                
                # Handle rushing stats
                if (display_name == 'Rushing' and 
                    player_position in rushing_positions):
                    stats_df = get_stats(stats, player_id, player_name, player_position)
                    if stats_df is not None:
                        rushing_data.append(stats_df)
                
                # Handle receiving stats  
                elif (display_name == 'Receiving' and 
                      player_position in receiving_positions):
                    stats_df = get_stats(stats, player_id, player_name, player_position)
                    if stats_df is not None:
                        receiving_data.append(stats_df)
                        
        except (requests.RequestException, KeyError, ValueError) as e:
            logging.error(f"Error fetching stats for {player_data.get('name', 'Unknown')} (ID: {player_id}): {e}")
        except Exception as e:
            logging.error(f"Unexpected error for {player_data.get('name', 'Unknown')}: {e}")
    
    # Batch concatenate all data at once (much more efficient)
    rushing_df = pl.concat(rushing_data) if rushing_data else pl.DataFrame()
    receiving_df = pl.concat(receiving_data) if receiving_data else pl.DataFrame()
    
    return rushing_df, receiving_df


async def get_player_stats_async(player_info : dict, year : int, max_concurrent : int = 50) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Async version of get_player_stats using aiohttp for non-blocking I/O.
    Most efficient for I/O-bound operations like API calls.

    Args:
        player_info (dict): A dictionary mapping player IDs to their information.
        year (int): The NFL season year for which to fetch player statistics.
        max_concurrent (int): Maximum number of concurrent requests.

    Returns:
        tuple[pl.DataFrame, pl.DataFrame]: Rushing and receiving statistics DataFrames.
    """
    rushing_data = []
    receiving_data = []
    
    # Define position groups for efficiency
    rushing_positions = {'RB', 'QB', 'WR', 'TE'}
    receiving_positions = {'WR', 'TE', 'RB'}
    
    async def fetch_player_stats(session: aiohttp.ClientSession, player_id: str, player_data: dict) -> tuple[list, list]:
        """Fetch stats for a single player asynchronously."""
        player_rushing = []
        player_receiving = []
        
        try:
            url = f"http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{year}/types/2/athletes/{player_id}/statistics/0?lang=en&region=us"
            
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                response.raise_for_status()
                data = await response.json()
            
            player_name = player_data['name']
            player_position = player_data['position']
            
            categories = data.get('splits', {}).get('categories', [])
            
            for stats in categories:
                display_name = stats.get('displayName', '')
                
                if (display_name == 'Rushing' and 
                    player_position in rushing_positions):
                    stats_df = get_stats(stats, player_id, player_name, player_position)
                    if stats_df is not None:
                        player_rushing.append(stats_df)
                
                elif (display_name == 'Receiving' and 
                      player_position in receiving_positions):
                    stats_df = get_stats(stats, player_id, player_name, player_position)
                    if stats_df is not None:
                        player_receiving.append(stats_df)
                        
        except Exception as e:
            logging.error(f"Error fetching stats for {player_data.get('name', 'Unknown')} (ID: {player_id}): {e}")
        
        return player_rushing, player_receiving
    
    # Create semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def fetch_with_semaphore(session: aiohttp.ClientSession, player_id: str, player_data: dict):
        async with semaphore:
            return await fetch_player_stats(session, player_id, player_data)
    
    # Process all players asynchronously
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_with_semaphore(session, player_id, player_data)
            for player_id, player_data in player_info.items()
        ]
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Task failed with exception: {result}")
            else:
                player_rushing, player_receiving = result
                rushing_data.extend(player_rushing)
                receiving_data.extend(player_receiving)
    
    # Batch concatenate all data
    rushing_df = pl.concat(rushing_data) if rushing_data else pl.DataFrame()
    receiving_df = pl.concat(receiving_data) if receiving_data else pl.DataFrame()
    
    return rushing_df, receiving_df


def get_player_stats_async_sync(player_info : dict, year : int, max_concurrent : int = 50) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Synchronous wrapper for the async version.
    Use this if you want async performance but need a sync interface.
    """
    return asyncio.run(get_player_stats_async(player_info, year, max_concurrent))


def get_player_gamelog(player_id : str) -> pl.DataFrame:
    # Fetch player gamelog data
    response = requests.get(f"https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/{player_id}/gamelog")
    
    try:
        data = response.json()
        stat_names = data['names']
        
        # Navigate to events more safely
        events = (data.get('seasonTypes', [{}])[0]
                 .get('categories', [{}])[0]
                 .get('events', []))
        
        if not events:
            print("No events found")
            exit()
        
        # Use list comprehension - efficient and readable for this use case
        dict_list = [
            {
                **dict(zip(stat_names, boxscore['stats'])),
                "gameId": boxscore['eventId']
            }
            for boxscore in events
        ]
        
        boxscore_df = pl.DataFrame(dict_list)
        boxscore_df = boxscore_df.with_columns(pl.lit(player_id).alias("playerId"))
        return boxscore_df
        
    except (KeyError, IndexError) as e:
        print(f"Error parsing response data: {e}")
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")

async def get_player_gamelog_async(player_id: str, player_name: str) -> pl.DataFrame:
    """
    Async version of get_player_gamelog using aiohttp.
    """
    async with aiohttp.ClientSession() as session:
        url = f"https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/{player_id}/gamelog"
        
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                response.raise_for_status()
                data = await response.json()
            
            stat_names = data['names']
            
            # Navigate to events more safely
            events = (data.get('seasonTypes', [{}])[0]
                     .get('categories', [{}])[0]
                     .get('events', []))
            
            if not events:
                return pl.DataFrame()
            
            # Use list comprehension - efficient and readable for this use case
            dict_list = [
                {
                    **dict(zip(stat_names, boxscore['stats'])),
                    "gameId": boxscore['eventId']
                }
                for boxscore in events
            ]
            
            boxscore_df = pl.DataFrame(dict_list)
            boxscore_df = boxscore_df.with_columns(pl.lit(player_id).alias("playerId"))
            boxscore_df = boxscore_df.with_columns(pl.lit(player_name).alias("playerName"))
            return boxscore_df
            
        except (KeyError, IndexError) as e:
            logging.error(f"Error parsing response data for player {player_id}: {e}")
            return pl.DataFrame()
        except aiohttp.ClientError as e:
            logging.error(f"Error fetching data for player {player_id}: {e}")
            return pl.DataFrame()


async def get_multiple_player_gamelogs_async(player_ids: list[tuple[str, str]], max_concurrent: int = 50) -> pl.DataFrame:
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
    
    async def fetch_with_semaphore(player_id: str, player_name: str) -> pl.DataFrame:
        async with semaphore:
            return await get_player_gamelog_async(player_id, player_name)
    
    # Create tasks for all players
    tasks = [fetch_with_semaphore(player_id, player_name) for player_id, player_name in player_ids]
    
    # Execute all tasks concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Combine results, filtering out errors
    valid_dataframes = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logging.error(f"Error fetching gamelog for player {player_ids[i]}: {result}")
        elif not result.is_empty():
            if result.width == 19:
                print(result)
            else:
                valid_dataframes.append(result)
    
    # Concatenate all valid results
    if valid_dataframes:
        return pl.concat(valid_dataframes)
    else:
        return pl.DataFrame()


def get_multiple_player_gamelogs_sync(player_ids: list[tuple[str, str]], max_concurrent: int = 50) -> pl.DataFrame:
    """
    Synchronous wrapper for the async version.
    Use this if you want async performance but need a sync interface.
    """
    return asyncio.run(get_multiple_player_gamelogs_async(player_ids, max_concurrent))


if __name__ == "__main__":

    # Example: Fetch gamelog for multiple players efficiently
    
    
    async def main():
        player_df = get_from_db("select DISTINCT player_id from nfl_roster_db where position = 'TE'")
        player_ids = player_df['player_id'].to_list()
        gamelog_df = await get_multiple_player_gamelogs_async(player_ids)
        print(gamelog_df.head(10))
    
    # Run the async demo
    asyncio.run(main())


