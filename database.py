import duckdb
import logging
import polars as pl
import os
from dotenv import load_dotenv

load_dotenv()
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")


def write_to_db(df: pl.DataFrame, table_name: str, id_field: str):
    with duckdb.connect(f'md:nfl_data?motherduck_token={MOTHERDUCK_TOKEN}') as conn:
        table_exists = conn.execute(f"""
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.tables 
                    WHERE table_name = '{table_name}'
                )
            """).fetchone()[0]
        
        if not table_exists:
                logging.info(f"Creating {table_name}...")
                conn.execute(f"""
                    CREATE TABLE {table_name} AS 
                    SELECT * FROM df
                """)
        else:
            logging.info(f"Updating {table_name}...")
            # Register the DataFrame as a temporary table
            conn.register('temp_df', df)
            
            # Use UPSERT (INSERT OR REPLACE) approach
            # First, delete existing records for players in the new data
            conn.execute(f"""
                DELETE FROM {table_name} 
                WHERE {id_field} IN (SELECT {id_field} FROM temp_df)
            """)
            
            # Then insert all the new data (this handles both new and updated players)
            conn.execute(f"""
                INSERT INTO {table_name} 
                SELECT * FROM temp_df
            """)
        conn.commit()
        logging.info(f"Successfully populated {table_name}...")

def get_from_db(query: str) -> pl.DataFrame:
    with duckdb.connect(f'md:nfl_data?motherduck_token={MOTHERDUCK_TOKEN}') as conn:
        return conn.execute(query).pl()
    
def write_to_game_db(df: pl.DataFrame, table_name: str):
    with duckdb.connect(f'md:nfl_data?motherduck_token={MOTHERDUCK_TOKEN}') as conn:
        table_exists = conn.execute(f"""
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.tables 
                    WHERE table_name = '{table_name}'
                )
                """).fetchone()[0]
        if not table_exists:
                logging.info(f"Creating {table_name}...")
                conn.execute(f"""
                    CREATE TABLE {table_name} AS 
                    SELECT * FROM df
                """)
        else:
            logging.info(f"Updating {table_name}...")
            # Register the DataFrame as a temporary table
            conn.register('temp_df', df)

            # Use UPSERT (INSERT OR REPLACE) approach
            # First, delete existing records for events in the new data
            conn.execute(f"""
                DELETE FROM {table_name} 
                WHERE game_id IN (SELECT game_id FROM temp_df)
            """)
            
            # Then insert all the new data (this handles both new and updated events)
            conn.execute(f"""
                INSERT INTO {table_name} 
                SELECT * FROM temp_df
            """)
        conn.commit()
        logging.info(f"Successfully populated {table_name}...")


def insert_into_db(df: pl.DataFrame, table_name: str):
    with duckdb.connect(f'md:nfl_data?motherduck_token={MOTHERDUCK_TOKEN}') as conn:
        table_exists = conn.execute(f"""
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.tables 
                    WHERE table_name = '{table_name}'
                )
                """).fetchone()[0]
        if not table_exists:
            logging.info(f"Creating {table_name}...")
            conn.execute(f"""
                CREATE TABLE {table_name} AS 
                SELECT * FROM df
            """)
        else:
            logging.info(f"Inserting into {table_name}...")
            # Register the DataFrame as a temporary table
            conn.register('temp_df', df)
            
            # Insert only records where game_id doesn't already exist
            conn.execute(f"""
                INSERT INTO {table_name} 
                SELECT * FROM temp_df
                WHERE game_id NOT IN (SELECT game_id FROM {table_name})
            """)
            conn.commit()
            logging.info(f"Successfully populated {table_name}...")

if __name__ == "__main__":
    import nfl_data_py as nfl
    import duckdb
    from database import MOTHERDUCK_TOKEN   

    df=nfl.import_ids()
    df = df.get(["espn_id", "merge_name"])
    df = df.dropna()
    df = df.astype({"espn_id": int})
    df = pl.from_pandas(df)
    df = df.rename({"merge_name": "player_name", "espn_id": "player_id"})

    # snap_counts_df = nfl.import_snap_counts([2024,2025])
    # polars_df = pl.from_pandas(snap_counts_df)
    # polars_df = polars_df.select("season", "week", "team", "position", "player", "offense_snaps", "offense_pct", "defense_snaps", "defense_pct")
    # snap_count_df = polars_df.rename({
    #     "offense_pct": "offense_snap_pct",
    #     "defense_pct": "defense_snap_pct",
    #     "player": "player_name",
    #     "week": "game_week",
    # })

    # Connect to a local DuckDB database file
    with duckdb.connect('nfl_data_local.db') as conn:
        snap_count_df = conn.sql("Select * from snap_counts").pl()
        # print(snap_count_df.filter(pl.col("player_name").str.contains("Metcalf")).select("player_name"))
        gamelog_df = conn.sql("Select * from nfl_player_gamelog_test").pl()
        gamelog_df = gamelog_df.with_columns(pl.col("player_id").cast(pl.Int64))
        print(gamelog_df.filter(pl.col("player_name")=='Lamar Jackson'))

        snap_count_df = snap_count_df.with_columns(pl.col("player_name").str.replace_all(r"\.", "").alias("player_name"))
        snap_count_df = snap_count_df.with_columns(pl.col("player_name").str.to_lowercase().alias("player_name"))
        snap_count_df = snap_count_df.join(df, on="player_name", how="left")
        snap_count_df = snap_count_df.select("season","game_week","player_id", "offense_snaps", "defense_snaps")
        # print(snap_count_df.filter(pl.col("player_id")==4360423))
        # gamelog_df = gamelog_df.with_columns(pl.col("player_id").cast(pl.Int64))
        gamelog_df = gamelog_df.join(snap_count_df, on=["player_id", "season", "game_week"], how="left")
        gamelog_df = gamelog_df.select("player_id", "player_name", "season", "game_week", "game_date", "game_id", "offense_snaps", "defense_snaps")
        print(gamelog_df.filter(pl.col("player_id")==3916387))