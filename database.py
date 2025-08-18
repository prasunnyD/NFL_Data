import duckdb
import logging
import polars as pl
import os
from dotenv import load_dotenv

load_dotenv()
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")


def write_to_db(df: pl.DataFrame, table_name: str):
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
                WHERE player_id IN (SELECT player_id FROM temp_df)
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
