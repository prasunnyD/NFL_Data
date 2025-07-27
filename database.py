import duckdb

MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
with duckdb.connect(f"md:nba_data?motherduck_token={MOTHERDUCK_TOKEN}") as conn: