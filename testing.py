import nfl_data_py as nfl
import polars as pl
import players
import duckdb


if __name__ == "__main__":

    df=nfl.import_ids()
    df = df.get(["espn_id", "merge_name"])
    df = df.dropna()
    df = df.astype({"espn_id": int})
    df = pl.from_pandas(df)
    df = df.rename({"merge_name": "player_name", "espn_id": "player_id"})

    snap_counts_df = nfl.import_snap_counts([2024,2025])
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
    snap_count_df = snap_count_df.select("season","game_week","player_id", "offense_snaps", "defense_snaps")
    
    with duckdb.connect('nfl_data_local.db') as conn:
        gamelog_df = conn.sql("Select * from nfl_player_gamelog_test").pl()
        gamelog_df = gamelog_df.with_columns(pl.col("player_id").cast(pl.Int64))
        gamelog_df = gamelog_df.join(snap_count_df, on=["player_id", "season", "game_week"], how="left")
        gamelog_df = gamelog_df.select("player_id", "player_name", "season", "game_week", "game_date", "game_id", "offense_snaps", "defense_snaps")
        print(gamelog_df.filter(pl.col("player_id")==3916387))
        



