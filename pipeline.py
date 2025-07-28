from prefect import flow, task
import players
import teams
import os
import database
from dotenv import load_dotenv


@task
def populate_player_overall_stats():
    team_dict = teams.get_teams()
    for team in team_dict:
        player_info = players.create_player_dict(team)
        rushing_df, receiving_df = players.get_player_stats(player_info, 2024)
        database.write_to_db(rushing_df, "nfl_rushing_db")
        database.write_to_db(receiving_df, "nfl_receiving_db")

@task
def populate_roster():
    team_dict = teams.get_teams()
    for team in team_dict:
        roster = teams.get_roster(team)
        database.write_to_db(roster, "nfl_roster_db")

@flow
def main():
    populate_roster()


if __name__ == "__main__":
    main()
    