import requests

def get_teams():
    action = requests.get("https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams")
    teams = action.json()['sports'][0]['leagues'][0]['teams']
    return teams