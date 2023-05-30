from flask import Flask, request
from flask_cors import CORS
import json
from src.database import Database, timer
from src.constants import SCHEMA, TEAMSCHEMA

app = Flask(__name__)
CORS(app, origins="*")

db = Database("players", "name", schema=SCHEMA)
db.create_table(override=False, testing=False)

team_db = Database('teams', 'id', schema=TEAMSCHEMA)

@app.route("/")
@timer
def home():
	return "<h1>Homepage. Please refer to /addTeam, /getTeams, /addPlayers, /getPlayers for player info</h1>"

@app.route("/addPlayer", methods=["POST"])
@timer
def add_players():
	data = request.get_json()
	print(f"[INFO] add_players requested with data {data}")
	db.add_entry(data)
	return "<h1>Success!</h1>"

@app.route("/getTeams", methods=["GET"])
@timer
def get_teams():
	return json.dumps(team_db.get_all_entries())

@app.route("/getPlayers", methods=["GET"])
@timer
def get_players():
	return json.dumps(db.get_all_entries())

@app.route("/player/<name>", methods=["GET"])
@timer
def get_player(name: str):
	return json.dumps(db.select('*', 'name', '=', name)[0])

@app.route("/byTeam/<teamID>", methods=["GET"])
@timer
def get_players_by_team(teamID: int):
	return json.dumps(db.select('*', 'team_id', '=', teamID, limit=25))

@app.route("/getTeamIDs")
@timer
def get_teams_ids():
	return json.dumps(team_db.select('id', '', '', '', distinct=True))

@app.route("/get25teams")
@timer
def get_25_teams():
	teamSelect = team_db.select('*', '', '', '', distinct=True, limit=25)
	ids = map(lambda x: x['id'], teamSelect)
	teams = {t['id'] : {'team': t, 'players': []} for t in teamSelect}
	for id in ids:
		teams[id]['players'] = db.select('*', 'team_id', '=', id, limit=20)
	return json.dumps(teams)

if __name__ == "__main__":
	try:
		app.run(debug=True, host="0.0.0.0", port=3000)
	except:
		db.conn.close()