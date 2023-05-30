import random
import string
import time

from src.database import Database
from src.constants import SCHEMA, TEAMSCHEMA

db = Database('players', 'name', schema=SCHEMA)

db.create_table(override=True)

db.table_name = 'teams'

db.SCHEMA = TEAMSCHEMA
db.create_table(override=True)

db.table_name = 'players'
db.SCHEMA = SCHEMA

team_ids = []

def generate_random_player(schema):
	player = {}
	for field, data_type in schema.items():
		if data_type == 'TEXT':
			player[field] = ''.join(random.choices(string.ascii_letters, k=5)) # Creates a random 5-letter field.
		elif data_type == 'INTEGER':
			if field == 'team_id':
				team_id = random.randint(0, 50)
				player[field] = team_id # team_id should go between 1 and 50
				team_ids.append(team_id)
			else:
				player[field] = random.randint(0, 10)
	return player

start_time = time.time()

for i in range(200000):
	print(f'sending player {i}..., time remaining: {(time.time() - start_time) / (i + 1) * 200000}')
	start = time.time()
	payload = generate_random_player(SCHEMA)
	db.add_entry(payload)

print("Finished doing players. Now teams...")

db.table_name = 'teams'
db.SCHEMA = TEAMSCHEMA

team_ids = set(team_ids)

for i, id in enumerate(team_ids):
	print(f"Adding team {i}/{len(team_ids)}")
	team = {
		'id': id,
		'city': ''.join(random.choices(string.ascii_letters, k=5)),
		'name': ''.join(random.choices(string.ascii_letters, k=3)),
		'image': 'data:image/png;base64, iVBORw0KGgoAAAANSUhEUgAAAAUAAAAFCAYAAACNbyblAAAAHElEQVQI12P4//8/w38GIAXDIBKE0DHxgljNBAAO9TXL0Y4OHwAAAABJRU5ErkJggg==',
		'animal': random.choice(['lions', 'tigers', 'elephants', 'giraffes', 'monkeys', 'pandas', 'zebras', 'koalas', 'kangaroos', 'hippos', 'crocodiles', 'rhinos', 'penguins', 'whales', 'dolphins', 'octopi'])
	}
	db.add_entry(team)