import sqlite3
from src.constants import SCHEMA
import json

from functools import wraps
import time

def timer(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f'Function {func.__name__} Took {total_time:.4f} seconds')
        return result
    return timeit_wrapper

class Database:
	"""
 	The database class. It handles all the I/O for the API
  	"""
	def __init__(self, table_name: str, row_key: str, fileName: str = "data.db", schema:dict = SCHEMA):
		self.conn = sqlite3.connect(fileName, check_same_thread=False)
		self.table_name = table_name
		self.SCHEMA = schema
		self.test_table_name = "test_" + table_name
		self.row_key = row_key

	def create_table(self, override: bool = False, testing: bool = False):
		"""
  		Creates a table if it doesn't exist.
		@params
  		override: bool - if True, wipes the table. If testing, such table will be the testing one. 
		testing: bool - if True, changes table name to its testing one.
		"""
		if testing:
			self.table_name = self.test_table_name

		if override:
			self.conn.execute(f"DROP TABLE IF EXISTS {self.table_name}")
		
		schema_definition = ', '.join(f"{column} {type_}" for column, type_ in self.SCHEMA.items())
		create_table_query = f"CREATE TABLE IF NOT EXISTS {self.table_name} ({schema_definition})"
		self.conn.execute(create_table_query)

	def add_entry(self, element: dict):
		"""
  		Adds an entry to the table.
		@params
  		element: dict - using the same schema as defined in `constants.py`
		"""
		table_columns = ', '.join(self.SCHEMA.keys())
		placeholders = ', '.join('?' * len(self.SCHEMA))
		insert_query = f"INSERT INTO {self.table_name} ({table_columns}) VALUES ({placeholders})"
		
		res = []
		for col in self.SCHEMA.keys():
			try:
				toAdd = element.get(col)
			except:
				toAdd = element.get(col)
			if type(toAdd) == type([]):
				toAdd = json.dumps(toAdd)
			res.append(toAdd)
		
		values = tuple(res)
		self.conn.execute(insert_query, values)
		self.conn.commit()

	def update_entry(self, key: str, updated_data: dict):
		"""
		Updates an entry with given key.
		@params
		key:str - the unique key amongst the elements in the table
		"""
		update_query = f"""UPDATE {self.table_name} SET {', '.join(
				f'{column} = ?' for column in updated_data.keys()
		)} WHERE {self.row_key} = ?"""
		values = tuple(updated_data.values()) + (key,)
		self.conn.execute(update_query, values)
		self.conn.commit()

	def delete_entry(self, key: str):
		"""
  		Deletes the entry with the given key	
		"""
		delete_query = f"DELETE FROM {self.table_name} WHERE {self.row_key} = ?"
		self.conn.execute(delete_query, (key,))
		self.conn.commit()

	def get_all_entries(self):
		"""
  		Gets all entries from the table
		"""
		cur = self.conn.cursor()
		cur.execute(f"SELECT * FROM {self.table_name}")
		rows = cur.fetchall()
		columns = [d[0] for d in cur.description]
		
		res = []
		for row in rows:
			res.append({columns[i]: row[i] for i in range(len(columns))})
		return res

	def select(self, properties:str, where_key:str, where_comparator:str, where_value:str, distinct:bool=False, limit:int=0, random=True):
		"""
  		A simple select statement.
		@params
  		where_key:str - the key to check for
		where_comparator:str - =, >, <, >=, <=
  		where_value:str - what it should be {comparator} to.
		distinct:bool = False - whether you want distinct results or not.
  		
 		EXAMPLE USAGE
   		If I want to execute `SELECT DISTINCT * FROM {table} WHERE money > 50`, I would need to call
	 	.select('*', 'money', '>', 50, distinct=True)
		"""
		cur = self.conn.cursor()
		d = "DISTINCT" if distinct else ""
		l = f"LIMIT {limit}" if limit else ""
		r = "ORDER BY RANDOM()" if random else ""
		if where_key == '' and where_comparator == '' and where_value == '':
			cur.execute(f"SELECT {d} {properties} FROM {self.table_name} {r} {l}")
		else:
			cur.execute(f"SELECT {d} {properties} FROM {self.table_name} WHERE {where_key} {where_comparator} ? {r} {l}", (where_value,))
		rows = cur.fetchall()
		columns = [d[0] for d in cur.description]
		
		res = []
		for row in rows:
			res.append({columns[i]: row[i] for i in range(len(columns))})
		return res

	def team_names(self):
		cur = self.conn.cursor()
		cur.execute(f"SELECT DISTINCT team FROM {self.table_name}")
		rows = cur.fetchall()
		columns = [d[0] for d in cur.description]
		
		res = []
		for row in rows:
			res.append({columns[i]: row[i] for i in range(len(columns))})
		return res