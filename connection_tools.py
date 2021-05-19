import os
from os.path import join as jn
import pickle

from numpy import *
from numpy.linalg import norm
from scipy.optimize import minimize_scalar
from scipy.special import erfinv
import pandas as pd
import tempfile
from pathlib import Path

from contextlib import contextmanager
from importlib.machinery import SourceFileLoader
#from geopy.distance import great_circle as distance_func
import datetime as dt

from .general_tools import mysql_server, ssh_client_connection, yes

class EmptyConnection(dict):
	def __init__(self):
		super().__init__()
		self['type'] = None

def extract_ssh_parameters(profile):
	kwargs={}
	name = profile
	import Mercury.libs.general_tools3 as general_tools3
	pouet = os.path.dirname(os.path.dirname(os.path.abspath(general_tools3.__file__)))
	cred = SourceFileLoader(name, jn(pouet, name + '.py')).load_module()
	cred.__dir__()

	kwargs['ssh_port'] = kwargs.get('port', 22)
	
	if not 'ssh_tunnel' in kwargs.keys() or kwargs['ssh_tunnel'] is None:
		if 'ssh_username' in cred.__dir__():
			ssh_auth = cred.__getattribute__('ssh_auth')
			kwargs['ssh_parameters'] = kwargs.get('ssh_parameters', {})
			if ssh_auth=='password':
				for par in ['ssh_username', 'ssh_password', 'ssh_hostname']:
					kwargs['ssh_parameters'][par] = kwargs['ssh_parameters'].get(par, cred.__getattribute__(par))	
			elif ssh_auth=='key':
				for par in ['ssh_username', 'ssh_key_password', 'ssh_pkey', 'ssh_hostname']:
					kwargs['ssh_parameters'][par] = kwargs['ssh_parameters'].get(par, cred.__getattribute__(par))	
				kwargs['allow_agent'] = True

	return kwargs

@contextmanager
def generic_connection(typ=None, connection=None, profile=None, path_profile=None, **kwargs):
	"""
	This a wrapper to uniformise remote (or local) connections for 
	files and databases.
	"""
	if typ=='mysql':
		with mysql_connection(profile=profile, connection=connection, path_profile=path_profile, **kwargs) as connection:
			yield connection
	elif typ=='file':
		with file_connection(profile=profile, connection=connection, **kwargs) as connection:
			yield connection
	elif typ is None:
		yield EmptyConnection()
	else:
		raise Exception('Type of connection', typ, 'is not supported.')

@contextmanager
def mysql_connection(connection=None, profile=None, path_profile=None, **kwargs):
	"""
	profile can be any string corresponding to a file 'db_profile_credentials.py'

	profile is ignored if a non-None engine is passed in argument.

	Usage:

	mysql_connection(connection=something)
	-> passes down connection. All other parameters are ignored.
	
	mysql_connection(profile='something') 
	-> opens connection with profile. All other parameters except connection are ignored.

	mysql_connection(username=..., password=...)
	-> creates a connection with these parameters

	mysql_connection()
	-> creates an empty connection {'type':'mysql', 'engine':None, 'ssh_tunnel':None}
	"""
	if not connection is None:
		yield connection
	else:
		if not profile is None:
			if not 'engine' in kwargs.keys() or kwargs['engine'] is None:
				name = profile + '_credentials'
				if path_profile	is None:
					import Mercury.libs.general_tools3 as general_tools3
					path_profile = os.path.dirname(os.path.dirname(os.path.abspath(general_tools3.__file__)))

				cred = SourceFileLoader(name, jn(path_profile, name + '.py')).load_module()

				for par in ['hostname', 'username', 'password', 'database']:
					kwargs[par] = kwargs.get(par, cred.__getattribute__(par))
				kwargs['port'] = kwargs.get('port', 3306)
				try:
					kwargs['connector']=cred.__getattribute__('mysql_connector')
				except:
					kwargs['connector'] = 'mysqldb'

				print ('DB connection to', kwargs['hostname'], end=" ")

				if not 'ssh_tunnel' in kwargs.keys() or kwargs['ssh_tunnel'] is None:
					if 'ssh_username' in cred.__dir__():
						ssh_auth = cred.__getattribute__('ssh_auth')
						kwargs['ssh_parameters'] = kwargs.get('ssh_parameters', {})
						if ssh_auth=='password':
							for par in ['ssh_username', 'ssh_password', 'ssh_hostname']:
								kwargs['ssh_parameters'][par] = kwargs['ssh_parameters'].get(par, cred.__getattribute__(par))	
						elif ssh_auth=='key':
							for par in ['ssh_username', 'ssh_key_password', 'ssh_pkey', 'ssh_hostname']:
								kwargs['ssh_parameters'][par] = kwargs['ssh_parameters'].get(par, cred.__getattribute__(par))	
							kwargs['allow_agent'] = True

						print ('with ssh tunneling through', kwargs['ssh_parameters']['ssh_hostname'])
					else:
						print ()
		
		if len(kwargs)==0:
			yield {'type':'mysql', 'engine':None, 'ssh_tunnel':None}
		else:
			with mysql_server(**kwargs) as mysql_connection:
				mysql_connection['type'] = 'mysql'
				yield mysql_connection

@contextmanager
def file_connection(connection=None, profile=None, base_path=None, **kwargs):
	"""
	To uniformise with mysql connection
	profile can be any string corresponding to a file 'db_profile_credentials.py'

	profile is ignored if a non-None engine is passed in argument.
	"""
	if not connection is None:
		yield connection
	else:
		if not profile is None:
			name = profile + '_credentials'
			import Mercury.libs.general_tools3 as general_tools3
			pouet = os.path.dirname(os.path.dirname(os.path.abspath(general_tools3.__file__)))
			cred = SourceFileLoader(name, jn(pouet, name + '.py')).load_module()

			if base_path is None:
				try:
					base_path = cred.__getattribute__('base_path')
				except AttributeError:
					base_path=''
				except:
					raise
		
		if not profile is None and profile!='local':

			for par in ['ssh_hostname', 'ssh_username', 'ssh_password', 'ssh_pkey', 'ssh_key_password']:
				if not par in kwargs.keys() and hasattr(cred, par):
					kwargs[par] = cred.__getattribute__(par)

			with ssh_client_connection(**kwargs) as ssh_connection:
				#connection['connection_type'] = 'ssh'
				connection = {'ssh_connection':ssh_connection,
							'type':'file',
							'base_path':base_path}
				yield connection

		else:
			connection = {'ssh_connection':None,
							'type':'file',
							'base_path':base_path}
			yield connection

generic_names = {'RNG':'output_RNG.csv.gz', 'sim_general':'output_sim_general.csv.gz',
				'flights':'output_flights.csv.gz', 'pax':'output_pax.csv.gz',
				'swaps':'output_swaps.csv.gz', 'eaman':'output_eaman.csv.gz', 'dci':'output_dci.csv.gz',
				'events':'output_events.csv.gz', 'messages':'output_messages.csv.gz'}

def get_data_csv(model_version=None, profile=None, n_iters=None, scenario=None, fil='flights',
	generic_names=generic_names, rep='/home/ldel/domino_output/csv_output'):
	"""
	High level function to get csv files on the server for model version >=1.25
	
	Note: one cannot pass a connection object coming from mysql_server here,
	they are not the same kind of objects.
	"""
	with ssh_connection(profile=profile) as ssh_connection_engine:
		dfs = []
		for i in n_iters:
			print ('Trying to read iteration', i)
			file_names = {k:str(model_version)+"_"+str(scenario)+"_"+str(i)+"_"+file_name for k, file_name in generic_names.items()}

			try:
				df = read_data(jn(rep,file_names[fil]),
									profile=profile,
									compression='gzip',
									which='csv',
									index_col=0,
									ssh_connection_engine=ssh_connection_engine)
				for st in ['aobt', 'sobt', 'aibt', 'sibt']:
					if st in df.columns:
						df[st] = pd.to_datetime(df.aobt)
				dfs.append(df)
			except FileNotFoundError:
				print ('Iteration not found')
				pass
			except:
				raise
			#df.head()
		df = pd.concat(dfs)
		
	return df

def read_data(fmt=None, connection=None, profile=None, **kwargs):
	"""
	Wrapper designed to have uniformed input reading.

	Parameters
	==========
	fmt: string
		either 'mysql', 'csv', or 'pickle'
	connection: dictionary
		
	kwargs_mysql: dictionary
		additional key arguments to be passed to read_mysql.
		Put select=None and query='something' to directly pass a query.

	Returns
	=======
	df: pandas Dataframe

	"""

	if not connection is None:
		if connection['type']=='mysql':
			fmt = 'mysql'
		else:
			if fmt is None:
				if 'file_name' in kwargs.keys():
					print (kwargs['file_name'].split('.')[-1])
				
					if kwargs['file_name'].split('.')[-1]=='csv':
						fmt = 'csv'
					elif kwargs['file_name'].split('.')[-1]=='pic':
						fmt = 'pickle'
					else:
						raise Exception("I could not guess the data format for", kwargs['file_name'], 'you need to pass it manually with fmt=')
	else:
		if fmt is None:
			if 'file_name' in kwargs.keys():
				#print (kwargs['file_name'])
				print (kwargs['file_name'].split('.')[-1])
				if kwargs['file_name'].split('.')[-1]=='csv':
					fmt = 'csv'
				elif kwargs['file_name'].split('.')[-1]=='pic':
					fmt = 'pickle'
				else:
					raise Exception("I could not guess the data format for", kwargs['file_name'], 'you need to pass it manually with fmt=')

	if fmt=='mysql':
		df = read_mysql(connection=connection, profile=profile, **kwargs)
	elif fmt=='csv':
		df = read_csv(connection=connection, profile=profile, **kwargs)
	elif fmt=='pickle':
		df = read_pickle(connection=connection, profile=profile, **kwargs)
	else:
		raise Exception('Unknown format mode:', fmt)

	return df

def read_csv(file_name='', path='', connection=None, profile=None, **other_paras):
	# if profile is None:
	# 	profile = 'local'

	with file_connection(connection=connection, profile=profile) as my_file_connection:
		ppath = Path(path)
		if ppath.anchor!='/' and not my_file_connection['base_path'] is None:
			ppath = Path(my_file_connection['base_path']) / path

		full_path = ppath / file_name
		if not my_file_connection['ssh_connection'] is None:
			# file is on a remote server, read it with sftp
			sftp_client = my_file_connection['ssh_connection'].open_sftp()
			with sftp_client.open(str(full_path)) as remote_file:
				df = pd.read_csv(remote_file, **other_paras)
		else:
			# file is local, read it directly.
			df = pd.read_csv(full_path, **other_paras)

	return df

def do_query(sql, con):
	# This section is required, otherwise the the execute part below fails...
	try:
		con.run_callable(
			con.dialect.has_table, sql, None
		)
	except Exception:
		# using generic exception to catch errors from sql drivers (GH24988)
		_is_table_name = False

	rs = con.execute(sql)

	return rs
	
def run_mysql_query(query, connection=None, profile=None, **options):
	"""
	This function whould be used only for queries that do not return data, e.g. table creation.
	Indeed, the output of the function is unstructured, on the contrary of read_mysql
	"""
	with mysql_connection(connection=connection, profile=profile) as connection:
		engine = connection['engine']

		rs = do_query(query, engine, **options)

	return rs

def read_pickle(file_name='', path='', connection=None, profile=None, byte=True, **garbage):
	"""
	"""
	if byte:
		mode = 'rb'
	else:
		mode = 'r'

	# if profile is None:
	# 	profile = 'local'

	#print (connection)
	with file_connection(connection=connection, profile=profile) as my_file_connection:
		ppath = Path(path)
		if ppath.anchor!='/' and not my_file_connection['base_path'] is None:
			ppath = Path(my_file_connection['base_path']) / path

		full_path = ppath / file_name
		if not my_file_connection['ssh_connection'] is None:
			# file is on a remote server, read it with sftp
			# folder creation on remote server.
			sftp_client = my_file_connection['ssh_connection'].open_sftp()
			with sftp_client.open(str(full_path)) as remote_file:
				df = pickle.load(remote_file)
		else:
			# file is local, read it directly.

			with open(full_path, mode) as f:
				df = pickle.load(f)
			
	return df

def read_mysql(select=None, fromm=None, conditions={}, query=None, connection=None, index_col=None, profile=None, **options):
	"""
	Read something from sql. 'select', 'fromm', and 'conditions' can be used for quick and dirty queries, but more 
	complex ones should be done with 'query' directly by setting 'select' to 'None'

	Parameters
	==========
	select: list of string
		name of attribute to get. If None, set to '*'. Ignored if query is not None.
	fromm: string
		table name to query. Ignored if query is not None.
	conditions: dictionnary
		keys are name of attributes and values are values to be matched by the attributes.
		Ignored if query is not None.
	query: string
		full query for the database. If None, attributes 'select', 'fromm' and 'conditions' are used.
	engine: sqlalchemy engine object
		If given then it is used to do the connection,
		if missing then it is created based on user, password, address and
		db_name from db_connection
	options: dictionnary
		options to be passed on to the sqlalchemy engine.

	Returns
	=======
	df: pandas Dataframe
		with results

	"""
	with mysql_connection(connection=connection, profile=profile) as connection:
		engine = connection['engine']
		if query is None:
			assert not fromm is None
			if select is None:
				select = '*' 
			elif type(select)==list:
				select_new = ''
				for stuff in select:
					select_new += stuff + ', '
				select = select_new[:-2]
			elif select=='*':
				pass
			else:
				pass
				#raise Exception()

			query = 'SELECT ' + select + ' FROM ' + fromm + ' WHERE '
			for k, v in conditions.items():
				if type(v) in[unicode, str]:
					v = '"' + v + '"'
				else:
					v = str(v)
				query += k + '=' + v + ' AND '

			if conditions=={}:
				query = query[:-7]
			else:
				query = query[:-5]


		if index_col is not None:
			df = pd.read_sql(query, engine, index_col=index_col,**options)
		else:
			df = pd.read_sql(query, engine,**options)

		return df

def write_data(data, fmt=None, connection=None, profile=None, **kwargs):

	"""
	Wrapper designed to have uniformed input writing.

	Parameters
	==========
	what: pandas Dataframe object,
		to be written
	where: string,
		name of file to get the data from if which='csv' or 'pickle'.
		Name of table otherwise.
	how: string,
		either 'update', 'replace', or 'append'. Define how the data should be added.
		'update' only works with 'mysql' right now.
	which: string,
		either 'mysql', 'csv', or 'pickle'.
	kwargs_mysql: dictionary
		additional key arguments to be passed to write_sql.

	"""
	if not connection is None:
		if connection['type']=='mysql':
			fmt = 'mysql'
		else:
			if fmt is None:
				if 'file_name' in kwargs.keys():
					if kwargs['file_name'].split('.')[-1]=='csv':
						fmt = 'csv'
					elif kwargs['file_name'].split('.')[-1]=='pic':
						fmt = 'pickle'
					else:
						raise Exception("I could not guess the data format for", kwargs['file_name'], 'you need to pass it manually with fmt=')
	else:
		if fmt is None:
			if 'file_name' in kwargs.keys():
				#print (kwargs['file_name'])
				if kwargs['file_name'].split('.')[-1]=='csv':
					fmt = 'csv'
				elif kwargs['file_name'].split('.')[-1]=='pic':
					fmt = 'pickle'
				else:
					raise Exception("I could not guess the data format for", kwargs['file_name'], 'you need to pass it manually with fmt=')

	if fmt=='mysql':
		#write_mysql(what, where, how=how, connection=connection, **kwargs_extra)
		write_mysql(data=data, connection=connection, profile=profile, **kwargs)
	elif fmt=='csv':
		write_csv(data=data, connection=connection, profile=profile, **kwargs)
	elif fmt=='pickle':
		write_pickle(data=data, connection=connection, profile=profile, **kwargs)
	else:
		raise Exception('Unknown format mode:', fmt)

def write_csv(data=None, file_name='',  path='', connection=None, profile=None,
	how='replace', create_folder=True, **other_paras):
	"""
	"""

	if how!='replace':
		print ('You chose to save in csv with mode', how)
		if yes("This is not implemented yet, shall I switch to 'replace'?"):
			how = 'replace'
		else:
			raise Exception('Aborted')

	if how=='replace':
		# if profile is None:
		# 	profile = 'local'

		with file_connection(connection=connection, profile=profile) as my_file_connection:
			ppath = Path(path)
			if ppath.anchor!='/' and not my_file_connection['base_path'] is None:
				ppath = Path(my_file_connection['base_path']) / path

			full_path = ppath / file_name

			if create_folder:
				full_path.parent.mkdir(parents=True,
										exist_ok=True)

			if not my_file_connection['ssh_connection'] is None:
				# file is on a remote server, write it with sftp
				sftp_client = my_file_connection['ssh_connection'].open_sftp()
				with sftp_client.open(str(full_path), mode='w') as remote_file:
					data.to_csv(remote_file, **other_paras)
			else:
				# file is local, write it directly.
				data.to_csv(full_path, **other_paras)
	else:
		raise Exception('Not implemented yet')

def write_pickle(data=None, file_name='',  path='', connection=None, profile=None,
	how='replace', create_folder=True, **other_paras):
	
	if how!='replace':
		print ('You chose to save in csv with mode', how)
		if yes("This is not implemented yet, shall I switch to 'replace'?"):
			how = 'replace'
		else:
			raise Exception('Aborted')

	if how=='replace':
		if byte:
			mode = 'wb'
		else:
			mode = 'w'

		# if profile is None:
		# 	profile = 'local'
		with file_connection(connection=connection, profile=profile) as my_file_connection:
			ppath = Path(path)
			if ppath.anchor!='/' and not my_file_connection['base_path'] is None:
				ppath = Path(my_file_connection['base_path']) / path

			full_path = ppath / file_name

			if create_folder:
				full_path.parent.mkdir(parents=True,
										exist_ok=True)

			if not my_file_connection['ssh_connection'] is None:
				# file is on a remote server, write it with sftp
				sftp_client = my_file_connection['ssh_connection'].open_sftp()
				with sftp_client.open(str(full_path), mode='w') as remote_file:
					#df = pd.read_csv(remote_file, **other_paras)
					df = pickle.dump(data, remote_file)
			else:
				# file is local, write it directly.
				with open(full_path, mode) as f:
					pickle.dump(data, f)
	else:
		raise Exception('Not implemented yet')

def create_indexes_in_table(engine, table, primary={}, indexes={}):
	if not primary and not indexes:
		#Need at least one
		return

	sql = "ALTEr TABLE "+table
	
	for k, v in primary.items():
		sql = sql+" CHANGE COLUMN "+k+" "+k+" "+v+" NOT NULL, "

	for k, v in indexes.items():
		sql = sql + " ADD INDEX " + k + " ("
		for i in v:
			sql += (i+" ASC, ")

		sql = sql[:-2]
		sql += "),"

	if bool(primary):
		sql += " ADD PRIMARY KEY ("
		for k in primary.keys():
			sql += k+","
		sql = sql[:-1]
		sql += "),"

	sql = sql[:-1]
	
	engine.execute(sql)

def write_mysql(data=None, table_name=None, how='update', key_for_update='id', 
	keys_for_update={}, connection=None, primary_dict={},
	profile=None, hard_update=False, index=False, use_temp_csv=False):
	"""
	
	Write a dataframe in the mysql database

	Parameters
	==========
	data: pandas DataFrame object,
		stuff to be put in database.
	table_name: string,
		name of table in database.
	how: string,
		either 'update', 'replace', or 'append'.
	key_for_update: string, int, or float,
		name of key for table row matching.
	engine: sqlalchemy engine object 
		If given then it is used to do the connection,
		if missing then it is created based on default parameters
	
	"""

	with mysql_connection(connection=connection, profile=profile) as connection:
		engine = connection['engine']
		
		create_primary_keys = not engine.dialect.has_table(engine, table_name)

		if how == 'replace':
			question = 'You chose to replace the following database table in output:\n'
			question += ' - ' + table_name + '\n'
			question += 'Are you sure?'
			if not yes(question):
				if yes("Should I switch to 'update'? (You can still save in cvs if you say no)"):
					how = 'update'
				else:
					if yes("Do you want to save the results in cvs? (Run is aborted otherwise)"):
						file_name = input('Type a name for the csv file:\n')
						data.to_csv(file_name)
					else:
						raise Exception('Aborted')

		if how != 'update':
			data.to_sql(table_name, engine, if_exists=how, index=index)
		else:
			if hard_update:
				# Remove all entries with attributes matching the ones given in keys_for_update.
				# TODO: This is slow and stupid, use mysql 'SET' command
				if engine.dialect.has_table(engine, table_name):
					with engine.connect() as con:
						query = 'DELETE FROM ' + table_name + ' WHERE '
						for key, value in keys_for_update.items():
							if type(value) is str:
								query +=  key + '="' + str(value) + '" AND '
							else:
								query +=  key + '=' + str(value) + ' AND '
						rs = con.execute(query[:-5])

					# Check if all columns are in database
					df_test = read_mysql(query="SELECT * FROM " + table_name + " LIMIT 1",
										connection=connection)
					for col in data.columns:
						if not str(col) in df_test:
							mask = ~pd.isnull(data[col])
							if type(data.loc[mask, col].iloc[0]) in [float, float64]:
								typ = 'FLOAT'
							elif type(data.loc[mask, col].iloc[0]) in [int, int64]:
								typ = 'INT'
							elif type(data.loc[mask, col].iloc[0]) in [str, unicode]:
								typ = 'VARCHAR(100)'
							# elif type(data.loc[mask, col].iloc[0]) in [list, tuple]:
							# 	max_car = max([len(data.loc[mask, col].iloc[i]) for i in range(len(data.loc[mask, col]))])
							# 	typ = 'VARCHAR(' + str(max_car*10) + ')'
							else:
								print ('Column:', col)
								raise Exception('Not sure which type of variable I should use for:', type(data[col].iloc[0]))
							
							query = "ALTER TABLE " + str(table_name) + " ADD COLUMN `" +\
									str(col) + "` " + typ

							print ('Attempting to create new column with query:', query)
							engine.execute(query)

				if use_temp_csv:
					load_data_infile(engine, data, table_name)
				else:
					data.to_sql(table_name, engine, if_exists='append', index=index)
			else:
				_update_table(data, table_name, key_for_update, engine=engine)

	if create_primary_keys:
		create_indexes_in_table(engine=engine,
								table=table_name,
								primary=primary_dict,
								indexes=index)

def _update_table(new_table, table_name, key_for_update, engine=None):
	with mysql_connection(engine=engine, profile='remote') as connection:
		# Get the existing table
		sql = """SELECT * FROM """ + table_name
		dff = read_mysql(query=sql, engine=connection['engine'])
		
		mask = new_table[key_for_update].isin(dff[key_for_update])
		
		# Add columns missing in DB
		query = "ALTER TABLE " + table_name
		l = len(query)
		for col in new_table.columns:
			if not col in dff.columns:
				typ = type(new_table[col].iloc[0])
				if typ in [int, int64]:
					type_sql = 'INT'
				elif typ in [float, float64]:
					type_sql = 'FLOAT'
				elif typ in [str, unicode]:
					type_sql = 'VARCHAR(255)'
				else:
					print (typ)
					raise Exception()
					
				query += ' ADD ' + col + ' ' + type_sql + ','
				
		if len(query)>l:
			query = query[:-1]
			
			connection['engine'].execute(query)
		
		# Update existing rows
		for idx in new_table[mask].index:
			query = "UPDATE " + table_name + " SET"

			for col in new_table.columns:
				if col!=key_for_update:
					value = new_table.loc[idx, col]
					if type(value) in [str, unicode]:
						value = '"' + value + '"'
					elif pd.isnull(value):
						value = 'NULL'
					else:
						value = str(value)
					query += " " + col + "=" + value +  ","

			query = query[:-1]
			query += " WHERE " + key_for_update + "=" + str(new_table.loc[idx, key_for_update])

			connection['engine'].execute(query)
			
		# Append other rows
		mask = ~new_table[key_for_update].isin(dff[key_for_update])
		new_table[mask].to_sql(table_name, connection['engine'], if_exists='append', index=False)        

#Load data using cvs files
def load_data_infile(engine, data, table, columns=None, drop_table=False, create_table=False, deactivate_checks_keys=False):
	if drop_table:
		engine.execute("DROP TABLE IF EXISTS " + table)
		create_table = True

	if columns is None:
		columns = data.columns.tolist()

	if create_table:
		data.loc[data.index[0:1], ].to_sql(table, engine, index=False, if_exists="replace")
		engine.execute("TRUNCATE "+table)

	temp = tempfile.NamedTemporaryFile()

	try:
		data[columns].to_csv(temp.name, index=False, header=True, sep=",", doublequote=True, encoding='utf-8', na_rep="\\N")

		if deactivate_checks_keys:
			engine.execute('set autocommit = 0;')
			engine.execute('set unique_checks = 0;')
			engine.execute('set foreign_key_checks = 0')

		columns=["`"+str(c)+"`" for c in columns]
		
		sql = "LOAD DATA LOCAL INFILE '" + temp.name + "'"\
			   + " INTO TABLE "+table \
			   + " FIELDS TERMINATED BY ','"\
			   + " LINES TERMINATED BY '\\n'"\
			   + " IGNORE 1 LINES"\
			   + " (" + str(columns).replace('[', '').replace(']', '').replace('\'', '')+");"

		#print()
		#print("-------")
		#print(sql)
		#print("-------")

		engine.execute(sql)
		engine.execute('commit;')

	finally:
		if deactivate_checks_keys:
			engine.execute('set autocommit = 1;')
			engine.execute('set unique_checks = 1;')
			engine.execute('set foreign_key_checks = 1')

		temp.close()