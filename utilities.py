import os
from os.path import join as jn
import pickle

from numpy import *
from copy import copy, deepcopy # keep this after the numpy import!
from numpy.linalg import norm
from scipy.optimize import minimize_scalar
from scipy.special import erfinv
import pandas as pd
from termcolor import colored
import uuid
import tempfile
from pathlib import Path

from sqlalchemy import create_engine
from contextlib import contextmanager
from sshtunnel import SSHTunnelForwarder, open_tunnel
from importlib.machinery import SourceFileLoader
from geopy.distance import great_circle as distance_func
import datetime as dt

from .general_tools3 import mysql_server, ssh_client_connection, yes, loading

from math import atan2

def flight_str(flight_uid):
	return 'Flight' +str(flight_uid)
	
def build_col_print_func(colour, verbose=True, **add_kwargs):

	def my_print(*args, **kwargs):
		kwargs['flush']=False
		args = [colored(arg, colour) for arg in args]
		# Internal kwarg always has precedence, find add_kwargs otherwise
		for k, v in add_kwargs.items():
			if not k in kwargs:
				kwargs[k] = v

		#kwargs['flush']=True
		if verbose:
			print (*args, **kwargs)
		
	return my_print


# verbose = False

alert_print = build_col_print_func('red') # | grep '^[\[31m' --> Ctr-V ESC --> one or another '^[\[32m\|^[\[37m'
# aoc_print = build_col_print_func('green', verbose=verbose) # | grep '^[\[32m'
# nm_print = build_col_print_func('yellow', verbose=verbose) # | grep '^[\[33m'
# flight_print = build_col_print_func('blue', verbose=verbose) # | grep '^[\[34m'
# airport_print = build_col_print_func('magenta', verbose=verbose) # | grep '^[\[35m'
# eaman_print = build_col_print_func('cyan', verbose=verbose) # | grep '^[\[36m'
# dman_print = build_col_print_func('cyan', verbose=verbose) # | grep '^[\[37m'
# """other possible colours: grey"""

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

# @contextmanager
# def ssh_connection(connection=None, profile=None, **kwargs):
# 	"""
# 	profile can be any string corresponding to a file 'db_profile_credentials.py'

# 	profile is ignored if a non-None engine is passed in argument.
# 	"""

# 	if (profile=='local') or (profile is None):
# 		yield {'engine':None}#, 'connection_type':None}
# 	else:
# 		if not 'connection' in kwargs.keys() or kwargs['connection'] is None:
# 			name = profile + '_credentials'
# 			import Mercury.libs.general_tools3 as general_tools3
# 			pouet = os.path.dirname(os.path.dirname(os.path.abspath(general_tools3.__file__)))
# 			cred = SourceFileLoader(name, jn(pouet, name + '.py')).load_module()
# 			#cred.__dir__()

# 			for par in ['ssh_hostname', 'ssh_username', 'ssh_password', 'ssh_pkey', 'ssh_key_password']:
# 				if not par in kwargs.keys() and hasattr(cred, par):
# 					kwargs[par] = cred.__getattribute__(par)

# 		with ssh_client_connection(**kwargs) as connection:
# 			#connection['connection_type'] = 'ssh'
# 			yield connection

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

def distance_euclidean(pt1, pt2):
	return norm(array(pt2)-array(pt1))

def haversine(lon1, lat1, lon2, lat2):
	"""
	Calculate the great circle distance between two points 
	on the earth (specified in decimal degrees)
	----------
	Parameters
	----------
	lon1, lat1 : coordinates point 1 in degrees
	lon2, lat2 : coordinates point 2 in degrees
	-------
	Return
	------
	Distance between points in km
	"""
	# convert decimal degrees to radians 
	lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
	# haversine formula 
	dlon = lon2 - lon1 
	dlat = lat2 - lat1 
	#a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
	#c = 2 * asin(sqrt(a)) 
	#c = 2 * atan2(sqrt(a), sqrt(1-a))
	#km = 6373. * c
	km = 2. * 6373. * arcsin(sqrt(sin(dlat/2.)**2 + cos(lat1) * cos(lat2) * sin(dlon/2.)**2)) 
	return km

def clone_pax(pax, new_n_pax):
	new_pax = deepcopy(pax)

	new_pax.id = uuid.uuid4()
	new_pax.original_id = pax.id
	new_pax.original_n_pax = pax.original_n_pax
	new_pax.n_pax = int(new_n_pax)

	#pax.clones.append(new_pax)

	return new_pax

def scale_and_s_from_quantile_sigma_lognorm(q, m, sig_p):
	"""
	assumes loc = 0.
	"""

	def build_f(sig_p, q, m):
		def f(x):
			return (exp(-x**2) + (sig_p/m)**2 * exp(2. * sqrt(2) * erfinv(2.*q - 1.) * x - 2. * x**2) -1.)**2
		
		return f
		
	f = build_f(sig_p, q, m)

	results = minimize_scalar(f, method='bounded', bounds=(0, sig_p))
	
	if results['fun']>1e-6:
		aprint ('results minimisation:', results)
	
	s = results['x']
	scale =  m * exp(-sqrt(2.) * s * erfinv(2.*q - 1.))
	
	return scale, s

def scale_and_s_from_mean_sigma_lognorm(mu, sig):
	"""
	mu and sig are the mean and sdt of the lognorm, not
	the underlying norm.

	assumes loc = 0.
	"""
	B = 1.+ (sig/mu)**2

	scale = mu/sqrt(B)

	s = sqrt(log(B))

	return scale, s

@contextmanager
def keep_time(obj, key=None):
	start = dt.datetime.now()
	yield
	elapsed = dt.datetime.now() - start

	obj.times[key] = obj.times.get(key, dt.timedelta(0.)) + elapsed

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

# Easy data pulling methods
def get_historical_flights(profile='remote_direct', engine=None):
	with mysql_connection(profile=profile, engine=engine) as connection:
	# Get model output
	# query = "SELECT * FROM output_flights where model_version='" + model_version +\
	# 		"' AND scenario_id=" + str(scenario_id) + " AND n_iter=" + str(n_iter)
	# df = read_mysql(query=query, engine=connection['engine'])

		query = """SELECT
				f.nid,
				ddr.ifps_id,
				f.callsign as flight_number,
				f.registration as tail_number,
				f.origin as origin_airport,
				f.destination as destination_airport,
				origin.mean_taxi_out,
				origin.std_taxi_out,
				destination.mean_taxi_in,
				destination.std_taxi_in,
				f.airline,
				f.airline_type,
				f.sobt,
				ddr.aobt,
				ddr.take_off_time,
				ddr.take_off_time_sch,
				f.sibt,
				ddr.landing_time,
				ddr.landing_time_sch,
				CASE ddr.landing_time
					WHEN NULL THEN NULL
					ELSE ddr.landing_time + INTERVAL mean_taxi_in MINUTE
				END AS aibt,
				ddr.cancelled,
				ddr.distance,
				ddr.distance_sch
				FROM domino_environment.flight_schedule AS f
				JOIN domino_environment.ddr_for_analyses AS ddr ON ddr.ifps_id=f.ifps_id
				JOIN (
					SELECT a_s_o.icao_id, 
							IF(t_o.mean_txo is not NULL, t_o.mean_txo, a_s_o.mean_taxi_out) as mean_taxi_out,
							IF(t_o.std_deviation is not NULL, t_o.std_deviation, a_s_o.std_taxi_out) as std_taxi_out
					FROM domino_environment.airport_info_static AS a_s_o
					LEFT JOIN domino_environment.taxi_out_static AS t_o ON t_o.icao_id=a_s_o.icao_id
					) as origin ON origin.icao_id = f.origin
				JOIN (SELECT a_s_d.icao_id,
							IF(t_i.mean_txi is not NULL, t_i.mean_txi, a_s_d.mean_taxi_in) as mean_taxi_in,
							IF(t_i.std_deviation is not NULL, t_i.std_deviation, a_s_d.std_taxi_in) as std_taxi_in
					FROM domino_environment.airport_info_static AS a_s_d
					LEFT JOIN domino_environment.taxi_in_static AS t_i ON t_i.icao_id=a_s_d.icao_id
					) as destination ON destination.icao_id = f.destination"""
		df_hist = read_mysql(query=query, engine=connection['engine'])

	return df_hist

@loading
def get_simulation_flights(model_version, scenario_id, n_iters, profile='remote_direct2', engine=None):
	if int(model_version.split('.')[1])<25:
		with mysql_connection(profile=profile, engine=engine) as connection:
			query = "SELECT * FROM output_flights where model_version='" + model_version + "' AND scenario_id=" + str(scenario_id) + " AND ("

			for n in n_iters:
				query += 'n_iter=' + str(n) + ' OR '

			query = query[:-4] + ')'

			df = read_mysql(query=query, engine=connection['engine'])
			#print (query)

			return df
	else:
		# Has to create a new ssh connection here
		return get_data_csv(model_version=model_version,
					profile=profile,
					n_iters=n_iters,
					scenario=scenario_id, 
					fil='flights')

@loading
def get_simulation_paxs(model_version, scenario_id, n_iters, profile='remote_direct2', engine=None):
	if int(model_version.split('.')[1])<25:
		with mysql_connection(profile=profile, engine=engine) as connection:
			query = "SELECT * FROM output_pax where model_version='" + model_version +            "' AND scenario_id=" + str(scenario_id) + " AND ("

			for n in n_iters:
				query += 'n_iter=' + str(n) + ' OR '

			query = query[:-4] + ')'

			df_pax = read_mysql(query=query, engine=connection['engine'])
			#print (query)

			return df_pax
	else:
		# Has to create a new ssh connection here
		return get_data_csv(model_version=model_version,
					profile=profile,
					n_iters=n_iters,
					scenario=scenario_id, 
					fil='pax')

def get_pax_schedules(profile='remote_direct', engine=None):
	# Get passenger data
	with mysql_connection(profile=profile, engine=engine) as connection:
		query = "SELECT * FROM pax_itineraries"

		df_pax_sch = read_mysql(query=query, engine=connection['engine'])
		
		return df_pax_sch

def build_single_iteration_df(df, n_iter, profile='remote_direct', engine=None):
	df2 = df[df['n_iter']==n_iter]
	yo = df2.set_index('id').sort_index()

	# Get the flight schedules to have the relationships between id and ifps_id
	with mysql_connection(profile=profile, engine=engine) as connection:
		query = "SELECT fs.nid, fs.ifps_id FROM flight_schedule as fs"

		df_sch = read_mysql(query=query, engine=connection['engine'])
		df_sch.rename(columns={'nid':'id'}, inplace=True)
		df_sch = pd.merge(df_sch, df2, on='id')[['id', 'ifps_id']]
		
	yoyo = df_sch.set_index('id')
	yoyo.sort_index(inplace=True)
	df2 = pd.concat([yo, yoyo],
				   axis=1)

	return df2

def compute_derived_metrics_simulations(df):
	# Computes derived FLIGHT metrics on simulation df
	df['departure_delay'] = (df['aobt']-df['sobt']).dt.total_seconds()/60.
	df['arrival_delay'] = (df['aibt']-df['sibt']).dt.total_seconds()/60.
	df['scheduled_G2G_time'] = (df['sibt']-df['sobt']).dt.total_seconds()/60.
	df['actual_G2G_time'] = (df['aibt']-df['aobt']).dt.total_seconds()/60.
	df['travelling_time_diff'] = ((df['aibt']-df['aobt']) - (df['sibt']-df['sobt'])).dt.total_seconds()/60.
	df['scheduled_flying_time'] = df['m1_fp_time_min']
	df['actual_flying_time'] = df['m3_fp_time_min']
	df['scheduled_flying_distance'] = df['m1_climb_dist_nm'] + df['m1_cruise_dist_nm'] + df['m1_descent_dist_nm']
	df['actual_flying_distance'] = df['m3_climb_dist_nm'] + df['m3_cruise_dist_nm'] + df['m3_descent_dist_nm']

	df['cancelled'] = pd.isnull(df['aobt'])

def compute_derived_metrics_historical(df_hist):
	# Computes derived FLIGHT metrics on historical df
	df_hist['departure_delay'] = (df_hist['aobt']-df_hist['sobt']).dt.total_seconds()/60.
	df_hist['arrival_delay'] = (df_hist['aibt']-df_hist['sibt']).dt.total_seconds()/60.
	df_hist['scheduled_G2G_time'] = (df_hist['sibt']-df_hist['sobt']).dt.total_seconds()/60.
	df_hist['actual_G2G_time'] = (df_hist['aibt']-df_hist['aobt']).dt.total_seconds()/60.
	df_hist['travelling_time_diff'] = ((df_hist['aibt']-df_hist['aobt']) - (df_hist['sibt']-df_hist['sobt'])).dt.total_seconds()/60.
	df_hist['scheduled_flying_time'] = (df_hist['landing_time_sch'] - df_hist['take_off_time_sch']).dt.total_seconds()/60.
	df_hist['actual_flying_time'] = (df_hist['landing_time'] - df_hist['take_off_time']).dt.total_seconds()/60.
	df_hist['taxi_out_traj'] = (df_hist['take_off_time'] - df_hist['aobt']).dt.total_seconds()/60.
	df_hist['taxi_out'] = df_hist['mean_taxi_out']
	df_hist['taxi_in'] = df_hist['mean_taxi_in']
	#df_hist['taxi_in_traj'] = (df_hist['aibt'] - df_hist['landing_time']).dt.total_seconds()/60.
	df_hist['scheduled_flying_distance'] = df_hist['distance_sch']/1.852
	df_hist['actual_flying_distance'] = df_hist['distance']/1.852

# def compute_derived_metrics_pax(df_pf):
# 	# Compute some derived metrics for PAX.
# 	# Only on merge df!
# 	df_pf['origin'] = df_pf.iloc[:].T.apply(find_origin)
# 	df_pf['destination'] = df_pf.iloc[:].T.apply(find_destination)
# 	df_pf['origin_sch'] = df_pf.iloc[:].T.apply(find_origin_sch)
# 	df_pf['destination_sch'] = df_pf.iloc[:].T.apply(find_destination_sch)
# 	df_pf['departure_delay'] = df_pf.iloc[:].T.apply(get_departure_delay)
# 	df_pf['arrival_delay'] = df_pf.iloc[:].T.apply(get_arrival_delay)
# 	df_pf['missed_connection'] = df_pf.T.apply(get_missed_connection)

def compute_metrics_flights(df):
	mets = ['departure_delay', 'arrival_delay', 'scheduled_G2G_time', 'actual_G2G_time', 'scheduled_flying_time', 'actual_flying_time',
			'axot', 'axit', 'travelling_time_diff', 'm3_holding_time']
	
	mets += ['duty_of_care', 'soft_cost', 'transfer_cost', 'compensation_cost', 'non_pax_cost', 'non_pax_curfew_cost', 'fuel_cost_m1', 'fuel_cost_m3', 'crco_cost',
			'total_cost', 'total_cost_wo_fuel']

	dic = {}
	dic['flight_number'] = len(df)
	for met in mets:
		dic[met+'_avg'] = df[met].mean()
		dic[met+'_std'] = df[met].std()
		dic[met+'_90'] = df[met].quantile(0.9)

	dic['fraction_cancelled'] = len(df[pd.isnull(df['aobt'])])/len(df)

	coin = df.copy()
	coin.loc[coin['arrival_delay']<0, 'arrival_delay'] = 0.

	dic['arrival_delay_CODA_avg'] = coin['arrival_delay'].mean()
	dic['arrival_delay_CODA_std'] = coin['arrival_delay'].std()
	dic['arrival_delay_CODA_90'] = coin['arrival_delay'].quantile(0.9)

	# Types of delays
	#tot_delay =  df['departure_delay'].sum()
	dic['reactionary_delay_avg'] = df[df['main_reason_delay']=='RD']['departure_delay'].sum()/len(df)
	dic['turnaround_delay_avg'] = df[df['main_reason_delay']=='TA']['departure_delay'].sum()/len(df)
	dic['atfm_ER_delay_avg'] = df[df['main_reason_delay']=='ER']['departure_delay'].sum()/len(df)
	dic['atfm_A_delay_avg'] = df[df['main_reason_delay']=='C']['departure_delay'].sum()/len(df)
	dic['atfm_W_delay_avg'] = df[df['main_reason_delay']=='W']['departure_delay'].sum()/len(df)

	dic['reactionary_delay_std'] = sqrt((df[df['main_reason_delay']=='RD']['departure_delay']**2).sum()/len(df) - dic['reactionary_delay_avg']**2)
	dic['turnaround_delay_std'] = sqrt((df[df['main_reason_delay']=='TA']['departure_delay']**2).sum()/len(df) - dic['turnaround_delay_avg']**2)
	dic['atfm_ER_delay_std'] = sqrt((df[df['main_reason_delay']=='ER']['departure_delay']**2).sum()/len(df) - dic['atfm_ER_delay_avg']**2)
	dic['atfm_A_delay_std'] = sqrt((df[df['main_reason_delay']=='C']['departure_delay']**2).sum()/len(df) - dic['atfm_A_delay_avg']**2)
	dic['atfm_W_delay_std'] = sqrt((df[df['main_reason_delay']=='W']['departure_delay']**2).sum()/len(df) - dic['atfm_W_delay_avg']**2)

	# TODO: compute real 90 percentile
	dic['reactionary_delay_90'] = df[df['main_reason_delay']=='RD']['departure_delay'].quantile(0.9)
	dic['turnaround_delay_90'] = df[df['main_reason_delay']=='TA']['departure_delay'].quantile(0.9)
	dic['atfm_ER_delay_90'] = df[df['main_reason_delay']=='ER']['departure_delay'].quantile(0.9)
	dic['atfm_A_delay_90'] = df[df['main_reason_delay']=='C']['departure_delay'].quantile(0.9)
	dic['atfm_W_delay_90'] = df[df['main_reason_delay']=='W']['departure_delay'].quantile(0.9)

	c = df['actual_flying_time'] - df['scheduled_flying_time']
	dic['flying_delay_avg'] = c.mean()
	dic['flying_delay_std'] = c.std()
	dic['flying_delay_90'] = c.quantile(0.9)

	c =  df['arrival_delay'] -  df['departure_delay'] - (df['actual_flying_time'] - df['scheduled_flying_time'])
	dic['taxi_delay_avg'] = c.mean()
	dic['taxi_delay_std'] = c.std()
	dic['taxi_delay_90'] = c.quantile(0.9)

	return pd.Series(dic)

def compute_metrics_pax(df_pf, arrival_delay_label='tot_arrival_delay',
	do_single_values=True):

	dic = {}
	dic['group_number'] =  len(df_pf)
	dic['pax_number'] = df_pf['n_pax'].sum()
	
	dic['group_new_number'] = len(df_pf[df_pf['id'].str.len()>=8])
	#coin = df_pf[df_pf['id2'].str.len()<8]
	# dic['group_leg1_different_number'] = len(coin[coin['leg1_sch']!=coin['leg1_act']])
	# dic['group_leg2_different_number'] = len(coin[(coin['leg2_sch']!=coin['leg2_act']) & (~pd.isnull(coin['leg2_sch']) | ~pd.isnull(coin['leg2_act']))])
	# dic['group_leg3_different_number'] = len(coin[(coin['leg3_sch']!=coin['leg3_act']) & (~pd.isnull(coin['leg3_sch']) | ~pd.isnull(coin['leg3_act']))])
	# dic['group_leg4_different_number'] = len(coin[(coin['leg4_sch']!=coin['leg4_act']) & (~pd.isnull(coin['leg4_sch']) | ~pd.isnull(coin['leg4_act']))])

	# mask2 = (coin['leg2_sch']!=coin['leg2_act']) & (~pd.isnull(coin['leg2_sch']) | ~pd.isnull(coin['leg2_act']))
	# mask3 = (coin['leg3_sch']!=coin['leg3_act']) & (~pd.isnull(coin['leg3_sch']) | ~pd.isnull(coin['leg3_act']))
	# mask4 = (coin['leg4_sch']!=coin['leg4_act']) & (~pd.isnull(coin['leg4_sch']) | ~pd.isnull(coin['leg4_act']))

	# mask = mask2 | mask3 | mask4

	# dic['group_it_different_number'] = len(coin[mask])
	# dic['pax_it_different_number'] = coin.loc[mask, 'n_pax'].sum()

	# mask1 = ~pd.isnull(coin['cancelled_leg1_sch']) & coin['cancelled_leg1_sch']
	# mask2 = ~pd.isnull(coin['cancelled_leg2_sch']) & coin['cancelled_leg2_sch']
	# mask3 = ~pd.isnull(coin['cancelled_leg3_sch']) & coin['cancelled_leg3_sch']
	# mask4 = ~pd.isnull(coin['cancelled_leg4_sch']) & coin['cancelled_leg4_sch']

	# mask = mask1 | mask2 | mask3 | mask4

	# dic['group_it_cancelled_leg_number'] = len(coin[mask])
	# dic['pax_it_cancelled_leg_number'] = coin[mask]['n_pax'].sum()

	# Break down with P2P
	mask_p2p = (pd.isnull(df_pf['leg2_sch'])) & (pd.isnull(df_pf['leg2']))
	dic['pax_number_p2p'] = df_pf.loc[mask_p2p, 'n_pax'].sum()
	dic['pax_number_con'] = df_pf.loc[~mask_p2p, 'n_pax'].sum()
	# dic['group_departure_delay_avg'] = df_pf['departure_delay'].mean()
	# dic['group_departure_delay_std'] = df_pf['departure_delay'].std()
	# dic['group_departure_delay_90'] = df_pf['departure_delay'].quantile(0.9)
	
	# dic['group_arrival_delay_avg'] = df_pf['arrival_delay'].mean()
	# dic['group_arrival_delay_std'] = df_pf['arrival_delay'].std()
	# dic['group_arrival_delay_90'] = df_pf['arrival_delay'].quantile(0.9)
	
	# dic['pax_departure_delay_avg'] = (df_pf['departure_delay']*df_pf['n_pax']).sum()/df_pf['n_pax'].sum()
	# dic['pax_departure_delay_std'] = sqrt((df_pf['departure_delay']**2*df_pf['n_pax']).sum()/df_pf['n_pax'].sum() - dic['pax_departure_delay_avg']**2)
	# dic['pax_departure_delay_90'] = df_pf['departure_delay'].quantile(0.9)

	# dic['pax_p2p_departure_delay_avg'] = (df_pf.loc[mask_p2p]['departure_delay']*df_pf.loc[mask_p2p]['n_pax']).sum()/df_pf.loc[mask_p2p]['n_pax'].sum()
	# dic['pax_p2p_departure_delay_std'] = sqrt((df_pf.loc[mask_p2p]['departure_delay']**2*df_pf.loc[mask_p2p]['n_pax']).sum()/df_pf.loc[mask_p2p]['n_pax'].sum() - dic['pax_p2p_departure_delay_avg']**2)
	# dic['pax_p2p_departure_delay_90'] = df_pf.loc[mask_p2p]['departure_delay'].quantile(0.9)

	# dic['pax_con_departure_delay_avg'] = (df_pf.loc[~mask_p2p]['departure_delay']*df_pf.loc[~mask_p2p]['n_pax']).sum()/df_pf.loc[~mask_p2p]['n_pax'].sum()
	# dic['pax_con_departure_delay_std'] = sqrt((df_pf.loc[~mask_p2p]['departure_delay']**2*df_pf.loc[~mask_p2p]['n_pax']).sum()/df_pf.loc[~mask_p2p]['n_pax'].sum() - dic['pax_con_departure_delay_avg']**2)
	# dic['pax_con_departure_delay_90'] = df_pf.loc[~mask_p2p]['departure_delay'].quantile(0.9)
	
	dic['pax_arrival_delay_avg'] = (df_pf[arrival_delay_label]*df_pf['n_pax']).sum()/df_pf['n_pax'].sum()
	dic['pax_arrival_delay_std'] = sqrt((df_pf[arrival_delay_label]**2*df_pf['n_pax']).sum()/df_pf['n_pax'].sum() - dic['pax_arrival_delay_avg']**2)
	dic['pax_arrival_delay_sem'] = dic['pax_arrival_delay_std']/sqrt(df_pf['n_pax'].sum())
	dic['pax_arrival_delay_nb'] = df_pf['n_pax'].sum()
	dic['pax_arrival_delay_nb_gp'] = len(df_pf)
	dic['pax_arrival_delay_90'] = df_pf[arrival_delay_label].quantile(0.9)

	dic['pax_p2p_arrival_delay_avg'] = (df_pf.loc[mask_p2p][arrival_delay_label]*df_pf.loc[mask_p2p]['n_pax']).sum()/df_pf.loc[mask_p2p]['n_pax'].sum()
	dic['pax_p2p_arrival_delay_std'] = sqrt((df_pf.loc[mask_p2p][arrival_delay_label]**2*df_pf.loc[mask_p2p]['n_pax']).sum()/df_pf.loc[mask_p2p]['n_pax'].sum() - dic['pax_p2p_arrival_delay_avg']**2)
	dic['pax_p2p_arrival_delay_sem'] = dic['pax_p2p_arrival_delay_std']/sqrt(df_pf.loc[mask_p2p]['n_pax'].sum())
	dic['pax_p2p_arrival_delay_nb'] = df_pf.loc[mask_p2p, 'n_pax'].sum()
	dic['pax_p2p_arrival_delay_nb_gp'] = len(df_pf.loc[mask_p2p])
	dic['pax_p2p_arrival_delay_90'] = df_pf.loc[mask_p2p][arrival_delay_label].quantile(0.9)

	dic['pax_con_arrival_delay_avg'] = (df_pf.loc[~mask_p2p][arrival_delay_label]*df_pf.loc[~mask_p2p]['n_pax']).sum()/df_pf.loc[~mask_p2p]['n_pax'].sum()
	dic['pax_con_arrival_delay_std'] = sqrt((df_pf.loc[~mask_p2p][arrival_delay_label]**2*df_pf.loc[~mask_p2p]['n_pax']).sum()/df_pf.loc[~mask_p2p]['n_pax'].sum() - dic['pax_con_arrival_delay_avg']**2)
	dic['pax_con_arrival_delay_sem'] = dic['pax_con_arrival_delay_std']/sqrt(df_pf.loc[~mask_p2p]['n_pax'].sum())
	dic['pax_con_arrival_delay_nb'] = df_pf.loc[mask_p2p, 'n_pax'].sum()
	dic['pax_con_arrival_delay_nb_gp'] = len(df_pf.loc[mask_p2p])
	dic['pax_con_arrival_delay_90'] = df_pf.loc[~mask_p2p][arrival_delay_label].quantile(0.9)

	mask = (df_pf[arrival_delay_label]!=360.)
	df_pouet = df_pf.loc[mask, [arrival_delay_label, 'n_pax']]#, 'departure_delay']]
	mask_p2p = pd.isnull(df_pf.loc[mask, 'leg2_sch'])
	# dic['pax_departure_delay_non_overnight_avg'] = (df_pouet['departure_delay']*df_pouet['n_pax']).sum()/df_pouet['n_pax'].sum()
	# dic['pax_departure_delay_non_overnight_std'] = sqrt((df_pouet['departure_delay']**2*df_pouet['n_pax']).sum()/df_pouet['n_pax'].sum() - dic['pax_departure_delay_non_overnight_avg']**2)
	# dic['pax_departure_delay_non_overnight_90'] = df_pouet['departure_delay'].quantile(0.9)

	# dic['pax_p2p_departure_delay_non_overnight_avg'] = (df_pouet.loc[mask_p2p]['departure_delay']*df_pouet.loc[mask_p2p]['n_pax']).sum()/df_pouet.loc[mask_p2p]['n_pax'].sum()
	# dic['pax_p2p_departure_delay_non_overnight_std'] = sqrt((df_pouet.loc[mask_p2p]['departure_delay']**2*df_pouet.loc[mask_p2p]['n_pax']).sum()/df_pouet.loc[mask_p2p]['n_pax'].sum() - dic['pax_p2p_departure_delay_non_overnight_avg']**2)
	# dic['pax_p2p_departure_delay_non_overnight_90'] = df_pouet.loc[mask_p2p]['departure_delay'].quantile(0.9)

	# dic['pax_con_departure_delay_non_overnight_avg'] = (df_pouet.loc[~mask_p2p]['departure_delay']*df_pouet.loc[~mask_p2p]['n_pax']).sum()/df_pouet.loc[~mask_p2p]['n_pax'].sum()
	# dic['pax_con_departure_delay_non_overnight_std'] = sqrt((df_pouet.loc[~mask_p2p]['departure_delay']**2*df_pouet.loc[~mask_p2p]['n_pax']).sum()/df_pouet.loc[~mask_p2p]['n_pax'].sum() - dic['pax_con_departure_delay_non_overnight_avg']**2)
	# dic['pax_con_departure_delay_non_overnight_90'] = df_pouet.loc[~mask_p2p]['departure_delay'].quantile(0.9)
	
	dic['pax_arrival_delay_non_overnight_avg'] = (df_pouet[arrival_delay_label]*df_pouet['n_pax']).sum()/df_pouet['n_pax'].sum()
	dic['pax_arrival_delay_non_overnight_std'] = sqrt((df_pouet[arrival_delay_label]**2*df_pouet['n_pax']).sum()/df_pouet['n_pax'].sum() - dic['pax_arrival_delay_non_overnight_avg']**2)
	dic['pax_arrival_delay_non_overnight_90'] = df_pouet[arrival_delay_label].quantile(0.9)

	dic['pax_p2p_arrival_delay_non_overnight_avg'] = (df_pouet.loc[mask_p2p][arrival_delay_label]*df_pouet.loc[mask_p2p]['n_pax']).sum()/df_pouet.loc[mask_p2p]['n_pax'].sum()
	dic['pax_p2p_arrival_delay_non_overnight_std'] = sqrt((df_pouet.loc[mask_p2p][arrival_delay_label]**2*df_pouet.loc[mask_p2p]['n_pax']).sum()/df_pouet.loc[mask_p2p]['n_pax'].sum() - dic['pax_p2p_arrival_delay_non_overnight_avg']**2)
	dic['pax_p2p_arrival_delay_non_overnight_90'] = df_pouet.loc[mask_p2p][arrival_delay_label].quantile(0.9)

	dic['pax_con_arrival_delay_non_overnight_avg'] = (df_pouet.loc[~mask_p2p][arrival_delay_label]*df_pouet.loc[~mask_p2p]['n_pax']).sum()/df_pouet.loc[~mask_p2p]['n_pax'].sum()
	dic['pax_con_arrival_delay_non_overnight_std'] = sqrt((df_pouet.loc[~mask_p2p][arrival_delay_label]**2*df_pouet.loc[~mask_p2p]['n_pax']).sum()/df_pouet.loc[~mask_p2p]['n_pax'].sum() - dic['pax_con_arrival_delay_non_overnight_avg']**2)
	dic['pax_con_arrival_delay_non_overnight_90'] = df_pouet.loc[~mask_p2p][arrival_delay_label].quantile(0.9)
	
	mask = (df_pf[arrival_delay_label]>0.)
	df_pouet = df_pf.loc[mask, [arrival_delay_label, 'n_pax']]#, 'departure_delay']]
	mask_p2p = pd.isnull(df_pf.loc[mask, 'leg2_sch'])
	# dic['pax_departure_delay_positive_avg'] = (df_pouet['departure_delay']*df_pouet['n_pax']).sum()/df_pouet['n_pax'].sum()
	# dic['pax_departure_delay_positive_std'] = sqrt((df_pouet['departure_delay']**2*df_pouet['n_pax']).sum()/df_pouet['n_pax'].sum() - dic['pax_departure_delay_positive_avg']**2)
	# dic['pax_departure_delay_positive_90'] = df_pouet['departure_delay'].quantile(0.9)

	# dic['pax_p2p_departure_delay_positive_avg'] = (df_pouet.loc[mask_p2p]['departure_delay']*df_pouet.loc[mask_p2p]['n_pax']).sum()/df_pouet.loc[mask_p2p]['n_pax'].sum()
	# dic['pax_p2p_departure_delay_positive_std'] = sqrt((df_pouet.loc[mask_p2p]['departure_delay']**2*df_pouet.loc[mask_p2p]['n_pax']).sum()/df_pouet.loc[mask_p2p]['n_pax'].sum() - dic['pax_p2p_departure_delay_positive_avg']**2)
	# dic['pax_p2p_departure_delay_positive_90'] = df_pouet.loc[mask_p2p]['departure_delay'].quantile(0.9)

	# dic['pax_con_departure_delay_positive_avg'] = (df_pouet.loc[~mask_p2p]['departure_delay']*df_pouet.loc[~mask_p2p]['n_pax']).sum()/df_pouet.loc[~mask_p2p]['n_pax'].sum()
	# dic['pax_con_departure_delay_positive_std'] = sqrt((df_pouet.loc[~mask_p2p]['departure_delay']**2*df_pouet.loc[~mask_p2p]['n_pax']).sum()/df_pouet.loc[~mask_p2p]['n_pax'].sum() - dic['pax_con_departure_delay_positive_avg']**2)
	# dic['pax_con_departure_delay_positive_90'] = df_pouet.loc[~mask_p2p]['departure_delay'].quantile(0.9)

	dic['pax_arrival_delay_positive_avg'] = (df_pouet[arrival_delay_label]*df_pouet['n_pax']).sum()/df_pouet['n_pax'].sum()
	dic['pax_arrival_delay_positive_std'] = sqrt((df_pouet[arrival_delay_label]**2*df_pouet['n_pax']).sum()/df_pouet['n_pax'].sum() - dic['pax_arrival_delay_positive_avg']**2)
	dic['pax_arrival_delay_positive_90'] = df_pouet[arrival_delay_label].quantile(0.9)

	dic['pax_p2p_arrival_delay_positive_avg'] = (df_pouet.loc[mask_p2p][arrival_delay_label]*df_pouet.loc[mask_p2p]['n_pax']).sum()/df_pouet.loc[mask_p2p]['n_pax'].sum()
	dic['pax_p2p_arrival_delay_positive_std'] = sqrt((df_pouet.loc[mask_p2p][arrival_delay_label]**2*df_pouet.loc[mask_p2p]['n_pax']).sum()/df_pouet.loc[mask_p2p]['n_pax'].sum() - dic['pax_p2p_arrival_delay_positive_avg']**2)
	dic['pax_p2p_arrival_delay_positive_90'] = df_pouet.loc[mask_p2p][arrival_delay_label].quantile(0.9)

	dic['pax_con_arrival_delay_positive_avg'] = (df_pouet.loc[~mask_p2p][arrival_delay_label]*df_pouet.loc[~mask_p2p]['n_pax']).sum()/df_pouet.loc[~mask_p2p]['n_pax'].sum()
	dic['pax_con_arrival_delay_positive_std'] = sqrt((df_pouet.loc[~mask_p2p][arrival_delay_label]**2*df_pouet.loc[~mask_p2p]['n_pax']).sum()/df_pouet.loc[~mask_p2p]['n_pax'].sum() - dic['pax_con_arrival_delay_positive_avg']**2)
	dic['pax_con_arrival_delay_positive_90'] = df_pouet.loc[~mask_p2p][arrival_delay_label].quantile(0.9)

	mask = (df_pf[arrival_delay_label]!=360.) & (df_pf[arrival_delay_label]>0.)
	df_pouet = df_pf.loc[mask, [arrival_delay_label, 'n_pax']]#, 'departure_delay']]
	mask_p2p = pd.isnull(df_pf.loc[mask, 'leg2_sch'])
	# dic['pax_departure_delay_no_p_avg'] = (df_pouet['departure_delay']*df_pouet['n_pax']).sum()/df_pouet['n_pax'].sum()
	# dic['pax_departure_delay_no_p_std'] = sqrt((df_pouet['departure_delay']**2*df_pouet['n_pax']).sum()/df_pouet['n_pax'].sum() - dic['pax_departure_delay_no_p_avg']**2)
	# dic['pax_departure_delay_no_p_90'] = df_pouet['departure_delay'].quantile(0.9)

	# dic['pax_p2p_departure_delay_no_p_avg'] = (df_pouet.loc[mask_p2p]['departure_delay']*df_pouet.loc[mask_p2p]['n_pax']).sum()/df_pouet.loc[mask_p2p]['n_pax'].sum()
	# dic['pax_p2p_departure_delay_no_p_std'] = sqrt((df_pouet.loc[mask_p2p]['departure_delay']**2*df_pouet.loc[mask_p2p]['n_pax']).sum()/df_pouet.loc[mask_p2p]['n_pax'].sum() - dic['pax_p2p_departure_delay_no_p_avg']**2)
	# dic['pax_p2p_departure_delay_no_p_90'] = df_pouet.loc[mask_p2p]['departure_delay'].quantile(0.9)

	# dic['pax_con_departure_delay_no_p_avg'] = (df_pouet.loc[~mask_p2p]['departure_delay']*df_pouet.loc[~mask_p2p]['n_pax']).sum()/df_pouet.loc[~mask_p2p]['n_pax'].sum()
	# dic['pax_con_departure_delay_no_p_std'] = sqrt((df_pouet.loc[~mask_p2p]['departure_delay']**2*df_pouet.loc[~mask_p2p]['n_pax']).sum()/df_pouet.loc[~mask_p2p]['n_pax'].sum() - dic['pax_con_departure_delay_no_p_avg']**2)
	# dic['pax_con_departure_delay_no_p_90'] = df_pouet.loc[~mask_p2p]['departure_delay'].quantile(0.9)

	dic['pax_arrival_delay_no_p_avg'] = (df_pouet[arrival_delay_label]*df_pouet['n_pax']).sum()/df_pouet['n_pax'].sum()
	dic['pax_arrival_delay_no_p_std'] = sqrt((df_pouet[arrival_delay_label]**2*df_pouet['n_pax']).sum()/df_pouet['n_pax'].sum() - dic['pax_arrival_delay_no_p_avg']**2)
	dic['pax_arrival_delay_no_p_90'] = df_pouet[arrival_delay_label].quantile(0.9)

	dic['pax_p2p_arrival_delay_no_p_avg'] = (df_pouet.loc[mask_p2p][arrival_delay_label]*df_pouet.loc[mask_p2p]['n_pax']).sum()/df_pouet.loc[mask_p2p]['n_pax'].sum()
	dic['pax_p2p_arrival_delay_no_p_std'] = sqrt((df_pouet.loc[mask_p2p][arrival_delay_label]**2*df_pouet.loc[mask_p2p]['n_pax']).sum()/df_pouet.loc[mask_p2p]['n_pax'].sum() - dic['pax_p2p_arrival_delay_no_p_avg']**2)
	dic['pax_p2p_arrival_delay_no_p_90'] = df_pouet.loc[mask_p2p][arrival_delay_label].quantile(0.9)

	dic['pax_con_arrival_delay_no_p_avg'] = (df_pouet.loc[~mask_p2p][arrival_delay_label]*df_pouet.loc[~mask_p2p]['n_pax']).sum()/df_pouet.loc[~mask_p2p]['n_pax'].sum()
	dic['pax_con_arrival_delay_no_p_std'] = sqrt((df_pouet.loc[~mask_p2p][arrival_delay_label]**2*df_pouet.loc[~mask_p2p]['n_pax']).sum()/df_pouet.loc[~mask_p2p]['n_pax'].sum() - dic['pax_con_arrival_delay_no_p_avg']**2)
	dic['pax_con_arrival_delay_no_p_90'] = df_pouet.loc[~mask_p2p][arrival_delay_label].quantile(0.9)

	if do_single_values:
		mask = (df_pf[arrival_delay_label]>15.)
		df_pouet = df_pf.loc[mask, [arrival_delay_label, 'n_pax']]#, 'departure_delay']]
		mask_p2p = pd.isnull(df_pf.loc[mask, 'leg2_sch'])
		dic['pax_arrival_delay_sup_15_nb'] = df_pouet['n_pax'].sum()
		dic['pax_arrival_delay_sup_15_nb_gp'] = len(df_pouet)
		dic['pax_arrival_delay_sup_15_avg'] = (df_pouet[arrival_delay_label]*df_pouet['n_pax']).sum()/df_pouet['n_pax'].sum()
		dic['pax_arrival_delay_sup_15_std'] = sqrt((df_pouet[arrival_delay_label]**2*df_pouet['n_pax']).sum()/df_pouet['n_pax'].sum() - dic['pax_arrival_delay_sup_15_avg']**2)
		dic['pax_arrival_delay_sup_15_sem'] = dic['pax_arrival_delay_sup_15_avg']/sqrt(df_pouet['n_pax'].sum())
		dic['pax_con_arrival_delay_sup_15_nb'] = df_pouet.loc[~mask_p2p, 'n_pax'].sum()
		dic['pax_con_arrival_delay_sup_15_nb_gp'] = len(df_pouet.loc[~mask_p2p])
		dic['pax_con_arrival_delay_sup_15_avg'] = (df_pouet.loc[~mask_p2p, arrival_delay_label]*df_pouet.loc[~mask_p2p, 'n_pax']).sum()/df_pouet.loc[~mask_p2p, 'n_pax'].sum()
		dic['pax_con_arrival_delay_sup_15_std'] = sqrt((df_pouet.loc[~mask_p2p, arrival_delay_label]**2*df_pouet.loc[~mask_p2p, 'n_pax']).sum()/df_pouet.loc[~mask_p2p, 'n_pax'].sum() - dic['pax_con_arrival_delay_sup_15_avg']**2)
		dic['pax_con_arrival_delay_sup_15_sem'] = dic['pax_con_arrival_delay_sup_15_avg']/sqrt(df_pouet.loc[~mask_p2p, 'n_pax'].sum())
		dic['pax_p2p_arrival_delay_sup_15_nb'] = df_pouet.loc[mask_p2p, 'n_pax'].sum()
		dic['pax_p2p_arrival_delay_sup_15_nb_gp'] = len(df_pouet.loc[mask_p2p])
		dic['pax_p2p_arrival_delay_sup_15_avg'] = (df_pouet.loc[mask_p2p, arrival_delay_label]*df_pouet.loc[mask_p2p, 'n_pax']).sum()/df_pouet.loc[mask_p2p, 'n_pax'].sum()
		dic['pax_p2p_arrival_delay_sup_15_std'] = sqrt((df_pouet.loc[mask_p2p, arrival_delay_label]**2*df_pouet.loc[mask_p2p, 'n_pax']).sum()/df_pouet.loc[mask_p2p, 'n_pax'].sum() - dic['pax_p2p_arrival_delay_sup_15_avg']**2)
		dic['pax_p2p_arrival_delay_sup_15_sem'] = dic['pax_p2p_arrival_delay_sup_15_avg']/sqrt(df_pouet.loc[mask_p2p, 'n_pax'].sum())
		
		mask = (df_pf[arrival_delay_label]>60.)
		df_pouet = df_pf.loc[mask, [arrival_delay_label, 'n_pax']]##, 'departure_delay']]
		mask_p2p = pd.isnull(df_pf.loc[mask, 'leg2_sch'])
		dic['pax_arrival_delay_sup_60_nb'] = df_pouet['n_pax'].sum()
		dic['pax_arrival_delay_sup_60_nb_gp'] = len(df_pouet)
		dic['pax_arrival_delay_sup_60_avg'] = (df_pouet[arrival_delay_label]*df_pouet['n_pax']).sum()/df_pouet['n_pax'].sum()
		dic['pax_arrival_delay_sup_60_std'] = sqrt((df_pouet[arrival_delay_label]**2*df_pouet['n_pax']).sum()/df_pouet['n_pax'].sum() - dic['pax_arrival_delay_sup_60_avg']**2)
		dic['pax_arrival_delay_sup_60_sem'] = dic['pax_arrival_delay_sup_60_avg']/sqrt(df_pouet['n_pax'].sum())
		dic['pax_con_arrival_delay_sup_60_nb'] = df_pouet.loc[~mask_p2p, 'n_pax'].sum()
		dic['pax_con_arrival_delay_sup_60_nb_gp'] = len(df_pouet.loc[~mask_p2p])
		dic['pax_con_arrival_delay_sup_60_avg'] = (df_pouet.loc[~mask_p2p, arrival_delay_label]*df_pouet.loc[~mask_p2p, 'n_pax']).sum()/df_pouet.loc[~mask_p2p, 'n_pax'].sum()
		dic['pax_con_arrival_delay_sup_60_std'] = sqrt((df_pouet.loc[~mask_p2p, arrival_delay_label]**2*df_pouet.loc[~mask_p2p, 'n_pax']).sum()/df_pouet.loc[~mask_p2p, 'n_pax'].sum() - dic['pax_con_arrival_delay_sup_60_avg']**2)
		dic['pax_con_arrival_delay_sup_60_sem'] = dic['pax_con_arrival_delay_sup_60_avg']/sqrt(df_pouet.loc[~mask_p2p, 'n_pax'].sum())
		dic['pax_p2p_arrival_delay_sup_60_nb'] = df_pouet.loc[mask_p2p, 'n_pax'].sum()
		dic['pax_p2p_arrival_delay_sup_60_nb_gp'] = len(df_pouet.loc[mask_p2p])
		dic['pax_p2p_arrival_delay_sup_60_avg'] = (df_pouet.loc[mask_p2p, arrival_delay_label]*df_pouet.loc[mask_p2p, 'n_pax']).sum()/df_pouet.loc[mask_p2p, 'n_pax'].sum()
		dic['pax_p2p_arrival_delay_sup_60_std'] = sqrt((df_pouet.loc[mask_p2p, arrival_delay_label]**2*df_pouet.loc[mask_p2p, 'n_pax']).sum()/df_pouet.loc[mask_p2p, 'n_pax'].sum() - dic['pax_p2p_arrival_delay_sup_60_avg']**2)
		dic['pax_p2p_arrival_delay_sup_60_sem'] = dic['pax_p2p_arrival_delay_sup_60_avg']/sqrt(df_pouet.loc[mask_p2p, 'n_pax'].sum())
		
		mask = (df_pf[arrival_delay_label]>180.)
		df_pouet = df_pf.loc[mask, [arrival_delay_label, 'n_pax']]#, 'departure_delay']]
		mask_p2p = pd.isnull(df_pf.loc[mask, 'leg2_sch'])
		dic['pax_arrival_delay_sup_180_nb'] = df_pouet['n_pax'].sum()
		dic['pax_arrival_delay_sup_180_nb_gp'] = len(df_pouet)
		dic['pax_arrival_delay_sup_180_avg'] = (df_pouet[arrival_delay_label]*df_pouet['n_pax']).sum()/df_pouet['n_pax'].sum()
		dic['pax_arrival_delay_sup_180_std'] = sqrt((df_pouet[arrival_delay_label]**2*df_pouet['n_pax']).sum()/df_pouet['n_pax'].sum() - dic['pax_arrival_delay_sup_180_avg']**2)
		dic['pax_arrival_delay_sup_180_sem'] = dic['pax_arrival_delay_sup_180_avg']/sqrt(df_pouet['n_pax'].sum())
		dic['pax_con_arrival_delay_sup_180_nb'] = df_pouet.loc[~mask_p2p, 'n_pax'].sum()
		dic['pax_con_arrival_delay_sup_180_nb_gp'] = len(df_pouet.loc[~mask_p2p])
		dic['pax_con_arrival_delay_sup_180_avg'] = (df_pouet.loc[~mask_p2p, arrival_delay_label]*df_pouet.loc[~mask_p2p, 'n_pax']).sum()/df_pouet.loc[~mask_p2p, 'n_pax'].sum()
		dic['pax_con_arrival_delay_sup_180_std'] = sqrt((df_pouet.loc[~mask_p2p, arrival_delay_label]**2*df_pouet.loc[~mask_p2p, 'n_pax']).sum()/df_pouet.loc[~mask_p2p, 'n_pax'].sum() - dic['pax_con_arrival_delay_sup_180_avg']**2)
		dic['pax_con_arrival_delay_sup_180_sem'] = dic['pax_con_arrival_delay_sup_180_avg']/sqrt(df_pouet.loc[~mask_p2p, 'n_pax'].sum())
		dic['pax_p2p_arrival_delay_sup_180_nb'] = df_pouet.loc[mask_p2p, 'n_pax'].sum()
		dic['pax_p2p_arrival_delay_sup_180_nb_gp'] = len(df_pouet.loc[mask_p2p])
		dic['pax_p2p_arrival_delay_sup_180_avg'] = (df_pouet.loc[mask_p2p, arrival_delay_label]*df_pouet.loc[mask_p2p, 'n_pax']).sum()/df_pouet.loc[mask_p2p, 'n_pax'].sum()
		dic['pax_p2p_arrival_delay_sup_180_std'] = sqrt((df_pouet.loc[mask_p2p, arrival_delay_label]**2*df_pouet.loc[mask_p2p, 'n_pax']).sum()/df_pouet.loc[mask_p2p, 'n_pax'].sum() - dic['pax_p2p_arrival_delay_sup_180_avg']**2)
		dic['pax_p2p_arrival_delay_sup_180_sem'] = dic['pax_p2p_arrival_delay_sup_180_avg']/sqrt(df_pouet.loc[mask_p2p, 'n_pax'].sum())
		
		mask = df_pf['modified_itinerary']
		mask_p2p = pd.isnull(df_pf['leg2_sch'])
		dic['pax_modified_itineraries_ratio'] = df_pf.loc[mask, 'n_pax'].sum()/df_pf['n_pax'].sum()
		dic['pax_con_modified_itineraries_ratio'] = df_pf.loc[mask & ~mask_p2p, 'n_pax'].sum()/df_pf.loc[~mask_p2p, 'n_pax'].sum()
		dic['pax_p2p_modified_itineraries_ratio'] = df_pf.loc[mask & mask_p2p, 'n_pax'].sum()/df_pf.loc[mask_p2p, 'n_pax'].sum()
		
	return pd.Series(dic)

# def build_aligned_pax_df(df_pax, df_pax_sch):
# 	# Align simulation and historical passenger dfs 
# 	# to compare simulations and historical data

# 	# IMPOSSIBLE TO USE IN VERSION 1.25 and superior!!!!
# 	# DON'T NEED THIS BECAUSE THEY ARE ALREADY ALIGNED
# 	nids = df_pax_sch[df_pax_sch['pax']!=0]['nid'].reset_index()['nid']
# 	print (nids.iloc[:20])
# 	pouet = df_pax.loc[df_pax['id'].str.len()<8].reset_index()
# 	pouet['nid'] = nids
# 	pouet.set_index('index', inplace=True)
# 	df_pax.loc[df_pax['id'].str.len()<8, 'nid'] = pouet['nid']

# 	df_pax_a = pd.merge(df_pax_sch, df_pax, on='nid')

# 	print ('Check if alignement of indices is good (should be 0:', len(df_pax_a[df_pax_a['avg_fare']!=df_pax_a['fare']]), ')')

# 	df_pax_a = df_pax_a[['nid', 'id', 'original_id', 'n_pax', 'pax_type', 'fare', 'leg1_x', 'leg2_x', 'leg3_x', 'leg4_x', 'compensation',
# 					'duty_of_care', 'initial_sobt', 'leg1_y', 'leg2_y', 'leg3_y', 'leg4_y']]
# 	df_pax_a.rename(columns={'leg1_x':'leg1_sch', 'leg2_x':'leg2_sch', 'leg3_x':'leg3_sch', 'leg4_x':'leg4_sch',
# 						  'leg1_y':'leg1_act', 'leg2_y':'leg2_act', 'leg3_y':'leg3_act', 'leg4_y':'leg4_act', 'leg5':'leg5_act'},
# 				 inplace=True)

# 	return df_pax_a

def build_aligned_pax_flight_df(df_pax_a, df):
	# Merge pax and flight data
	# Pax data should already have been merged between 
	# historical and simulation
	cols = ['origin', 'destination', 'sobt', 'sibt', 'aobt', 'aibt', 'departure_delay', 'arrival_delay', 'main_reason_delay', 'scheduled_G2G_time',
		'actual_G2G_time', 'id', 'cancelled']

	df_pf = df_pax_a.copy()
	for leg in ['leg1', 'leg2', 'leg3', 'leg4']:
		tr = {k:k+'_'+leg for k in cols}
		tr['id'] = leg

		df_pf = pd.merge(df_pf, df[cols].rename(columns=tr),
				 on=leg, how='left')
		
	cols = ['origin', 'destination', 'cancelled', 'id', 'sobt', 'sibt', 'aobt', 'aibt', 'departure_delay', 'arrival_delay', 'main_reason_delay', 'scheduled_G2G_time',
			'actual_G2G_time']

	df_pf['leg4_sch'] = df_pf['leg4_sch'].astype(float)
	for leg in ['leg1_sch', 'leg2_sch', 'leg3_sch', 'leg4_sch']:
		tr = {k:k+'_'+leg for k in cols}
		tr['id'] = leg
		
		df_pf = pd.merge(df_pf, df[cols].rename(columns=tr),
				 on=leg, how='left')

	return df_pf

# def find_destination(x):
# 	# Get final destination
# 	l = [x['destination_leg1'], x['destination_leg2'], x['destination_leg3'], x['destination_leg4']]
	
# 	try:
# 		return next(item for item in l if item is not None)
# 	except:
# 		print (l)
# 		raise
		
# def find_origin(x):
# 	# Get original origin
# 	l = [x['origin_leg1'], x['origin_leg2'], x['origin_leg3'], x['origin_leg4']]
# 	try:
# 		return next(item for item in l if item is not None)
# 	except:
# 		print (l)
# 		raise

# def find_destination_sch(x):
# 	#print (x)
# 	l = [x['destination_leg1_sch'], x['destination_leg2_sch'], x['destination_leg3_sch'], x['destination_leg4_sch']]
	
# 	try:
# 		return next(item for item in l if item is not None)
# 	except:
# 		print (l)
# 		raise
		
# def find_origin_sch(x):
# 	#print (x)
# 	l = [x['origin_leg1_sch'], x['origin_leg2_sch'], x['origin_leg3_sch'], x['origin_leg4_sch']]
	
# 	try:
# 		return next(item for item in l if item is not None)
# 	except:
# 		print (l)
# 		raise

# def get_arrival_delay(x):
# 	# Get final passenger delay
# 	l_sch = [x['origin_leg1_sch'], x['origin_leg2_sch'], x['origin_leg3_sch'], x['origin_leg4_sch']]
# 	l = [x['origin_leg1_sch'], x['origin_leg2_sch'], x['origin_leg3_sch'], x['origin_leg4_sch']]
	
# 	can_sch = [x['cancelled_leg1_sch'], x['cancelled_leg2_sch'], x['cancelled_leg3_sch'], x['cancelled_leg4_sch']]
	
# 	#idx_sch = next(i for i, item in enumerate(l) if item is not None)
	
# 	if (array(l_sch)==array(l)).all() and True in can_sch:
# 		return 6. * 60.
# 	else:
# 		l = [x['sibt_leg1_sch'], x['sibt_leg2_sch'], x['sibt_leg3_sch'], x['sibt_leg4_sch']]

# 		last_sibt = next(item for item in l if item is not None)

# 		l = [x['aibt_leg1'], x['aibt_leg2'], x['aibt_leg3'], x['aibt_leg4']]

# 		last_aibt = next(item for item in l if item is not None)

# 		return (last_aibt - last_sibt).total_seconds()/60.

# def get_departure_delay(x):
# 	# Get passenger departure delay
	
# 	sobt = x['sobt_leg1_sch']
# 	aobt = x['aobt_leg1']

# 	return (aobt - sobt).total_seconds()/60.

# def get_missed_connection(x):
# 	# Compute the group of pax has missed at least one connection (sure???)
# 	l_sch = [x['origin_leg1_sch'], x['origin_leg2_sch'], x['origin_leg3_sch'], x['origin_leg4_sch']]
# 	l = [x['origin_leg1_sch'], x['origin_leg2_sch'], x['origin_leg3_sch'], x['origin_leg4_sch']]
	
# 	can_sch = [x['cancelled_leg1_sch'], x['cancelled_leg2_sch'], x['cancelled_leg3_sch'], x['cancelled_leg4_sch']]
	
# 	#idx_sch = next(i for i, item in enumerate(l) if item is not None)
	
# 	return not((array(l_sch)==array(l)).all() and not(True in can_sch))

def binarise(x):
	if x:
		return 1
	else:
		return 0

# New methods below
def merge_pax_flights(df_f, df_p):
	#cols = ['origin', 'destination', 'sobt', 'sibt', 'aobt', 'aibt', 'departure_delay', 'arrival_delay', 'main_reason_delay', 'scheduled_G2G_time',
	#		'actual_G2G_time', 'id', 'cancelled']

	df_pf = df_p.copy()
	df_pf['leg4'] = df_pf['leg4'].astype(float)
	for leg in ['leg1', 'leg2', 'leg3', 'leg4']:
		tr = {k:k+'_'+leg for k in df_f.columns}
		tr['nid'] = leg#+'_act'

		df_pf = pd.merge(df_pf, df_f.rename(columns=tr),
				 on=leg, how='left')

	return df_pf

def compute_derived_metrics_pax_generic(df_pf):
	# Compute some derived metrics for PAX.
	# Only on merged df!
	df_pf['origin'] = df_pf.iloc[:].T.apply(find_origin_generic)
	df_pf['destination'] = df_pf.iloc[:].T.apply(find_destination_generic)
	#df_pf['departure_delay'] = df_pf.iloc[:].T.apply(get_departure_delay)
	#df_pf['arrival_delay'] = df_pf.iloc[:].T.apply(get_arrival_delay)
	#df_pf['missed_connection'] = df_pf.T.apply(get_missed_connection)

def produce_historical_flight_pax_df(profile='remote_direct', engine=None):
	with mysql_connection(profile=profile, engine=engine) as connection:
		df_f = get_historical_flights(profile=profile, engine=connection['engine'])

		df_p = get_pax_schedules(profile=profile, engine=connection['engine'])

	compute_derived_metrics_historical(df_f)

	df_pf = merge_pax_flights(df_f, df_p)

	compute_derived_metrics_pax_generic(df_pf)

	return df_pf

def produce_sim_flight_pax_df(profile='remote_direct', engine=None):
	# NON TESTED
	with mysql_connection(profile=profile, engine=engine) as connection:
		df_f = get_simulation_flights(profile=profile, engine=connection['engine'])

		df_p = get_simulation_paxs(profile=profile, engine=connection['engine'])

	df_pf = merge_pax_flights(df_f, df_p)

	compute_derived_metrics_pax_generic(df_pf)

	return df_pf

def compute_derived_metrics_hist_sim(df_pf):
	# Compute some derived metrics for PAX.
	# Only on merged df!
	#df_pf['origin'] = df_pf.iloc[:].T.apply(find_origin)
	#df_pf['destination'] = df_pf.iloc[:].T.apply(find_destination)
	df_pf['departure_delay'] = df_pf.iloc[:].T.apply(get_departure_delay)
	df_pf['arrival_delay'] = df_pf.iloc[:].T.apply(get_arrival_delay)
	df_pf['missed_connection'] = df_pf.T.apply(get_missed_connection)

def merge_hist_sim():
	# TBD
	pass

def produce_hist_sim_df():
	# TBD
	df_h = produce_historical_flight_pax_df()
	df_s = produce_sim_flight_pax_df()
	compute_derived_metrics_hist_sim(df_h, df_s)
		
def find_destination_generic(x):
	# Get original origin
	l = [x['airport2_sch'], x['airport3_sch'], x['airport4_sch']]
	try:
		return next(l[i] for i in range(len(l)-1,-1,-1) if l[i] is not None)
	except:
		print (l)
		raise

def find_origin_generic(x):
	# Get original origin
	return x['airport1_sch']
