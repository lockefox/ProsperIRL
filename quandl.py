#!/Python27/python.exe

import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket
from os import path
import urllib2
import ConfigParser
import pypyodbc
from datetime import datetime

conf = ConfigParser.ConfigParser()
conf.read(['init.ini','init_local.ini'])

dataset_format= conf.get('QUANDL','format')
base_query    = conf.get('QUANDL','base_query_v1')
base_query_v1 = conf.get('QUANDL','base_query_v2')
quandl_token  = conf.get('QUANDL','token')
user_agent    = conf.get('GLOBALS','user_agent')
fetch_retry   = int(conf.get('GLOBALS','default_retries'))
default_sleep = int(conf.get('GLOBALS','default_sleep'))
snooze_routine= conf.get('QUANDL','query_limit_period')
script_dir = os.path.dirname(__file__)
####DB STUFF####
db_host   = conf.get('GLOBALS','db_host')
db_user   = conf.get('GLOBALS','db_user')
db_pw     = conf.get('GLOBALS','db_pw')
db_port   = int(conf.get('GLOBALS','db_port'))
db_schema = conf.get('GLOBALS','db_schema')
db_driver = conf.get('GLOBALS','db_driver')
conn = pypyodbc.connect('DRIVER={%s};SERVER=%s;PORT=%s;UID=%s;PWD=%s;DATABASE=%s' \
	% (db_driver,db_host,db_port,db_user,db_pw,db_schema))
cur = conn.cursor()

####TABLES####
sources_db = conf.get('QUANDL','souces_db')


class Quandl_sourcelist(object):
	def __init__ (self,search_parameters): #search_parameters= 'query=*&source_code=_stuff_'
		self.per_page = conf.get('QUANDL','per_page')
		self.query = '%sdatasets.%s?%s&per_page=%s&' % \
			(base_query,dataset_format,search_parameters,self.per_page)
		if quandl_token != '':
			self.query = '%sauth_token=%s&' % (self.query,quandl_token)
		self.fetch_docs = True
		self.fetch_sources = False
		self.start_page = 1
	def __iter__(self):
		page_num = self.start_page
		keep_querying = True
		query_results_JSON = {}
		
		while keep_querying:
			JSON_data = fetchUrl('%spage=%s' % (self.query, page_num))
			responses_expected = JSON_data['per_page']
			data_returned = len(JSON_data['docs'])
			
			if data_returned < responses_expected:
				keep_querying = False
			
			if self.fetch_docs:
				query_results_JSON['docs'] = JSON_data['docs']

			if self.fetch_sources:
				query_results_JSON['sources']=JSON_data['sources']
			
			page_num += 1
			yield query_results_JSON

class Quandl_dataFeed(object):
	def __init__(self,query_path):
		self.query = '%s%s.%s?' % (base_query_v1,query_path,dataset_format)
		if quandl_token != '':
			self.query = '%sauth_token=%s&' % (self.query, quandl_token)
		self.args = ''
		self.query = '%s%s' % (self.query, self.args)
		self.sources = ''
		
	def fetchFeed(self):	#returns JSON from QUANDL
		result = fetchUrl(self.query)
		self.sources = self.load_sources(result)
		return result
	
	def load_sources(self, JSON_return):	#Returns values for updating quandl_sources.sql
		#returns: '''('feed_source','feed_code','feed_name','frequency','from_date','to_date','updated_at',id,'columns')'''
			
		return_str = '''('%s','%s','%s','%s','%s','%s','%s',%s,'%s')''' %(\
			JSON_return['source_code'],\
			JSON_return['code'],\
			JSON_return['name'].replace('\'','\\\''),\
			JSON_return['frequency'],\
			JSON_return['from_date'],\
			JSON_return['to_date'],\
			updated_at_converter(JSON_return['updated_at']),\
			JSON_return['id'],\
			','.join(JSON_return['column_names'].replace('\'','\\\''))
		
		return return_str	
		
def fetchUrl(url):	
	return_result = ""
	print 'fetching %s' % url
	request = urllib2.Request(url)
	request.add_header('Accept-Encoding','gzip')
	request.add_header('User-Agent','user_agent')
	headers = {}
	response = ""
	for tries in range (0,fetch_retry):
		time.sleep(default_sleep*tries)
		try:
			opener = urllib2.build_opener()
			raw_response = opener.open(request)
			headers = raw_response.headers
			response = raw_response.read()
		except urllib2.HTTPError as e:
			print 'HTTPError:%s %s' % (e,url)
			continue
		except urllib2.URLError as e:
			print 'URLError:%s %s' % (e,url)
			continue
		except socket.error as e:
			print 'Socket Error:%s %s' % (e,url)
			continue
		
		_snooze(headers)
		
		do_gzip = False
		try:
				if headers['Content-Encoding'] == 'gzip':
					do_gzip = True
		except KeyError as e:
			None
		
		if do_gzip:
			try:
				buf = StringIO.StringIO(response)
				zipper = gzip.GzipFile(fileobj=buf)
				return_result = json.load(zipper)
			except ValueError as e:
				print "Empty response: retry %s" % url
				continue
			except IOError as e:
				print "gzip unreadable: Retry %s" %url
				continue
			else:
				break
		else:
			return_result = json.loads(response)
			break
	else:
		print headers
		sys.exit(-2)
	
	return return_result
	
def _snooze(headers):
	try:
		RateLimit    = int(headers['X-RateLimit-Limit'])
		LimitRemains = int(headers['X-RateLimit-Remaining'])
	except KeyError as e:
		print "WARNING: could not find rate-limit headers %s" % e
		time.sleep(default_sleep)
		return
	snooze_period = 0.0
	if snooze_routine == 'HOURLY':
		allowance_used = 1.0 - (LimitRemains/RateLimit)	
		if allowance_used > .5:
			snooze_period = default_sleep * allowance_used
		elif allowance_used > .8:
			snooze_period = default_sleep * allowance_used * 2
		elif allowance_used > .9:
			snooze_period = default_sleep * allowance_used * 4
	if snooze_period > 0:
		time.sleep(default_sleep)

def _initSQL():
	global conn,cur

	cur.execute('''SHOW TABLES LIKE \'%s\'''' % souces_db)
	sources_db_exists = len(cur.fetchall())
	if sources_db_exists == 0:
		sources_init = open(path.relpath('sql/%s.sql' % (souces_db)),'r').read()
		sources_commands = sources_init.split(';') #split up... because SQL?!
		try:
			for command in sources_commands:
				cur.execute(command)
				conn.commit()
		except Exception, e:
			print e
			sys.exit(2)
		print '%s.%s table:\tCREATED' % (db_schema,souces_db)
	else:
		print '%s.%s table:\tGOOD' % (db_schema,souces_db)
	
	
def _writeSQL(table, headers_list, data_list, hard_overwrite=True, debug=False):
	#insert_statement = 'INSERT INTO %s (\'%s) VALUES' % (table, '\',\''.join(headers_list))
	insert_statement = 'INSERT INTO %s (%s) VALUES' % (table, ','.join(headers_list))
	if debug:
		print insert_statement
	for entry in data_list:
		value_string = ''
		for value in entry:
			if isinstance(value, (int,long,float)): #if number, add value
				value_string = '%s,%s' % ( value_string, value)
			else:		#if string value: add 'value'
				if value == None:
					value_string = '%s,NULL' % ( value_string)
				else:
					value = value.replace('\'', '\\\'') #sanitize apostrophies
					value_string = '%s,\'%s\'' % ( value_string, value)
		value_string = value_string[1:]
		if debug:
			print value_string
		insert_statement = '%s (%s),' % (insert_statement, value_string)
	
	
	insert_statement = insert_statement[:-1]	#pop off trailing ','
	if hard_overwrite:
		duplicate_str = "ON DUPLICATE KEY UPDATE "
		for header in headers_list:
			duplicate_str = "%s %s=%s," % (duplicate_str, header, header)
		
		insert_statement = "%s %s" % (insert_statement, duplicate_str)
		insert_statement = insert_statement[:-1]	#pop off trailing ','
	if debug:
		print insert_statement
	cur.execute(insert_statement)
	cur.commit()
	
def _query_feedSources(query = 'query=*&source_code=NASDAQOMX', destination_db = sources_db):
	test_obj = {}
	testQobj = Quandl_sourcelist(query)
	print testQobj.query
	
	sources_table_headers = []
	cur.execute('SHOW COLUMNS FROM `%s`' % destination_db)
	table_info = cur.fetchall()
	#TODO: If len(table_info) == 0 exception
	for column in table_info:
		sources_table_headers.append(column[0])
		
	for JSON_page in testQobj:
		data_list = []
		for entry in JSON_page['docs']:
			tmp_data_list = []
			tmp_data_list.append(entry['source_code'])
			tmp_data_list.append(entry['code'])
			tmp_data_list.append(entry['name'])
			tmp_data_list.append(entry['frequency'])
			tmp_data_list.append(entry['from_date'])
			tmp_data_list.append(entry['to_date'])
			#--#	
			tmp_data_list.append(updated_at_converter(entry['updated_at']))
			#--#
			tmp_data_list.append(entry['id'])
			tmp_data_list.append(','.join(entry['column_names']))
			data_list.append(tmp_data_list)
			
		_writeSQL(destination_db, sources_table_headers, data_list)

def updated_at_converter(update_at_str):
	update_time = datetime.strptime(update_at_str)
	return update_time.strftime('%Y-%m-%d %H:%M:%S')
	
def main():
	_initSQL()
	
	
if __name__ == "__main__":
	main()	