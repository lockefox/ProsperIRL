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
base_query    = conf.get('QUANDL','base_query')
quantl_token  = conf.get('QUANDL','token')
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
souces_db = conf.get('QUANDL','souces_db')


class Quandl_sourcelist(object):
	def __init__ (self,search_parameters,output_table=''): #search_parameters= 'query=*&source_code=_stuff_'
		self.per_page = conf.get('QUANDL','per_page')
		self.query = '%sdatasets.%s?%s&per_page=%s&' % \
			(base_query,dataset_format,search_parameters,self.per_page)
		if quantl_token != '':
			self.query = '%sauth_token=%s&' % (self.query,quantl_token)
		self.table = output_table
	def fetchResults(self, fetch_docs = True, write_SQL = True, fetch_sources = False):
		page_number = 1
		keep_querying = True
		query_results_JSON = {}
		if fetch_docs:
			query_results_JSON['docs']    = []
		if fetch_sources:
			query_results_JSON['sources'] = []
		
		while keep_querying:
			response_str = fetchUrl('%spage=%s' % (self.query,page_number))
			
			JSON_data = json.loads(response_str)
			responses_reported = JSON_data['per_page']
			data_returned = len(JSON_data['highlighting']) #originally pulled 'docs' but some id's are hidden from 'docs' and show on highlighting
			
			if data_returned < responses_reported:
				keep_querying = False
			
			if page_number > 1:	#DEBUG
				keep_querying = False
			if fetch_docs:                                      
				for entry in JSON_data['docs']:                 #
					query_results_JSON['docs'].append(entry)	#can cut for run time
				if write_SQL and (self.table != ''):
					cur.execute('SHOW COLUMNS FROM `%s`' % self.table)
					table_info = cur.fetchall()
					#TODO: if len(table_info)==0, exception
					headers_list = []
					for column in table_info:
						headers_list.append(column[0])
					
					data_list = []
					for entry in JSON_data['docs']:
						tmp_data_list = []
						tmp_data_list.append(entry['source_code'])
						tmp_data_list.append(entry['code'])
						tmp_data_list.append(entry['name'])
						tmp_data_list.append(entry['frequency'])
						tmp_data_list.append(entry['from_date'])
						tmp_data_list.append(entry['to_date'])
						tmp_data_list.append(entry['updated_at'])
						tmp_data_list.append(entry['id'])
						tmp_data_list.append(','.join(entry['column_names']))
						#TODO: exception if len(tmp_data_list) != len(headers_list)
						data_list.append(tmp_data_list)
					
					_writeSQL(self.table,headers_list,data_list)
			if fetch_sources:
				for entry in JSON_data['sources']:
					query_results_JSON['sources'].append(entry)
			page_number +=1
			
		return query_results_JSON
		
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
			return_result = response
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
	
	
def _writeSQL(table, headers_list, data_list):
	#insert_statement = 'INSERT INTO %s (\'%s) VALUES' % (table, '\',\''.join(headers_list))
	insert_statement = 'INSERT INTO %s (%s) VALUES' % (table, ','.join(headers_list))
	print insert_statement
	for entry in data_list:
		value_string = ''
		for value in entry:
			#TODO: convert YYYY-MM-DDTHH:MM:SS.SSSZ to TIMESTAMP
			if isinstance(value, (int,long,float)): #if number, add value
				value_string = '%s,%s' % ( value_string, value)
			else:		#if string value: add 'value'
				value_string = '%s,\'%s\'' % ( value_string, value)
		value_string = value_string[1:]
		print value_string
		insert_statement = '%s (%s),' % (insert_statement, value_string)
	print insert_statement
	cur.execute(insert_statement[:-1])
	cur.commit()
def main():
	_initSQL()
	
	test_obj = {}
	testQobj = Quandl_sourcelist('query=*&source_code=NASDAQOMX',souces_db)
	print testQobj.query
	test_obj = testQobj.fetchResults()
	print test_obj
		
if __name__ == "__main__":
	main()	