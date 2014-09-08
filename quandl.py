#!/Python27/python.exe

import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket
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

####DB STUFF####
db_host   = conf.get('GLOBALS','db_host')
db_user   = conf.get('GLOBALS','db_user')
db_pw     = conf.get('GLOBALS','db_pw')
db_port   = int(conf.get('GLOBALS','db_port'))
db_schema = conf.get('GLOBALS','db_schema')
db_driver = conf.get('GLOBALS','db_driver')
conn = pypyodbc.connect('DRIVER={%s};SERVER=%s;PORT=%s;UID=%s;PWD=%s;DATABASE=%s' \
	% (db_driver,db_host,db_port,db_user,db_pw,db_schema))

class Quandl_sourcelist(object):
	def __init__ (self,search_parameters): #search_parameters= 'query=*&source_code=_stuff_'
		self.per_page = conf.get('QUANDL','per_page')
		self.query = '%sdatasets.%s?%s&per_page=%s&' % \
			(base_query,dataset_format,search_parameters,self.per_page)
		if quantl_token != '':
			self.query = '%sauth_token=%s&' % (self.query,quantl_token)
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
			
			if page_number > 5:	#DEBUG
				keep_querying = False
			if fetch_docs:
				for entry in JSON_data['docs']:
					query_results_JSON['docs'].append(entry)
			
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
	global engine,session
	
	
	
def main():
	_initSQL()
	
	test_obj = {}
	testQobj = Quandl_sourcelist('query=*&source_code=NASDAQOMX')
	print testQobj.query
	test_obj = testQobj.fetchResults()
	print test_obj
		
if __name__ == "__main__":
	main()	