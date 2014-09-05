#!/Python27/python.exe

import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket
import urllib2
import ConfigParser
import pypyodbc
from datetime import datetime

conf = ConfigParser.ConfigParser()
conf.read(['init_local.ini','init.ini'])

dataset_format= conf.get('QUANDL','format')
base_query    = conf.get('QUANDL','base_query')
quantl_token  = conf.get('QUANDL','token')
user_agent    = conf.get('GLOBALS','user_agent')
fetch_retry   = int(conf.get('GLOBALS','default_retries'))
default_sleep = int(conf.get('GLOBALS','default_sleep'))

class Quandl_sourcelist(Class):
	def __init__ (self,search_parameters): #search_parameters= 'query=*&source_code=_stuff_'
		self.per_page = conf.get('QUANDL','per_page')
		self.query = '%sdatasets.%s?%s&per_page=%s&auth_token=%s&' % \
			(base_query,dataset_format,search_parameters,self.per_page,quantl_token)
	def fetchResults(fetch_docs = True, fetch_sources=False):
		page_number = 1
		keep_querying = True
		query_results_JSON = {}
		if fetch_docs:
			query_results_JSON['docs']    = []
		if fetch_sources:
			query_results_JSON['sources'] = []
		
		while keep_querying:
			response_str = fetchUrl('%spage=%s' % self.query,page_number)
			JSON_data = json.loads(response_str)
			responses_reported = JSON_data['per_page']
			data_returned = len(JSON_data['docs'])
			if data_returned < responses_reported:
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
	
	request = urllib2.Request(url)
	request.add_header('Accept-Encoding','gzip')
	request.add_header('User-Agent','user_agent')
	headers = []
	for tries in range (0,fetch_retry):
		time.sleep(default_sleep*tries)
		try:
			opener = urllib2.build_opener()
			raw_response = opener.open(request)
			headers = raw_response.headers
			response = raw_response.read()
		except urllib2.HTTPError as e:
			print 'HTTPError:%s %s' % (e,url)
		except urllib2.URLError as e:
			print 'URLError:%s %s' % (e,url)
		except socket.error as e:
			print 'Socket Error:%s %s' % (e,url)
	
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
			except IOError as e:
				print "gzip unreadable: Retry %s" %url
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
	None 