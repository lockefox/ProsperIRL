#!/Python27/python.exe

import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket
from os import path
import urllib2
import ConfigParser
import pypyodbc
from datetime import datetime

conf = ConfigParser.ConfigParser()
conf.read(['init.ini','init_local.ini'])

####DB STUFF####
db_host   = conf.get('GLOBALS','db_host')
db_user   = conf.get('GLOBALS','db_user')
db_pw     = conf.get('GLOBALS','db_pw')
db_port   = int(conf.get('GLOBALS','db_port'))
db_schema = conf.get('GLOBALS','db_schema')
db_driver = conf.get('GLOBALS','db_driver')

conn = None
cur  = None

def init_tables():
	global conn,cur
	table_dict = dict(conf.items('TABLES'))
	for table,name in table_dict.iteritems():
		cur.execute('''SHOW TABLES LIKE \'%s\'''' % name)
		db_exists = len(cur.fetchall())
		if db_exists:
			print '%s.%s:\tGOOD' % (db_schema,name)
		else:
			table_init = open(path.relpath('sql/%s.sql' % name)).read()
			table_init_commands = table_init.split(';') #One command per execute
			try:
				for command in table_init_commands:
					cur.execute(command)
					conn.commit()
			except Exception as e:
					print '%s.%s:\tERROR' % (db_schema,name)
					print e[1]
					sys.exit(2)
			print '%s.%s:\tCREATED' % (db_schema,name)

def load_stocklist(hard_overwrite=True, debug=False):
	cur.execute('''SHOW COLUMNS FROM `%s`''' % conf.get('TABLES','company_info_db'))
	table_info = cur.fetchall()
	#TODO: if len(table_info) == 0: exception
	header = []
	for column in table_info:
		header.append(column[0])
	
	header_str = ','.join(header)
	base_sql_write = '''INSERT INTO %s (%s) VALUES ''' % (conf.get('TABLES','company_info_db'), header_str)

	print 'Writing:\tNASDAQ'
	_write_companies(base_sql_write, conf.get('REFERENCES','nasdaq_list'), 'NASDAQ', header, hard_overwrite, debug)
	
	print 'Writing:\tNYSE'
	_write_companies(base_sql_write, conf.get('REFERENCES','nyse_list'), 'NYSE', header, hard_overwrite, debug)
	
	print 'Writing:\tAMEX'	
	_write_companies(base_sql_write, conf.get('REFERENCES','amex_list'), 'AMEX', header, hard_overwrite, debug)
	
def _write_companies(base_insert_statement, csv_file, exchange_str, header, hard_overwrite=True, debug=False):
	LIST_file = open(path.relpath('references/%s' % csv_file)).read()
	LIST = _CSV_parse(LIST_file)
	
	LIST_commit = base_insert_statement
	for stock_info in LIST:
		if stock_info[0] == 'Symbol':	#skip headder
			continue
		if stock_info[0] == '':
			break

		data_str = '\'%s\',\'%s\',\'%s\',%s,\'%s\',\'%s\',\'%s\'' %(\
			_NA_to_NULL(stock_info[0]),\
			exchange_str,\
			_NA_to_NULL(stock_info[1].replace('\'','\\\'')),\
			_NA_to_NULL(stock_info[5]),\
			_NA_to_NULL(stock_info[6]),\
			_NA_to_NULL(stock_info[7]),\
			_NA_to_NULL(stock_info[8]))
		LIST_commit = '%s (%s),' % (LIST_commit, data_str)
	
	LIST_commit = LIST_commit[:-1]	#remove trailing ','
	LIST_commit = LIST_commit.replace('\'NULL\'','NULL') #get proper NULLs in string cols
	if hard_overwrite:
		duplicate_str = '''ON DUPLICATE KEY UPDATE'''
		for head in header:
			duplicate_str = '%s %s=%s,' % (duplicate_str, head, head)
		LIST_commit = '%s %s' % (LIST_commit, duplicate_str)
		LIST_commit = LIST_commit[:-1]	#remove trailing ','
	
	if debug:
		print LIST_commit
	cur.execute(LIST_commit)
	cur.commit()
def _NA_to_NULL(str_parse):
	if str_parse.lower() == 'n/a':
		return 'NULL'
	else:
		return str_parse
		
def _CSV_parse(csv_raw):
	list_return = []
	list_parse = csv_raw.split('\n')
	
	for item_str in list_parse:
		#item_str = item_str.replace('"','') #strip out quotes
		list_line = item_str.split('","')
		tmp_list = []
		for item in list_line:
			tmp_list.append(item.replace('"',''))
		list_return.append(tmp_list)
	
	return list_return
	
def main():
	global conn, cur
	try:	#Test DB connection
		conn = pypyodbc.connect('DRIVER={%s};SERVER=%s;PORT=%s;UID=%s;PWD=%s;DATABASE=%s' \
			% (db_driver,db_host,db_port,db_user,db_pw,db_schema))
		cur = conn.cursor()
	except Exception as e:
		print 'DB connection:\tFAILED'
		print e[1]
		sys.exit(2)
	print 'DB connection:\tGOOD'
	
	init_tables()
	
	load_stocklist()

if __name__ == "__main__":
	main()	