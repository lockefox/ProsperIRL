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

if __name__ == "__main__":
	main()	