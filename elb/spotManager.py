#!/usr/bin/python3
'''
***
Spot Instance Manager

./spotManager.py <configfile> ListAll - Lists all instances currently tagged with my configured spot running name
./spotManager.py <configfile> ListUtilized - Lists all instances running with a lock file present on S3 and utilization match
./spotManager.py <configfile> ListNotUtilized [Delete] - Lists (and Deletes) all instances running that aren't processing anything
./spotManager.py <configfile> ListLocksWithoutMatchingInstance [Delete] - Lists (and deletes) all locks that don't have a corresponding instance running the data
./spotManager.py <configfile> DeleteLock [Locked Directory in MMDDYYYY]

This application manages spot instances and management of log processing through log files and worker http requests
***

Author: Philip Matuskiewicz - philip.matuskiewicz@nyct.com       

Changes:
	6/6/16 - Initial script

Acknowledgements:
	http://stackoverflow.com/questions/7936572/python-call-a-function-from-string-name

'''

import sys
import boto
from boto.s3.key import Key
import configparser

#global variables
DELETE_CMD = False

CONFIG = configparser.ConfigParser()
if len(sys.argv) == 3 or len(sys.argv) == 4:
	inputini = sys.argv[1];
	if inputini.endswith(".ini"):
		CONFIG.read(inputini)
	else:
		print ("Config file not specified, look at my header for how to use this script")
		sys.exit(0)
	
	if len(sys.argv) == 4:
		c = str(sys.argv[3]).strip().lower()
		if "delete" in c:
			DELETE_CMD = True
		else:
			if (not c.isdigit()) and (len(c) is not 8):
				print("Number doesn't have 8 characters, is this right?")
				sys.exit(0)
			else:
				DELETE_CMD = c
	c = sys.argv[2]
	method_name = "do_" + str(c.strip().lower())
else:
	print ("usage listed in this file")
	sys.exit(0)

#Load configuration from ini file
DST_PATH = CONFIG.get('main', 'DST_PATH')
DST_AWS_ACCESS_KEY = CONFIG.get('main', 'DST_AWS_ACCESS_KEY')
DST_AWS_SECRET_KEY = CONFIG.get('main', 'DST_AWS_SECRET_KEY')
PROCESSING_LOCK_FILE = CONFIG.get('main', 'PROCESSING_LOCK_FILE')

#main implementation 
def releaseLock(filePath):
	dst_s3conn = boto.connect_s3(DST_AWS_ACCESS_KEY, DST_AWS_SECRET_KEY)
	dst_bucket = dst_s3conn.get_bucket(DST_PATH[:DST_PATH.index('/')])
	lock_file_path = "%s%s%s" % (DST_PATH[DST_PATH.index('/'):],filePath,PROCESSING_LOCK_FILE)
	lock_file_key = Key(dst_bucket, lock_file_path)
	if not lock_file_key.exists():
		print("There was no lock... hoping nothing went wrong!  You may want to verify this.")
	else:
		instanceid = bytes(lock_file_key.get_contents_as_string()).decode(encoding='UTF-8')
		dst_bucket.delete_key(lock_file_key)
		print("The lock file has been removed, the instance it was running on is \"%s\"" % instanceid)
	dst_s3conn.close()

def getLocksWithoutMatchingInstance():
	out = list()
	dst_s3conn = boto.connect_s3(DST_AWS_ACCESS_KEY, DST_AWS_SECRET_KEY)
	bucket = dst_s3conn.get_bucket(DST_PATH[:DST_PATH.index('/')])
	for year in bucket.list(prefix=DST_PATH[DST_PATH.index('/')+1:], delimiter='/'):
		yearint = year.name[-5:-1]
		for month in bucket.list(prefix=year.name, delimiter='/'):
			monthint = month.name[-3:-1]
			for day in bucket.list(prefix=month.name, delimiter='/'):
				dayint = day.name[-3:-1]
				dstdir = "%s%s%s" % (monthint, dayint, yearint)
				for filePath in bucket.list(prefix=day.name, delimiter='/'):
					if PROCESSING_LOCK_FILE in filePath.name:
						instance = bytes(filePath.get_contents_as_string()).decode(encoding='UTF-8')
						out.append("%s~%s" % (filePath.name, instance))
	dst_s3conn.close()
	return out

#command implementation 

def do_listall():
	return "True"

def do_listutilized():
	return "True"

def do_listnotutilized():
	return "True"

def do_listlockswithoutmatchinginstance():
	data = getLocksWithoutMatchingInstance()
	print (data)
	if DELETE_CMD:
		print("Not implemented, delete requested")	

def do_deletelock():
	if DELETE_CMD.isdigit():
		#MMDDYYYY
		m = DELETE_CMD[:2]
		d = DELETE_CMD[2:4]
		y = DELETE_CMD[4:]
		releaselockdir = "%s/%s/%s/" % (y,m,d)
		releaseLock(releaselockdir)

# run teh application by invoking the correct method	
possibles = globals().copy()
possibles.update(locals())
method = possibles.get(method_name)
if not method:
	raise NotImplementedError("Command %s is not implemented" % method_name)
method()
