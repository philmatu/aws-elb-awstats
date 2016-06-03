#!/usr/bin/python3
'''
***
Write Lock Manager
Releases the lock on the directory that is specified
Also, shows any locks that exist (if requested)
***

Author: Philip Matuskiewicz - philip.matuskiewicz@nyct.com       

Changes:
	6/3/16 - Initial script
'''

import sys
import boto
from boto.s3.key import Key
import configparser

CONFIG = configparser.ConfigParser()
DATE_TO_PROCESS = False
if len(sys.argv) == 2 or len(sys.argv) == 3:
	inputini = sys.argv[1];
	if inputini.endswith(".ini"):
		CONFIG.read(inputini)
	else:
		print ("usage: ./locks.py <configfile> [<date_to_release_in_MMDDYYYY>], lists all locks, otherwise deletes specified release date")
		sys.exit(0)
	if len(sys.argv) == 3:
		#test format and set date
		DATE_TO_PROCESS = str(sys.argv[2]).strip()
		if len(DATE_TO_PROCESS) is not 8:
			print("Please enter the date to handle as MMDDYYYY (8 integers), you entered %s" % DATE_TO_PROCESS)
			sys.exit(0)
		if not DATE_TO_PROCESS.isdigit():
			print("The date to handle you entered is not in MMDDYYYY (8 integers), please try again.  You entered %s" % DATE_TO_PROCESS)
			sys.exit(0)
else:
	print ("usage: ./locks.py <configfile> [<date_to_release_in_MMDDYYYY>], lists all locks, otherwise deletes specified release date")
	sys.exit(0)

#Load configuration from ini file
DST_PATH = CONFIG.get('main', 'DST_PATH')
DST_AWS_ACCESS_KEY = CONFIG.get('main', 'DST_AWS_ACCESS_KEY')
DST_AWS_SECRET_KEY = CONFIG.get('main', 'DST_AWS_SECRET_KEY')
PROCESSING_LOCK_FILE = CONFIG.get('main', 'PROCESSING_LOCK_FILE')

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
		print("The lock file has been removed, the instance it was running on is %s" % instanceid)
	dst_s3conn.close()

if DATE_TO_PROCESS is not False:
	#MMDDYYYY
	m = DATE_TO_PROCESS[:2]
	d = DATE_TO_PROCESS[2:4]
	y = DATE_TO_PROCESS[4:]
	releaselockdir = "%s/%s/%s/" % (y,m,d)
	releaseLock(releaselockdir)
	sys.exit(0)

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
					print("The destination key \"%s\"(MMDDYYYY) is locked by instance \"%s\"" % (dstdir, instance))

dst_s3conn.close()
