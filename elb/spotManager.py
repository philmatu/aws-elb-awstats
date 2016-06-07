#!/usr/bin/python3
'''
***
Spot Instance Manager

./spotManager.py <configfile> <command> [<delete>]

Commands:
ListAll - List all instances and locks, note if instance is active, note if instance has a job running
ListOrphanedInstances - List all instances that are running but have no jobs attached to them (if "delete" is present as a second command, this will delete these instances listed)
ListMismatchedLocks - List all locks that mismatch with what the instance says it's doing, this should be empty, but if it has a value, it's left up to the user to figure out how to fix it
ListOrphanedLocks - List all locks that don't have an associated instance that is running (if "delete" is present as a second command, this will delete the locks listed)
DeleteLock - Deletes the lock file for a given directory (<delete> should be in form MMDDYYYY)
DeleteInstance - Deletes the instance specified (<delete> should be an instance-id)

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
import boto.ec2
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
INSTANCE_NAME_TAG = CONFIG.get('spot', 'INSTANCE_NAME_TAG')
EC2_REGION = CONFIG.get('spot', 'EC2_REGION')
EC2_AWS_ACCESS_KEY = CONFIG.get('spot', 'EC2_AWS_ACCESS_KEY')
EC2_AWS_SECRET_KEY = CONFIG.get('spot', 'EC2_AWS_SECRET_KEY')

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

def getLocks():
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

def getInstances():
	# EC2_AWS_ACCESS_KEY EC2_AWS_SECRET_KEY INSTANCE_NAME_TAG
	conn = boto.ec2.connect_to_region(EC2_REGION, aws_access_key_id=EC2_AWS_ACCESS_KEY, aws_secret_access_key=EC2_AWS_SECRET_KEY)
	instances = conn.get_all_instances(filters={"tag:Name" : INSTANCE_NAME_TAG})
	print (instances)


#command implementation 

#TODO: List all instances and locks, note if instance is active, note if instance has a job running
def do_listall():
	print(getInstances())

#TODO: List all instances that are running but have no jobs attached to them
def do_listorphanedinstances():
	return "True"

#TODO List all locks that mismatch with what the instance says it's doing
def do_listmismatchedlocks():
	return "True"

#TODO List all locks that don't have an associated instance that is running
def do_listorphanedlocks():
	locks = getLocks()
	for lock in locks:
		parts = lock.split("~")
		lockfilepath = parts[0]
		instanceid = parts[1]
		if DELETE_CMD:
			print("Not implemented, delete requested")	

#TODO only allow lock delete if it is not associated with a running instance, otherwise make force option
def do_deletelock():
	if DELETE_CMD.isdigit():
		#MMDDYYYY
		m = DELETE_CMD[:2]
		d = DELETE_CMD[2:4]
		y = DELETE_CMD[4:]
		releaselockdir = "%s/%s/%s/" % (y,m,d)
		releaseLock(releaselockdir)

#TODO delete the instance listed
def do_deleteinstance():
	return "True"

# run teh application by invoking the correct method	
possibles = globals().copy()
possibles.update(locals())
method = possibles.get(method_name)
if not method:
	raise NotImplementedError("Command %s is not implemented" % method_name)
method()
