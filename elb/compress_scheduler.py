#!/usr/bin/python3
'''
***
Compress Scheduler script
This script looks in the source directory for elb logs and holds them in memory
It then looks in the destination directory to find unproccessed files (missing / incomplete)
Incomplete files are removed automatically if the Lock file isn't there
Directories are then scheduled via Amazon's Queue Service
This script works across AWS accounts
***

Usage:
./compress_scheduler.py <configfile> [<date_to_process/startAt_in_MMDDYYYY/or_\"FILE\"_toUseAWStatsStatusFileDate> <date_to_end_at_if_intended>]

ex:
schedule everything that is ready for processing and wasn't already processed:
python3 compress_scheduler.py config.ini 

schedule only May 1, 2016
python3 compress_scheduler.py config.ini 05012016

schedule all files that aren't done between May 1, 2016 and May 10, 2016 (including this date)
python3 compress_scheduler.py config.ini 05012016 05102016

schedule all files that can be processed by awstats
python3 compress_scheduler.py config.ini file

schedule 

Author: Philip Matuskiewicz - philip.matuskiewicz@nyct.com       

Changes:
	5/28/16 - Initial Logic / Script
	5/30/16 - Final Logic to get data into SQS
	5/31/16 - Added date-to-handle parameter option to avoid back-processing
	6/1/16 - Status file in gz format accounted for, still needs to account for reprocess queue
	6/2/16 - Finalized Script with Queues implemented
	6/9/16 - Added a second parameter to allow for a queue range
'''

import sys
import os
import boto
import boto.sqs
import datetime
from boto.sqs.message import RawMessage
from boto.s3.key import Key
import configparser
import json

CONFIG = configparser.ConfigParser()

USE_AWSTATS_POSITION_FILE = False
DATE_TO_PROCESS = False
DATE_TO_END = False
ENQUEUED_TASKS = list()
if len(sys.argv) == 2 or len(sys.argv) == 3 or len(sys.argv) == 4:
	inputini = sys.argv[1];
	if inputini.endswith(".ini"):
		CONFIG.read(inputini)
	else:
		print ("usage: ./compress_scheduler.py <configfile> [<date_to_process/startAt_in_MMDDYYYY/or_\"FILE\"_toUseAWStatsStatusFileDate> <date_to_end_at_if_intended>]")
		sys.exit(0)
	if len(sys.argv) == 3:
		if "file" in str(sys.argv[2]).strip().lower():
			USE_AWSTATS_POSITION_FILE = True
	if (not USE_AWSTATS_POSITION_FILE) and (len(sys.argv) == 3 or len(sys.argv) == 4):
		if len(sys.argv) == 4:
			DATE_TO_END = str(sys.argv[3]).strip()
			if len(DATE_TO_END) is not 8:
				print("Please enter the end date as MMDDYYYY (8 integers), you entered %s" % DATE_TO_END)
				sys.exit(0)
			if not DATE_TO_END.isdigit():
				print("The end date to handle you entered is not in MMDDYYYY (8 integers), please try again.  You entered %s" % DATE_TO_END)
				sys.exit(0)
		#test format and set date
		DATE_TO_PROCESS = str(sys.argv[2]).strip()
		if len(DATE_TO_PROCESS) is not 8:
			print("Please enter the date to handle as MMDDYYYY (8 integers) or \"FILE\", you entered %s" % DATE_TO_PROCESS)
			sys.exit(0)
		if not DATE_TO_PROCESS.isdigit():
			print("The date to handle you entered is not in MMDDYYYY (8 integers) or \"FILE\", please try again.  You entered %s" % DATE_TO_PROCESS)
			sys.exit(0)
else:
	print ("usage: ./compress_scheduler.py <configfile> [<date_to_process/startAt_in_MMDDYYYY/or_\"FILE\"_toUseAWStatsStatusFileDate> <date_to_end_at_if_intended>]")
	sys.exit(0)

#Load configuration from ini file
SRC_PATH = CONFIG.get('main', 'SRC_PATH')
SRC_AWS_ACCESS_KEY = CONFIG.get('main', 'SRC_AWS_ACCESS_KEY')
SRC_AWS_SECRET_KEY = CONFIG.get('main', 'SRC_AWS_SECRET_KEY')
DST_PATH = CONFIG.get('main', 'DST_PATH')
DST_AWS_ACCESS_KEY = CONFIG.get('main', 'DST_AWS_ACCESS_KEY')
DST_AWS_SECRET_KEY = CONFIG.get('main', 'DST_AWS_SECRET_KEY')
PROCESSING_STATUS_FILE = CONFIG.get('main', 'PROCESSING_STATUS_FILE') # contains all files that are finished, contains DONE if all processing is done
PROCESSING_STATUS_FILE_COMPLETE_TXT = CONFIG.get('main', 'PROCESSING_STATUS_FILE_COMPLETE_TXT') 
PROCESSING_LOCK_FILE = CONFIG.get('main', 'PROCESSING_LOCK_FILE')
QUEUE_NAME = CONFIG.get('main', 'QUEUE_NAME')
INCOMPLETE_TASKS_QUEUE_NAME = CONFIG.get('main', 'INCOMPLETE_TASKS_QUEUE_NAME')
QUEUE_AWS_ACCESS_KEY = CONFIG.get('main', 'QUEUE_AWS_ACCESS_KEY')
QUEUE_AWS_SECRET_KEY = CONFIG.get('main', 'QUEUE_AWS_SECRET_KEY')
AWSTATS_LAST_ADDED_FILE = CONFIG.get('main', 'AWSTATS_LAST_ADDED_FILE')

PREVQUEUEITEMS = set()
#SQS allows max string length of 256KB, in my case, the max is around 50KB which is sufficient for our needs
def enqueue(dstdir, tasks):
	qconn = boto.sqs.connect_to_region("us-east-1", aws_access_key_id=QUEUE_AWS_ACCESS_KEY, aws_secret_access_key=QUEUE_AWS_SECRET_KEY)
	logProcQueue = qconn.get_queue(QUEUE_NAME)
	if logProcQueue is None:
		print ("Creating SQS Queue: %s with Key %s" % (QUEUE_NAME,QUEUE_AWS_ACCESS_KEY))
		logProcQueue = qconn.create_queue(QUEUE_NAME)
	
	data_out = {}
	data_out['directory'] = "%s/" % dstdir
	data_out['tasklist'] = tasks
	
	#get all the previous tasks in teh queue already to ensure no duplicates, then readd them
	messages = logProcQueue.get_messages(visibility_timeout=30, wait_time_seconds=2, num_messages=10)
	while len(messages) > 0:
		for message in messages:
			raw_json = message.get_body()
			data = json.loads(raw_json)
			if len(data['directory']) > 0:
				PREVQUEUEITEMS.add(data['directory'])
		messages = logProcQueue.get_messages(visibility_timeout=30, wait_time_seconds=2, num_messages=10)#continue reading
	
	if data_out['directory'] in PREVQUEUEITEMS:
		print("The directory \"%s\" is already in the processing queue, skipping" % data_out['directory'])
	else:
		json_tasks = json.dumps(data_out)
		if len(json_tasks) > 250000:
			print("Task %s has too much data, going to send 'too_long' so the worker does a manual lookup of work to do" % (data_out['directory']))
			data_out = {}
			data_out['directory'] = "%s/" % dstdir
			data_out['tasklist'] = "too_long"
			json_tasks = json.dumps(data_out)
		queuemessage = RawMessage()
		queuemessage.set_body(json_tasks)
		print("Enqueing Task %s" % data_out['directory'])
		logProcQueue.write(queuemessage)
	
	qconn.close()

#destination directory of the remote server, and the files that should be there (plus the status file)
def processDirectory(dstdir, dirlist):
	#create s3 connection on destination directory
	dests3conn = boto.connect_s3(DST_AWS_ACCESS_KEY, DST_AWS_SECRET_KEY)
	destbucket = dests3conn.get_bucket(DST_PATH[:DST_PATH.index('/')])
	
	#does DST_PATH exist?
	dst_path = "%s%s" % (DST_PATH[DST_PATH.index('/')+1:],dstdir)
	dst_path_list = destbucket.list(prefix=dst_path, delimiter='/')
	dst_path_exists = False
	for dstresult in dst_path_list:
		if DST_PATH[DST_PATH.index('/')+1:] in dstresult.name:
			dst_path_exists = True
		
	#prepare to either create / retrieve the status file
	status_file_path = "%s/%s" % (dst_path,PROCESSING_STATUS_FILE)
	status_file_key = Key(destbucket, status_file_path)
	
	#prepare to look for the lock file, if it exists, simply pass a warning out and move on (as if we've already covered the directory)
	lock_file_path = "%s/%s" % (dst_path,PROCESSING_LOCK_FILE)
	lock_file_key = Key(destbucket, lock_file_path)
	
	#make sure that we can get the data file from the directory if it exists
	if dst_path_exists:
		if lock_file_key.exists():
			locking_machine = bytes(lock_file_key.get_contents_as_string()).decode(encoding='UTF-8')
			print ("WARN: Lock file exists on %s, The instance id listed is: %s" % (lock_file_path,locking_machine))
			return
		if not status_file_key.exists():
			print ("WARN: failed to retrieve file \"%s\", deleting the existing directory and starting over." % status_file_path)
			for dk in destbucket.list(prefix=dst_path):
				print("Deleting the key \"%s\"" % dk)
				destbucket.delete_key(dk)
			dst_path_exists = False
	
	filesInDstDirectory = list()
	dst_path_list_full = destbucket.list(prefix=dst_path)
	for dstresult_full in dst_path_list_full:
		filesInDstDirectory.append(dstresult_full.name)
	
	if dst_path_exists:
		print ("The directory %s exists, checking that all files are complete" % dst_path)
		status_file_text = bytes(status_file_key.get_contents_as_string()).decode(encoding='UTF-8')
		
		firstLine = True
		#create a list of all the files in the meta data file that it claims are completed
		completedFiles = list()
		if len(status_file_text) > 0:
			for line in status_file_text.split("\n"):
				if firstLine:
					firstLine = False
					if PROCESSING_STATUS_FILE_COMPLETE_TXT in line:
						print("The directory %s is completed already, exiting this directory"%dst_path)
						return
				completedFiles.append(line)
				#find any file in the meta data that isn't present in the source directory, warn on these
				#as it may be a corrupted meta data file
				if line not in dirlist: #file not in source, keep in mind "not in" will allow for gz handling without issue
					#NOTE: perhaps in the future, we can remove the rogue lines from this file
					print ("WARNING: the file \"%s\" wasn't found in the source directory, is the meta data corrupted?" % line)

		#find any file on the source that isn't listed in the completed meta data file
		ToBeProcessedFiles = list()
		REMOVEFILE = list()
		for srcfilename in dirlist:
			if srcfilename not in completedFiles:
				if PROCESSING_STATUS_FILE not in srcfilename:#except for the status file, which would be present
					ToBeProcessedFiles.append(srcfilename)
			#find any file in the destination directory that isn't on the completed list and is not slated for processing
			#these files are incomplete, and need to be reprocessed / deleted (and are present in the source directory)
			for df in filesInDstDirectory:
				if df in srcfilename:
					print ("Warn, <srcdir> file \"%s\" exists, but not marked completed, NOW deleting and add for reprocessing" % srcfilename)
					REMOVEFILE.append(df)
					ToBeProcessedFiles.append(srcfilename)
					
		#finally, find rogue files and warn about them (files in the dest directory, but on on meta data or source directory)
		for dstfilename in filesInDstDirectory:
			if PROCESSING_STATUS_FILE not in dstfilename:#except for the status file, which would be present
				dstcheckstring = dstfilename.split("/")[-1]
				if dstcheckstring.endswith(".gz"):
					dstcheckstring = dstcheckstring[:-3]
				if dstcheckstring not in completedFiles:
				#the file isn't in the meta data or slated for completetion (from source directory)
					if dstcheckstring not in ToBeProcessedFiles:
						print ("WARNING: The rogue file \"%s\" is present in the destination directory, you might want to delete this." % dstfilename)
		for remkey in REMOVEFILE:
			print ("Deleting incomplete / unwanted file \"%s\"" % remkey)
			rogue_deletion_file_key = Key(destbucket, remkey)
			destbucket.delete_key(rogue_deletion_file_key)
	
	else:
		print ("The directory %s does not exist, creating task to process this directory" % dst_path)
		status_file_key.set_contents_from_string("")
		ToBeProcessedFiles = list()
		for srcfilename in dirlist:
			ToBeProcessedFiles.append(srcfilename)
	
	if len(ToBeProcessedFiles) > 0:
		print("Queuing directory %s for processing" % dstdir)
		enqueue(dstdir, ToBeProcessedFiles)
		ENQUEUED_TASKS.append(dstdir)
	else:
		print("The directory should already be complete, queuing to get the line added! %s"%dstdir)
		enqueue(dstdir, ToBeProcessedFiles)
		ENQUEUED_TASKS.append(dstdir)
	dests3conn.close()

def readIncompleteQueue(deleteAfterRead=True):
	qconn = boto.sqs.connect_to_region("us-east-1", aws_access_key_id=QUEUE_AWS_ACCESS_KEY, aws_secret_access_key=QUEUE_AWS_SECRET_KEY)
	logProcQueue = qconn.get_queue(INCOMPLETE_TASKS_QUEUE_NAME)
	if logProcQueue is None:
		print("Checked the incomplete queue %s, nothing was there, so we'll continue with scheduling as normal")
		return list()
	out = set()#ensures unique values
	messages = logProcQueue.get_messages(visibility_timeout=10,wait_time_seconds=2, num_messages=10)
	while len(messages) > 0:
		for message in messages:
			raw_json = message.get_body()
			data = json.loads(raw_json)
			if len(data['directory']) > 0:
				out.add(data['directory'][:-1])#remove final / which is in directory
			if deleteAfterRead:
				logProcQueue.delete_message(message)
		messages = logProcQueue.get_messages(visibility_timeout=10,wait_time_seconds=2, num_messages=10)#continue reading
	qconn.close()
	return out

def enQueueNonCompletedDirectory(directory):
	qconn = boto.sqs.connect_to_region("us-east-1", aws_access_key_id=QUEUE_AWS_ACCESS_KEY, aws_secret_access_key=QUEUE_AWS_SECRET_KEY)
	logProcQueue = qconn.get_queue(INCOMPLETE_TASKS_QUEUE_NAME)
	if logProcQueue is None:
		print ("Creating SQS Queue: %s with Key %s" % (INCOMPLETE_TASKS_QUEUE_NAME,QUEUE_AWS_ACCESS_KEY))
		logProcQueue = qconn.create_queue(INCOMPLETE_TASKS_QUEUE_NAME)
	data_out = {}
	data_out['directory'] = directory #in format of yyyy/mm/dd
	json_encoded_message = json.dumps(data_out)
	queuemessage = RawMessage()
	queuemessage.set_body(json_encoded_message)
	print("Enqueing Directory (YYYY/MM/DD) %s for reconsideration, I didn't touch it this time for whatever reason" % data_out['directory'])
	logProcQueue.write(queuemessage)

#Begin main code

#assume that system clock is in GMT/UTC as AWS logs are in
todaysdate = datetime.datetime.today().strftime('%Y/%m/%d/')

startdate = False
enddate = False
matchdir = False
endmatchdir = False
if DATE_TO_END is not False:
	#MMDDYYYY
	em = DATE_TO_END[:2]
	ed = DATE_TO_END[2:4]
	ey = DATE_TO_END[4:]
	endmatchdir = "%s/%s/%s" % (ey,em,ed)
	enddate = datetime.datetime.strptime(DATE_TO_END, "%m%d%Y").date()
if DATE_TO_PROCESS is not False:
	#MMDDYYYY
	sm = DATE_TO_PROCESS[:2]
	sd = DATE_TO_PROCESS[2:4]
	sy = DATE_TO_PROCESS[4:]
	startdate = datetime.datetime.strptime(DATE_TO_PROCESS, "%m%d%Y").date()
	matchdir = "%s/%s/%s" % (sy,sm,sd)

if USE_AWSTATS_POSITION_FILE:
	if os.path.exists(AWSTATS_LAST_ADDED_FILE):
		with open(AWSTATS_LAST_ADDED_FILE, "r") as startfile:
			for line in startfile:
				line = line.strip()
				if len(line) > 3: # line will be in YYYYMMDD or %Y%m%d
					sy = line[:4]
					sm = line[4:6]
					sd = line[6:8]
					startdate = datetime.datetime.strptime(line, "%Y%m%d").date()
					matchdir = "%s/%s/%s" % (sy,sm,sd)
					enddate = datetime.datetime.strptime(todaysdate, "%Y/%m/%d/").date()
					endmatchdir = enddate.strftime('%Y/%m/%d')
	else:
		print("The AWStats file %s doesn't exist, make sure you've actually run the awstats updater first with a fresh processed batch.")
		sys.exit(0)

INCOMPLETE_LIST = list()
INCOMPLETE_DEL = False
if endmatchdir is False and matchdir is not False:
	#if we're running a match directory operation, we shouldn't look at incomplete items
	INCOMPLETE_LIST = readIncompleteQueue(deleteAfterRead=False)
elif USE_AWSTATS_POSITION_FILE:
	INCOMPLETE_LIST = readIncompleteQueue(deleteAfterRead=False)
else:
	INCOMPLETE_DEL = True
	INCOMPLETE_LIST = readIncompleteQueue()

s3conn = boto.connect_s3(SRC_AWS_ACCESS_KEY, SRC_AWS_SECRET_KEY)
bucket = s3conn.get_bucket(SRC_PATH[:SRC_PATH.index('/')])
for year in bucket.list(prefix=SRC_PATH[SRC_PATH.index('/')+1:], delimiter='/'): 
	yearint = year.name[-5:-1]
	for month in bucket.list(prefix=year.name, delimiter='/'):
		monthint = month.name[-3:-1]
		for day in bucket.list(prefix=month.name, delimiter='/'):
			testdirname = ""
			if day.name.endswith('/'):
				testdirname = day.name[len(day.name)-11:-1]
			else:
				testdirname = day.name[len(day.name)-11:-1]
			digitsum = sum(c.isdigit() for c in testdirname)
			if digitsum != 8:
				sys.exit(0)
				#first directory... no files in it
				continue
			dirlist = list()
			dayint = day.name[-3:-1]
			srcdir = day.name
			dstdir = "%s/%s/%s" % (yearint, monthint, dayint)
			if dstdir in todaysdate:
				continue # don't process today's date
			if matchdir is not False:
				if (dstdir not in matchdir) and (endmatchdir is False):
					continue
				elif endmatchdir is not False:
					currentprocdate = datetime.datetime.strptime("%s%s%s"%(monthint,dayint,yearint), "%m%d%Y").date()
					if USE_AWSTATS_POSITION_FILE:
						if not startdate <= currentprocdate < enddate:
							continue
					else:
						if not startdate <= currentprocdate <= enddate:
							continue
			for fileWithPath in bucket.list(prefix=srcdir, delimiter='/'):
				fname = fileWithPath.name.split('/')[-1]
				dirlist.append(fname)
			processDirectory(dstdir, dirlist)
for task in INCOMPLETE_LIST:
	if task not in ENQUEUED_TASKS:
		if INCOMPLETE_DEL:
			enQueueNonCompletedDirectory("%s/"%task)
		if matchdir is False:
			print("WARNING: The task \"%s\" wasn't queued for reprocessing, yet it failed on a worker... Please manually verify!" % task)
	else:
		print("info: The task \"%s\" was queued for reprocessing... it previously failed on a worker node." % task)
print("Please kindly wait 30 seconds before reexecuting this script, the queue needs to restore itself.")
s3conn.close()
