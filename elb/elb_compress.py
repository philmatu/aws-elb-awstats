#!/usr/bin/python3
'''
***
ELB Log Compressor script
Takes logs from a given directory and places a cleaned gzip of them in another s3 directory (which will also be compatible with awstats)
Input Logs are fed in through Amazon SQS via the scheduler, this works across AWS Accounts!
This is meant to be run on Spot Instances, meaning reduced cost to process huge amounts of data logs
***

Author: Philip Matuskiewicz - philip.matuskiewicz@nyct.com       

Changes:
	5/14/16 - Initial Script
	5/28/16 - Configuration added, Parameters to compress individually added (external coordinator)
	5/30/16 - Incorporated SQS for spot instance use, not ready to use yet
	5/31/16 - Added compression support / upload to s3 support / lock/status file updates (for parts)
	6/2/16 - Finalized Script
	6/3/16 - added signal killing abilities, not tested yet in this commit #TODO
'''

import sys
import signal
from io import BytesIO
import boto
import boto.sqs
from boto.sqs.message import Message
from boto.s3.key import Key
import smart_open
import threading
import concurrent.futures
import json
import time
import shlex
import re
from urllib.parse import urlencode, urlparse, parse_qsl, urlunparse
import urllib.request
import configparser
import gzip

WRITE_LOCK = threading.Lock()
CONFIG = configparser.ConfigParser()

if len(sys.argv) == 2:
        inputini = sys.argv[1];
        if inputini.endswith(".ini"):
                CONFIG.read(inputini)
        else:
                print ("usage: ./elb_compress.py <configfile>")
                sys.exit(0)
else:
        print ("usage: ./elb_compress.py <configfile>")
        sys.exit(0)

#Load configuration from ini file
SRC_PATH = CONFIG.get('main', 'SRC_PATH')
SRC_AWS_ACCESS_KEY = CONFIG.get('main', 'SRC_AWS_ACCESS_KEY')
SRC_AWS_SECRET_KEY = CONFIG.get('main', 'SRC_AWS_SECRET_KEY')
DST_PATH = CONFIG.get('main', 'DST_PATH')
DST_AWS_ACCESS_KEY = CONFIG.get('main', 'DST_AWS_ACCESS_KEY')
DST_AWS_SECRET_KEY = CONFIG.get('main', 'DST_AWS_SECRET_KEY')
REMOVE_QUERY_STRING_KEYS = CONFIG.get('main', 'REMOVE_QUERY_STRING_KEYS').split(",")
PROCESSING_STATUS_FILE = CONFIG.get('main', 'PROCESSING_STATUS_FILE') # contains all files that are finished, contains DONE if all processing is done
PROCESSING_STATUS_FILE_COMPLETE_TXT = CONFIG.get('main', 'PROCESSING_STATUS_FILE_COMPLETE_TXT').strip()
PROCESSING_LOCK_FILE = CONFIG.get('main', 'PROCESSING_LOCK_FILE')
QUEUE_NAME = CONFIG.get('main', 'QUEUE_NAME')
INCOMPLETE_TASKS_QUEUE_NAME = CONFIG.get('main', 'INCOMPLETE_TASKS_QUEUE_NAME')
QUEUE_AWS_ACCESS_KEY = CONFIG.get('main', 'QUEUE_AWS_ACCESS_KEY')
QUEUE_AWS_SECRET_KEY = CONFIG.get('main', 'QUEUE_AWS_SECRET_KEY')
#this script does not process anything from the current day due to added logic to keep track of hourly file dumps

#compiled regex for threading, these compiled bits are thread safe
spacePorts = re.compile('( \d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):([0-9][0-9]*)')
removeHost = re.compile('(http|https)://.*:(80|443)')
fixTime = re.compile('([0-9]{4}-[0-9]{2}-[0-9]{2})T([0-9]{2}:[0-9]{2}:[0-9]{2})\.[0-9]*Z')

#global constant variables
DIRECTORY = ""#what comes after both SRC_PATH and DST_PATH, folder wise, received via Queue
CHUNK_SIZE = 8192 #compression block size
awsmetaurl = "http://169.254.169.254/latest/meta-data/instance-id"

def handle_SIGINT_THREADS(signum, frame):
	print("Thread canceling")

def handle_SIGINT_MAIN(signum, frame):
	print("Caught the kill signal from ctrl c, Publishing directory \"%s\" to incomplete queue" % DIRECTORY)
	enQueueNonCompletedDirectory(DIRECTORY)
	releaseLock(DIRECTORY)
	sys.exit(0)

def createLock(filePath):
	dst_s3conn = boto.connect_s3(DST_AWS_ACCESS_KEY, DST_AWS_SECRET_KEY)
	dst_bucket = dst_s3conn.get_bucket(DST_PATH[:DST_PATH.index('/')])
	lock_file_path = "%s%s%s" % (DST_PATH[DST_PATH.index('/'):],filePath,PROCESSING_LOCK_FILE)
	lock_file_key = Key(dst_bucket, lock_file_path)
	if not lock_file_key.exists():
		resource = urllib.request.urlopen(awsmetaurl)
		instanceid = resource.read().decode('utf-8')
		lock_file_key.set_contents_from_string(instanceid)
		print("The lock is now acquired to begin processing on %s" % lock_file_path)
		dst_s3conn.close()
		return True
	else:
		instanceid = bytes(lock_file_key.get_contents_as_string()).decode(encoding='UTF-8')
		print("The lock file exists, the instance running is %s" % instanceid[2:-1])
	dst_s3conn.close()
	return False

def releaseLock(filePath):
	dst_s3conn = boto.connect_s3(DST_AWS_ACCESS_KEY, DST_AWS_SECRET_KEY)
	dst_bucket = dst_s3conn.get_bucket(DST_PATH[:DST_PATH.index('/')])
	lock_file_path = "%s%s%s" % (DST_PATH[DST_PATH.index('/'):],filePath,PROCESSING_LOCK_FILE)
	lock_file_key = Key(dst_bucket, lock_file_path)
	if not lock_file_key.exists():
		print("OH CRAP... There was no lock... hoping nothing went wrong!  You may want to verify this.")
		return False
	else:
		instanceid = bytes(lock_file_key.get_contents_as_string()).decode(encoding='UTF-8')
		resource = urllib.request.urlopen(awsmetaurl)
		myinstanceid = resource.read().decode('utf-8')
		if myinstanceid not in instanceid:
			print("WARNING: the instance id's don't match, mine is %s, the lock one is %s" %(myinstanceid,instanceid))
		dst_bucket.delete_key(lock_file_key)
		print("The lock file has been removed, releasing for future processing")
	dst_s3conn.close()
	return False

def isAlreadyInStatusFile(completedFile):
	dst_s3conn = boto.connect_s3(DST_AWS_ACCESS_KEY, DST_AWS_SECRET_KEY)
	dst_bucket = dst_s3conn.get_bucket(DST_PATH[:DST_PATH.index('/')])
	status_file_path = "%s/%s" % (completedFile.rsplit('/', 1)[0],PROCESSING_STATUS_FILE)
	status_file_key = Key(dst_bucket, status_file_path)
	theCompletedFile = completedFile.rsplit('/', 1)[1]
	if not status_file_key.exists():
		return True
	else:
		status_file_text = bytes(status_file_key.get_contents_as_string()).decode(encoding='UTF-8')
		if theCompletedFile in status_file_text:
			return True
	return False

#the completedFile doesn't have a .gz, even though we know all files here should end in GZ, this is handled by the scheduler
def updateStatusFile(completedFile, completionListVerify=list()):
	dst_s3conn = boto.connect_s3(DST_AWS_ACCESS_KEY, DST_AWS_SECRET_KEY)
	dst_bucket = dst_s3conn.get_bucket(DST_PATH[:DST_PATH.index('/')])
	status_file_path = "%s/%s" % (completedFile.rsplit('/', 1)[0],PROCESSING_STATUS_FILE)
	status_file_key = Key(dst_bucket, status_file_path)
	theCompletedFile = completedFile.rsplit('/', 1)[1].strip()
	
	if not status_file_key.exists():
		if len(completionListVerify) > 0:
			print("Seeking Verification for %s, but this directory has no status file... rerun scheduler to delete / restart processing please.  I'll end now on this directory" % completedFile)
			return
		print ("WARN: failed to retrieve file \"%s\", starting new key." % status_file_path)
		status_file_key.set_contents_from_string(theCompletedFile)
	else:
		status_file_text = bytes(status_file_key.get_contents_as_string()).decode(encoding='UTF-8')
		if len(completionListVerify) > 0:
			for line in completionListVerify:
				if line not in status_file_text.split("\n"):
					print("The task \"%s\" isn't in the status file, this means I didn't complete successfully... I will notify the scheduler queue of my directory: %s" % (line,completedFile))
					enQueueNonCompletedDirectory(DIRECTORY)#string in YYYY/MM/DD from the original queue
					return
			for line in status_file_text.split("\n"):
				if line not in completionListVerify:
					print("The line \"%s\" was not found in the list of tasks, this means I didn't complete successfully... I will notify the scheduler queue of my directory: %s" % (line,completedFile))
					enQueueNonCompletedDirectory(DIRECTORY)#string in YYYY/MM/DD from the original queue
					return
			print ("All files that were queued by scheduler are finished and in status file, appending completion text now for the next pipeline step, directory is %s" % completedFile)
			new_status_file_text = "%s\n%s" % (PROCESSING_STATUS_FILE_COMPLETE_TXT,status_file_text)
			status_file_key.set_contents_from_string(new_status_file_text)
		else:
			if len(status_file_text) < 3:
				new_status_file_text = theCompletedFile
			else:
				new_status_file_text = "%s\n%s" % (status_file_text, theCompletedFile)
			status_file_key.set_contents_from_string(new_status_file_text)
	print("Updated Status file with latest data, file %s" % theCompletedFile)

def compress(src): #takes in a filename that is in the SRCPATH directory and places a compressed/gzipped version into DSTPATH
	signal.signal(signal.SIGINT, handle_SIGINT_THREADS)
	if len(src) < 15:
		return
	src_s3conn = boto.connect_s3(SRC_AWS_ACCESS_KEY, SRC_AWS_SECRET_KEY)
	src_bucket = src_s3conn.get_bucket(SRC_PATH[:SRC_PATH.index('/')])
	src_path = "%s%s%s" % (SRC_PATH.split("/", 1)[1], DIRECTORY, src)
	srcFileKey = src_bucket.get_key(src_path)

	dst_s3conn = boto.connect_s3(DST_AWS_ACCESS_KEY, DST_AWS_SECRET_KEY)
	dst_bucket = dst_s3conn.get_bucket(DST_PATH[:DST_PATH.index('/')])
	dst_path_sans_GZ = "%s%s%s" % (DST_PATH.split("/", 1)[1], DIRECTORY, src)
	dst_path = "%s.gz" % dst_path_sans_GZ
	mpu = dst_bucket.initiate_multipart_upload(dst_path)

	if isAlreadyInStatusFile(dst_path_sans_GZ):
		print("The file %s is already in the status file, meaning it should be done... skipping" % dst_path_sans_GZ)
		return

	buf = "" #buffer to hold onto chunks of data at a time
	part = 1
	outStream = BytesIO()
	compressor = gzip.GzipFile(fileobj=outStream, mode='wb')
	with smart_open.smart_open(srcFileKey) as srcStream:
		for line in srcStream:
			cleanedString = clean(line)
			if len(cleanedString) < 1:
				continue
			buf = "%s%s\n" % (buf, cleanedString)
			while(len(buf) > CHUNK_SIZE):
				block = bytes(buf[:CHUNK_SIZE],'utf_8')
				#write compressed out as binary data
				compressor.write(block)
				if outStream.tell() > 10<<20:  # min size for multipart upload is 5242880
					outStream.seek(0)
					mpu.upload_part_from_file(outStream, part)
					outStream.seek(0)
					outStream.truncate()
					part = part + 1
				buf = buf[CHUNK_SIZE:]
	if(len(buf) > 0):
		block = bytes(buf[:CHUNK_SIZE],'utf_8')
		#write compressed out as binary data
		compressor.write(block)
		compressor.close()
		outStream.seek(0)
		mpu.upload_part_from_file(outStream, part)
		mpu.complete_upload()
	with WRITE_LOCK:
		updateStatusFile(dst_path_sans_GZ)

def clean(line):
	line = line.strip()
	if len(line) < 20:
		return ""
	line = str(line)[2:-1] #convert byte into string
	line = spacePorts.sub('\\1 \\2', line)
	line = removeHost.sub('', line)
	line = fixTime.sub('\\1 \\2', line)
	
	#we are missing a backend processing time, since a 504 fails, so replace this on the lines where we have a 504, eliminating many errors
	line = line.replace("-1 -1 -1 504 0 0 0", "-1 -1 -1 -1 504 0 0 0")
	
	splt = len(shlex.split(line)) #lexical parse gives me tokens enclosed by quotes for url string
	if splt is 15:
		line = ("%s \"\" - -\n" % line)
	elif splt is 16:
		line = ("%s - -\n" % line)
	elif splt is 18:
		line = ("%s\n" % line)
	else:
		print("Malformed line, skpping: %s" % line)
		return ""
	line = shlex.split(line)
#original log file format
#%time2 %elb %host %host_port %host_r %host_r_port %request_processing_time %backend_processing_time %response_processing_time %code %backend_status_code %received_bytes %bytesd %methodurl %uaquot %other %other
# new log file format
# %time2 %host %host_port %backend_processing_time %backend_status_code %bytesd %methodurl %uaquot %encryption_layer
#AWStats format
# %time2 %host %host_port %other %code %bytesd %methodurl %uaquot %other
	
	methodurl_stripped = line[14]
	if "GET" not in methodurl_stripped:
		if "HTTP" not in methodurl_stripped:
			if "POST" not in methodurl_stripped:
				print("Malformed url at 14, skpping: %s" % line)
				return ""
	url_parts = methodurl_stripped.split(" ")
	qs = list(urlparse(url_parts[1]))
	if len(qs[4]) > 0:
		qs_parts = dict(parse_qsl(qs[4]))
		for removeKey in REMOVE_QUERY_STRING_KEYS:
			if removeKey in qs_parts:
				del qs_parts[removeKey]
		qs[4] = urlencode(qs_parts)
		new_method = urlunparse(qs)
		methodurl_stripped = "%s %s %s" % (url_parts[0], new_method, url_parts[2])
	finalLine = "%s %s %s %s %s %s %s \"%s\" \"%s\" %s" % (line[0], line[1], line[3], line[4], line[8], line[11], line[13], methodurl_stripped, line[15], line[16])
	return finalLine
	

def readQueue():
	qconn = boto.sqs.connect_to_region("us-east-1", aws_access_key_id=QUEUE_AWS_ACCESS_KEY, aws_secret_access_key=QUEUE_AWS_SECRET_KEY)
	logProcQueue = qconn.get_queue(QUEUE_NAME)
	if logProcQueue is None:
		print ("No such Queue on SQS called %s with account %s" % (QUEUE_NAME, QUEUE_AWS_ACCESS_KEY))
		sys.exit(0)
	readMessage = logProcQueue.read(visibility_timeout=10) #give me 10 seconds to remove the queue item
	if readMessage is not None:
		logProcQueue.delete_message(readMessage)
		return readMessage.get_body()
	return None

def enQueueNonCompletedDirectory(directory):
	qconn = boto.sqs.connect_to_region("us-east-1", aws_access_key_id=QUEUE_AWS_ACCESS_KEY, aws_secret_access_key=QUEUE_AWS_SECRET_KEY)
	logProcQueue = qconn.get_queue(INCOMPLETE_TASKS_QUEUE_NAME)
	if logProcQueue is None:
		print ("Creating SQS Queue: %s with Key %s" % (INCOMPLETE_TASKS_QUEUE_NAME,QUEUE_AWS_ACCESS_KEY))
		logProcQueue = qconn.create_queue(INCOMPLETE_TASKS_QUEUE_NAME)
	data_out = {}
	data_out['directory'] = directory #in format of yyyy/mm/dd
	json_encoded_message = json.dumps(data_out)
	queuemessage = Message()
	queuemessage.set_body(json_encoded_message)
	print("Enqueing Directory (YYYY/MM/DD) %s for re-scheduling and re-processing due to incomplete processing with me" % data_out['directory'])
	logProcQueue.write(queuemessage)

signal.signal(signal.SIGINT, handle_SIGINT_MAIN)
while True:
	count = 0
	message = readQueue()
	if message is None:
		count = count + 1
		if count > 5:
			print("There were no messages in the queue, no need to remain operational.  Quitting.")
			sys.exit(0)
		print("No data in queue, waiting 5 seconds and trying again")
		time.sleep(5) #5 second sleep, 25 second total wait from queue before we consider all tasks done for the day		
		continue
	count = 0
	data = json.loads(message)
	DIRECTORY = data['directory'] #appended to src and dst path from configuration file
	tasks = data['tasklist']
	if createLock(DIRECTORY):
		with concurrent.futures.ProcessPoolExecutor() as executor:
			executor.map(compress, tasks)
		with WRITE_LOCK:
			src_path = "%s%s" % (DST_PATH.split("/", 1)[1], DIRECTORY)
			updateStatusFile(src_path, tasks) #completion is here if all checks out... we assume that the scheduler's queue has all the tasks for a day in this list
		releaseLock(DIRECTORY)
	else:
		print("Exiting without doing work, couldn't acquire a lock for processing the date associated with %s." % tasks[0]);
	print("Done")
	sys.exit(0)

#TODO
#on kill we need to run enQueueNonCompletedDirectory(DIRECTORY) and then releaseLock(DIRECTORY)

#TODO we need to add a thread to the parent idle process that checks aws meta data for termination notice
