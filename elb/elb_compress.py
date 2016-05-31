#!/usr/bin/python3
'''
***
ELB Log Compressor script
Takes logs from a given directory and places a cleaned gzip of them in another s3 directory (which will also be compatible with awstats)
Logs are fed in through Amazon SQS via the scheduler, this works across AWS Accounts!
This is meant to be run on Spot Instances, meaning reduced cost to process huge amounts of data logs
***

Author: Philip Matuskiewicz - philip.matuskiewicz@nyct.com       

Changes:
	5/14/16 - Initial Script
	5/28/16 - Configuration added, Parameters to compress individually added (external coordinator)
	5/30/16 - Incorporated SQS for spot instance use, not ready to use yet
'''

import sys
import boto
import boto.sqs
from boto.sqs.message import Message
from boto.s3.key import Key
import smart_open
import concurrent.futures
import shlex
import re
from urllib.parse import urlencode, urlparse, parse_qsl, urlunparse
import configparser

CONFIG = configparser.ConfigParser()

if len(sys.argv) == 3:
        inputini = sys.argv[1];
        if inputini.endswith(".ini"):
                CONFIG.read(inputini)
        else:
                print ("usage: ./elb_compress.py <configfile> <date_to_handle_in_MMDDYYYY>")
                sys.exit(0)
else:
        print ("usage: ./elb_compress.py <configfile> <date_to_handle_in_MMDDYYYY>")
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
PROCESSING_STATUS_FILE_COMPLETE_TXT = CONFIG.get('main', 'PROCESSING_STATUS_FILE_COMPLETE_TXT')
PROCESSING_LOCK_FILE = CONFIG.get('main', 'PROCESSING_LOCK_FILE')
QUEUE_NAME = CONFIG.get('main', 'QUEUE_NAME')
QUEUE_AWS_ACCESS_KEY = CONFIG.get('main', 'QUEUE_AWS_ACCESS_KEY')
QUEUE_AWS_SECRET_KEY = CONFIG.get('main', 'QUEUE_AWS_SECRET_KEY')
#this script does not process anything from the current day due to added logic to keep track of hourly file dumps

DSTDIR = ""
#compiled regex for threading, these compiled bits are thread safe
spacePorts = re.compile('( \d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):([0-9][0-9]*)')
removeHost = re.compile('(http|https)://.*:(80|443)')
fixTime = re.compile('([0-9]{4}-[0-9]{2}-[0-9]{2})T([0-9]{2}:[0-9]{2}:[0-9]{2})\.[0-9]*Z')

def readQueue():
	qconn = boto.sqs.connect_to_region("us-east-1", aws_access_key_id=QUEUE_AWS_ACCESS_KEY, aws_secret_access_key=QUEUE_AWS_SECRET_KEY)
	logProcQueue = qconn.get_queue(QUEUE_NAME)
	if logProcQueue is None:
		print ("No such Queue on SQS called %s with account %s" % (QUEUE_NAME, QUEUE_AWS_ACCESS_KEY))
		sys.exit(0)
	readMessage = logProcQueue.read()
	if readMessage is not None:
		print (readMessage.get_body())
		logProcQueue.delete_message(readMessage)

def download(src):
	if len(src) < 15:
		return
	src_s3conn = boto.connect_s3(SRC_AWS_ACCESS_KEY, SRC_AWS_SECRET_KEY)
	src_bucket = src_s3conn.get_bucket(SRC_PATH[:SRC_PATH.index('/')])
	srcFileKey = src_bucket.get_key(src)

	#TODO write out to s3 via gzip stream
	#dst_s3conn = boto.connect_s3(DST_AWS_ACCESS_KEY, DST_AWS_SECRET_KEY)
	#dst_bucket = dst_s3conn.get_bucket(DST_PATH[:DST_PATH.index('/')])
	#with open(outfilename, "w") as outfile:
	with smart_open.smart_open(srcFileKey) as srcStream:
			for line in srcStream:
				line = line.strip()
				if len(line) > 20:
					line = spacePorts.sub('\\1 \\2', line)
					line = removeHost.sub('', line)
					line = fixTime.sub('\\1 \\2', line)
					splt = len(shlex.split(line)) #lexical parse gives me tokens enclosed by quotes for url string
					if splt is 15:
						line = ("%s \"\" - -\n" % line)
					elif splt is 16:
						line = ("%s - -\n" % line)
					elif splt is 18:
						line = ("%s\n" % line)
					line = shlex.split(line)
#original log file format
#%time2 %elb %host %host_port %host_r %host_r_port %request_processing_time %backend_processing_time %response_processing_time %code %backend_status_code %received_bytes %bytesd %methodurl %uaquot %other %other
# new log file format
# %time2 %host %host_port %backend_processing_time %backend_status_code %bytesd %methodurl %uaquot %encryption_layer
#AWStats format
# %time2 %host %host_port %other %code %bytesd %methodurl %uaquot %other
				methodurl_stripped = line[14]
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
				print (finalLine)

def processDirectory(src):
	fileList = list()
	s3conn = boto.connect_s3(SRC_AWS_ACCESS_KEY, SRC_AWS_SECRET_KEY)
	bucket = s3conn.get_bucket(SRC_PATH[:SRC_PATH.index('/')])
	for remoteFile in bucket.list(prefix=src, delimiter='/'):
		if remoteFile.name[-3:] in "log":
			remoteFilePath = remoteFile.name.strip()
			fileList.append(remoteFilePath)

	#TODO remove max workers
	with concurrent.futures.ProcessPoolExecutor(max_workers=1) as executor:
		executor.map(download, fileList)


#Begin main code
s3conn = boto.connect_s3(SRC_AWS_ACCESS_KEY, SRC_AWS_SECRET_KEY)
bucket = s3conn.get_bucket(SRC_PATH[:SRC_PATH.index('/')])
for year in bucket.list(prefix=SRC_PATH[SRC_PATH.index('/')+1:], delimiter='/'): 
	yearint = year.name[-5:-1]
	for month in bucket.list(prefix=year.name, delimiter='/'):
		monthint = month.name[-3:-1]
		for day in bucket.list(prefix=month.name, delimiter='/'):
			dayint = day.name[-3:-1]
			srcdir = day.name
			DSTDIR = "%s/%s/%s" % (yearint, monthint, dayint)
			
			processDirectory(srcdir)
			#TODO start/stop control

			sys.exit(0)
