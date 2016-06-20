#!/usr/bin/python3
'''
***
ELB Log Compressor script
Takes logs from a given directory and places a cleaned gzip of them in another s3 directory (which will also be compatible with awstats)
Input Logs are fed in through Amazon SQS via the scheduler, this works across AWS Accounts!
This is meant to be run on Spot Instances, meaning reduced cost to process huge amounts of data logs
config.ini WORKER_EXIT_AFTER_DONE can be 1 for true, or 0 for false to exit or not after an empty queue
***

#TODO: in the future, we need to catch the shutdown signal too from EC2 incase of accidental deletion

Author: Philip Matuskiewicz - philip.matuskiewicz@nyct.com       

Changes:
	5/14/16 - Initial Script
	5/28/16 - Configuration added, Parameters to compress individually added (external coordinator)
	5/30/16 - Incorporated SQS for spot instance use, not ready to use yet
	5/31/16 - Added compression support / upload to s3 support / lock/status file updates (for parts)
	6/2/16 - Finalized Script
	6/3/16 - added signal killing abilities
	6/6/16 - Added timezone conversion and many bugfixes
	6/9/16 - Added web directory updates
'''

import traceback
import syslog
import sys
import os
import signal
from io import BytesIO
import boto
import boto.sqs
from boto.sqs.message import RawMessage
from boto.s3.key import Key
import smart_open
import threading
import concurrent.futures
import json
import iso8601
import pytz
import time
import shlex
import re
import socket
from urllib.parse import urlencode, urlparse, parse_qsl, urlunparse
import urllib.request
import configparser
import gzip

SPOT_KILL = False
WRITE_LOCK = threading.Lock()
CONFIG = configparser.ConfigParser()
DATE_TO_PROCESS = False
if len(sys.argv) == 2 or len(sys.argv) == 3:
	inputini = sys.argv[1];
	if inputini.endswith(".ini"):
		CONFIG.read(inputini)
	else:
		print ("usage: ./elb_compress.py <configfile> [<date_to_handle_in_MMDDYYYY>]")
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
	print ("usage: ./elb_compress.py <configfile> [<date_to_handle_in_MMDDYYYY>]")
	sys.exit(0)

#Load configuration from ini file
LOCAL_PYTZ_TIMEZONE = CONFIG.get('main', 'LOCAL_PYTZ_TIMEZONE')
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
AWS_SPOT_CHECK_SLEEP_INTERVAL_SECONDS = int(CONFIG.get('main', 'AWS_SPOT_CHECK_SLEEP_INTERVAL_SECONDS'))
WORKER_EXIT_AFTER_DONE = CONFIG.get('main', 'WORKER_EXIT_AFTER_DONE')
WORKER_WEB_ROOT = CONFIG.get('main', 'WORKER_WEB_ROOT')
WORKER_STATUS_FILE_NAME = CONFIG.get('main', 'WORKER_STATUS_FILE_NAME')
#this script does not process anything from the current day due to added logic to keep track of hourly file dumps

#compiled regex for threading, these compiled bits are thread safe
spacePorts = re.compile('( \d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):([0-9][0-9]*)')
removeHost = re.compile('(http|https)://.*:(80|443)')
ET_TZ=pytz.timezone(LOCAL_PYTZ_TIMEZONE)

#global constant variables that we don't need to configure
EXECUTOR = None
FUTURES = None
DIRECTORY = ""#what comes after both SRC_PATH and DST_PATH, folder wise, received via Queue
CHUNK_SIZE = 8192 #compression block size
AWS_META_INSTANCEID_URL = "http://169.254.169.254/latest/meta-data/instance-id"
AWS_META_SPOTTERMINATIONTIME_URL = "http://169.254.169.254/latest/meta-data/spot/termination-time" #filled in with time if spot instance is set to terminate

def handle_SIGINT_THREADS(signum, frame):
	print("Ending my thread now")
	os.system('kill $PPID')

def handle_SIGINT_MAIN(signum, frame):
	if len(DIRECTORY) > 0:
		print("Caught the kill signal from ctrl c, Publishing directory \"%s\" to incomplete queue (unless manual mode)" % DIRECTORY)
		enQueueNonCompletedDirectory(DIRECTORY)
		releaseLock(DIRECTORY)
	if SPOT_KILL:
		os.system('shutdown -h now')
	os.system('kill $PPID')

def setLocalWebStatusFileText(data):
	#creates the file if it doesn't exist, updates it otherwise
	path = "%s%s" % (WORKER_WEB_ROOT,WORKER_STATUS_FILE_NAME) 
	with open(path, "w") as text_file:
		text_file.write(data)
	print("Wrote out the data \"%s\" to local web status file at \"%s\"" % (data,path))

def createLock(filePath):
	dst_s3conn = boto.connect_s3(DST_AWS_ACCESS_KEY, DST_AWS_SECRET_KEY)
	dst_bucket = dst_s3conn.get_bucket(DST_PATH[:DST_PATH.index('/')])
	lock_file_path = "%s%s%s" % (DST_PATH[DST_PATH.index('/'):],filePath,PROCESSING_LOCK_FILE)
	lock_file_key = Key(dst_bucket, lock_file_path)
	if not lock_file_key.exists():
		resource = urllib.request.urlopen(AWS_META_INSTANCEID_URL)
		instanceid = resource.read().decode('utf-8')
		lock_file_key.set_contents_from_string(instanceid)
		setLocalWebStatusFileText(filePath)
		print("The lock is now acquired to begin processing on %s" % lock_file_path)
		dst_s3conn.close()
		return True
	else:
		instanceid = bytes(lock_file_key.get_contents_as_string()).decode(encoding='UTF-8')
		print("The lock file exists, the instance running is %s" % instanceid)
	dst_s3conn.close()
	return False

def releaseLock(filePath):
	setLocalWebStatusFileText("")
	dst_s3conn = boto.connect_s3(DST_AWS_ACCESS_KEY, DST_AWS_SECRET_KEY)
	dst_bucket = dst_s3conn.get_bucket(DST_PATH[:DST_PATH.index('/')])
	lock_file_path = "%s%s%s" % (DST_PATH[DST_PATH.index('/'):],filePath,PROCESSING_LOCK_FILE)
	lock_file_key = Key(dst_bucket, lock_file_path)
	if not lock_file_key.exists():
		print("There was no lock... hoping nothing went wrong!  You may want to verify this.")
		dst_s3conn.close()
		return False
	else:
		instanceid = bytes(lock_file_key.get_contents_as_string()).decode(encoding='UTF-8')
		resource = urllib.request.urlopen(AWS_META_INSTANCEID_URL)
		myinstanceid = resource.read().decode('utf-8')
		if myinstanceid not in instanceid:
			print("WARNING: the instance id's don't match, mine is %s, the lock one is %s" %(myinstanceid,instanceid))
		dst_bucket.delete_key(lock_file_key)
		print("The lock file has been removed, releasing for future processing")
		dst_s3conn.close()
		return True
	return False #shouldn't get here
def directoryAlreadyCompleted(path):
	dst_s3conn = boto.connect_s3(DST_AWS_ACCESS_KEY, DST_AWS_SECRET_KEY)
	dst_bucket = dst_s3conn.get_bucket(DST_PATH[:DST_PATH.index('/')])
	status_file_path = "%s%s%s" % (DST_PATH[DST_PATH.index('/'):],path,PROCESSING_STATUS_FILE)
	status_file_key = Key(dst_bucket, status_file_path)
	if not status_file_key.exists():
		return False
	else:
		status_file_text = bytes(status_file_key.get_contents_as_string()).decode(encoding='UTF-8')
		if PROCESSING_STATUS_FILE_COMPLETE_TXT in status_file_text:
			return True
	return False

def isAlreadyInStatusFile(completedFile):
	dst_s3conn = boto.connect_s3(DST_AWS_ACCESS_KEY, DST_AWS_SECRET_KEY)
	dst_bucket = dst_s3conn.get_bucket(DST_PATH[:DST_PATH.index('/')])
	status_file_path = "%s/%s" % (completedFile.rsplit('/', 1)[0],PROCESSING_STATUS_FILE)
	status_file_key = Key(dst_bucket, status_file_path)
	theCompletedFile = completedFile.rsplit('/', 1)[1]
	if not status_file_key.exists():
		status_file_key.set_contents_from_string('')
		return False
	else:
		status_file_text = bytes(status_file_key.get_contents_as_string()).decode(encoding='UTF-8')
		if theCompletedFile in status_file_text:
			return True
	return False

def getDirectoryList(key,sec,inpath,d=None):
	out = {}
	s3conn = boto.connect_s3(key, sec)
	bucket = s3conn.get_bucket(inpath[:inpath.index('/')])
	for year in bucket.list(prefix=inpath[inpath.index('/')+1:], delimiter='/'):
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
					#first directory... no files in it
					continue
				dirlist = list()
				dayint = day.name[-3:-1]
				srcdir = day.name
				dstdir = "%s/%s/%s/" % (yearint, monthint, dayint)
				if d is not None:
					if d not in dstdir:
						continue
				tmp = list()
				for fileWithPath in bucket.list(prefix=srcdir, delimiter='/'):
					fname = fileWithPath.name.split('/')[-1]
					tmp.append(fname)
				out[dstdir] = tmp
	s3conn.close()
	return out

#the completedFile doesn't have a .gz, even though we know all files here should end in GZ, this is handled by the scheduler
def validateStatusFile(completedFile, completionListVerify=None):
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
		#make sure that everything we were going to complete is in the status file
		status_file_text = bytes(status_file_key.get_contents_as_string()).decode(encoding='UTF-8')
		for line in completionListVerify:
			if len(line) < 1:
				print("Empty task, ignoring")
				continue
			if line not in status_file_text.split("\n"):
				print("The task \"%s\" isn't in the status file, this means I didn't complete successfully... I will notify the scheduler queue of my directory: %s" % (line,completedFile))
				enQueueNonCompletedDirectory(DIRECTORY)#string in YYYY/MM/DD from the original queue
				return
		#make sure every line of the status file has a file in the directory
		dldir = completedFile.split('/', 1)[1]
		awss3dirlist = getDirectoryList(key=DST_AWS_ACCESS_KEY,sec=DST_AWS_SECRET_KEY,inpath=DST_PATH,d=dldir)
		for line in status_file_text.split("\n"):
			if len(line) < 1:
				print("Warn: blank line in status file detected... this isn't normal but can be handled gracefully")
				continue#ignore blank lines, although this is abnormal
			tmpnotin = False
			for item in awss3dirlist[dldir]:
				if line in item:
					tmpnotin = True
					break
			if not tmpnotin:
				if PROCESSING_STATUS_FILE_COMPLETE_TXT in line:
					#if we manage to get this far with a complete status file, don't rewrite the data into it
					print("The directory \"%s\" was already completed, not sure how we got here, but it's ok, I'll end processing now."%completedFile)
					return
				print("The line \"%s\" was not found in the list of tasks, this means I didn't complete successfully... I will notify the scheduler queue of my directory: %s" % (line,completedFile))
				enQueueNonCompletedDirectory(DIRECTORY)#string in YYYY/MM/DD from the original queue
				return
		print ("All files that were queued by scheduler are finished and in status file, appending completion text now for the next pipeline step, directory is %s" % completedFile)
		new_status_file_text = "%s\n%s" % (PROCESSING_STATUS_FILE_COMPLETE_TXT,status_file_text)
		status_file_key.set_contents_from_string(new_status_file_text)
	dst_s3conn.close()
		
def updateStatusFile(completedFile):
	dst_s3conn = boto.connect_s3(DST_AWS_ACCESS_KEY, DST_AWS_SECRET_KEY)
	dst_bucket = dst_s3conn.get_bucket(DST_PATH[:DST_PATH.index('/')])
	status_file_path = "%s/%s" % (completedFile.rsplit('/', 1)[0],PROCESSING_STATUS_FILE)
	status_file_key = Key(dst_bucket, status_file_path)
	theCompletedFile = completedFile.rsplit('/', 1)[1].strip()

	try:	
		if not status_file_key.exists():
			if len(completionListVerify) > 0:
				print("Seeking Verification for %s, but this directory has no status file... rerun scheduler to delete / restart processing please.  I'll end now on this directory" % completedFile)
				return
			print ("WARN: failed to retrieve file \"%s\", starting new key." % status_file_path)
			status_file_key.set_contents_from_string(theCompletedFile)
		else:
			status_file_text = bytes(status_file_key.get_contents_as_string()).decode(encoding='UTF-8')
			#everything has been done if we hvae a completionList object, so validate this
			if len(status_file_text) < 3:
				new_status_file_text = theCompletedFile
			else:
				new_status_file_text = "%s\n%s" % (status_file_text, theCompletedFile)
			status_file_key.set_contents_from_string(new_status_file_text)
		print("Updated Status file with latest data, file %s" % theCompletedFile)
	except:
		print("Exception trying to update Status file with dir \"%s\", trying again" % completedFile)
		updateStatusFile(completedFile)
	#validate status file contents
	status_file_text = bytes(status_file_key.get_contents_as_string()).decode(encoding='UTF-8')
	if theCompletedFile in status_file_text:
		return
	else:
		print("The file \"%s\" didn't get outputted to the status file, trying again")
		updateStatusFile(completedFile)
	dst_s3conn.close()

def compress(src,is_retry=False): #takes in a filename that is in the SRCPATH directory and places a compressed/gzipped version into DSTPATH
	#create signal for subprocess
	signal.signal(signal.SIGINT, handle_SIGINT_THREADS)
	if len(src) < 15:
		return False
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
		src_s3conn.close()
		dst_s3conn.close()
		print("The file %s is already in the status file, meaning it should be done... skipping" % dst_path_sans_GZ)
		return False

	print("Compressing the file %s" % src)

	buf = "" #buffer to hold onto chunks of data at a time
	part = 1
	outStream = BytesIO()
	compressor = gzip.GzipFile(fileobj=outStream, mode='wb')
	logcount = 0
	try:
		with smart_open.smart_open(srcFileKey) as srcStream:
			for line in srcStream:
				#simply  delete any bad characters, could be utf16-le, but we don't really need to save these characters
				line = bytes(line).decode('utf-8', errors='ignore')
				try:
					cleanedString = clean(line)
				except: 
					traceback.print_exc()
					syslog.syslog(syslog.LOG_ERR, "AWSTATSPARSE EXCEPTION THROWN with line %s, file %s"%(line,srcFileKey))
					print("PARSE EXCEPTION THROWN with line %s in file %s"%(line,srcFileKey))
					os.system('kill $PPID')
				logcount = logcount + 1
				if logcount > 10000:
					print ("10000 more Log entries processed...")
					logcount = 0
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
			print("Finishing processing of log %s" % srcFileKey)
			block = bytes(buf,'utf_8')
			compressor.write(block)
			compressor.close()
			outStream.seek(0)
			mpu.upload_part_from_file(outStream, part)
			outStream.seek(0)
			outStream.truncate()
			mpu.complete_upload()
		src_s3conn.close()
		dst_s3conn.close()
		return dst_path_sans_GZ
	except:
		traceback.print_exc()
		print("The smart_open library failed to open the file / maintain a connection")
		#socket timed out or another read error of some type
		if is_retry:
			print("Already retried, giving up with %s"%src)
			return False
		return compress(src,is_retry=True)		

def isIPV6(addr):
	try:
		socket.inet_pton(socket.AF_INET6, addr)
		return True
	except socket.error:
		return False

def splitIPV6Ports(line):
	#after ipv4 split... try for ipv6
	#split ipv6 addresses, this could show up in the 5/4/3 array position only, these will have a final : with port if they are an IP
	#Acknowledgement from: http://stackoverflow.com/questions/319279/how-to-validate-ip-address-in-python
	try:
		parts = shlex.split(line) #lexical parse gives me tokens enclosed by quotes for url string as awstats sees them
	except:
		return ""
	if ":" in parts[5]:
		subparts = parts[5].rsplit(":", 1)
		if isIPV6(subparts[0]) and (len(subparts) > 1):
			return line.replace(parts[5], "%s %s" % (subparts[0], subparts[1]))
	elif ":" in parts[4]:
		subparts = parts[4].rsplit(":", 1)
		if isIPV6(subparts[0]) and (len(subparts) > 1):
			return line.replace(parts[4], "%s %s" % (subparts[0], subparts[1]))
	elif ":" in parts[3]:
		subparts = parts[3].rsplit(":", 1)
		if isIPV6(subparts[0]) and (len(subparts) > 1):
			return line.replace(parts[3], "%s %s" % (subparts[0], subparts[1]))
	return line

#takes full line of log file in format 2016-05-12T21:48:58.253468Z appelb-pr-ElasticL-3M29U6FNWKZ7... and converts the time into a local timezone as configured
#logresolvmerge will automatically merge this data together on the day of the year where there is a time change / overlap
def convertTimeToLocal(line):
	parts = line.split(" ", 1)
	try:
		gmt_dt = iso8601.parse_date(parts[0])
	except:
		return ""
	et_dt = gmt_dt.astimezone(ET_TZ)
	return ("%s %s" % (et_dt.strftime('%Y-%m-%d %H:%M:%S'), parts[1]))

#caused by: 2016-06-03 06:45:07 appelb-pr-ElasticL-3M29U6FNWKZ7 94.103.129.51 34835 10.137.122.228 80 0.000021 0.000898 0.000021 302 302 0 341 "GET http://example.com:80/m/index%3Bjsessionid=965DE6793829DC313C?q=302996'" HTTP/1.1" "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; generic_01_01; YPC 3.2.0; .NET CLR 1.1.4322; yplus 5.3.04b)" - -
def cleanURLString(line):
	#cleans quotes (" and ') from the URL string that users can enter
	ind_st = line.find("\"GET") +1#remove quote
	ind_end = line.rfind("HTTP/1.1\"") -1#remove quote
	data = line[ind_st:ind_end]
	data = data.replace("\"","")
	data = data.replace("\'","")
	line = "%s%s%s" % (line[:ind_st],data,line[ind_end:])
	return line

#NOTE: This is highly specific to PhilMatu's needs, you may want to bypass this with returning qs_parts immediately (or customize to your needs)
def customURLClean(qs_parts):
	#return qs_parts #do nothing
	latlonstore = None
	lstore = None #handles mobile location input
	for Key in qs_parts:
		cskey = Key.lower()
		if (cskey in "lineref") or (cskey in "monitoringref"):
			parts = qs_parts[Key].split('_')
			qs_parts[Key] = parts[len(parts)-1]
		if (cskey in "lat" and "lat" in cskey) or (cskey in "lon" and "lon" in cskey):
			if latlonstore is None:
				latlonstore = Key
			else:
				if ("lat" in cskey):
					lat = qs_parts[Key]
					lon = qs_parts[latlonstore]
				else:
					lon = qs_parts[Key]
					lat = qs_parts[latlonstore]
				latlonstore = "%s_%s"%(lat,lon)
		if (cskey in "l") and ("l" in cskey):
			lstore = qs_parts[Key].replace(",", "_")
		
	if latlonstore is not None:
		qs_parts["CustAWStatsLocation"] = latlonstore
	elif lstore is not None:	
		qs_parts["CustAWStatsLocation"] = lstore
		
	return qs_parts

def clean(line):
	line = line.strip()
	if len(line) < 20:
		return ""
	if line.startswith("#"):
		return "" #ignore comments
	if len(line.split(" ")) < 8:
		return "" #if there aren't many spaces, this is likely a malformed URL
	line = cleanURLString(line)
	line = convertTimeToLocal(line)
	if len(line) < 20:
		return ""
	line = spacePorts.sub('\\1 \\2', line)
	line = splitIPV6Ports(line)
	if len(line) < 20:
		return ""#bad string format
	line = removeHost.sub('', line)
	
	#we are missing a backend processing time, since a 504 fails, so replace this on the lines where we have a 504, eliminating many errors
	line = line.replace("-1 -1 -1 504 0 0 0", "-1 -1 -1 -1 504 0 0 0")
	line = line.replace("-1 -1 -1 502 0 0 0", "-1 -1 -1 -1 502 0 0 0")

	#this method has a try/catch in splitIPV6Ports incase of " or ' characters in the URL string which would cause issues...	
	parts = shlex.split(line) #lexical parse gives me tokens enclosed by quotes for url string as awstats sees them
	splt = len(parts) #lexical parse gives me tokens enclosed by quotes for url string
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
		qs_parts = customURLClean(qs_parts)
		qs[4] = urlencode(qs_parts)
		new_method = urlunparse(qs)
		methodurl_stripped = "%s %s %s" % (url_parts[0], new_method.replace("%2C",",").replace("%2c",",").replace("%252C", ",").replace("%252c", ","), url_parts[2])
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
	if DATE_TO_PROCESS is not False:
		return #don't queue when running in manual mode
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
	print("Enqueing Directory (YYYY/MM/DD) %s for re-scheduling and re-processing due to incomplete processing with me" % data_out['directory'])
	logProcQueue.write(queuemessage)

def checkForTerminationThread():
	while 1:
		try:
			spotdataresource = urllib.request.urlopen(AWS_META_SPOTTERMINATIONTIME_URL)
		except urllib.error.HTTPError as e:
			# Return code error (e.g. 404, 501, ...) as e.code
			if e.code != 404:
				print("There was an error with the request, but it wasn't a 404, but rather it was \"%s\"... continuing processing" % e.code)
		except urllib.error.URLError as e:
			#not an http error
			print('URLError of some type getting spot instance termination time, error will be printed now and then more processing will happen')
			print(e)
		else:
			the_termination_time = spotdataresource.read().decode('utf-8')
			print("The spot instance will terminate at %s... starting shutdown" % the_termination_time)
			SPOT_KILL = True
			os.system('kill $PPID')
		time.sleep(AWS_SPOT_CHECK_SLEEP_INTERVAL_SECONDS)

#start main thread

#specific date processing support
signal.signal(signal.SIGINT, handle_SIGINT_MAIN)

spot_term_thread = threading.Thread(target = checkForTerminationThread)
spot_term_thread.start()

manual_dirlist = list()
matchdir = False
if DATE_TO_PROCESS is not False:
	#MMDDYYYY
	m = DATE_TO_PROCESS[:2]
	d = DATE_TO_PROCESS[2:4]
	y = DATE_TO_PROCESS[4:]
	matchdir = "%s/%s/%s/" % (y,m,d)
	dlist = getDirectoryList(key=SRC_AWS_ACCESS_KEY,sec=SRC_AWS_SECRET_KEY,inpath=SRC_PATH,d=matchdir)
	for key in dlist:
		if matchdir in key:
			manual_dirlist = list(dlist[key])

setLocalWebStatusFileText("")
#queue processing mode (if a date isn't set)
while True:
	if (len(manual_dirlist) > 0) and (matchdir is not False):
		print ("Manual directory provided, Queue disabled")
		DIRECTORY = matchdir
		tasks = list(manual_dirlist)
	else:
		print("Queue Mode Enabled")
		count = 0
		message = readQueue()
		if message is None:
			if "1" in WORKER_EXIT_AFTER_DONE:
				print("No data in queue, exiting (config of WORKER_EXIT_AFTER_DONE = 1)")
				os.system('kill $PPID')
			print("No data in queue, waiting 30 seconds and trying again")
			time.sleep(30) #5 second sleep, 25 second total wait from queue before we consider all tasks done for the day		
			continue
		count = 0
		data = json.loads(message)
		DIRECTORY = data['directory'] #appended to src and dst path from configuration file
		
		if ("too_long" in data['tasklist']) and (data['tasklist'].strip() in "too_long"):
			#do manual lookup of tasks because sqs limit is 256KB and my text is too long... 
			#most this does is adds relookup of all files to ensure they're already complete (a few extra get requests, basically)
			tasks = list()
			dlist = getDirectoryList(key=SRC_AWS_ACCESS_KEY,sec=SRC_AWS_SECRET_KEY,inpath=SRC_PATH,d=data['directory'])
			for key in dlist:
				for item in dlist[key]:
					tasks.append(item)
			print("Queued up %d items for processing on directory %s manually, the scheduler said there was too much data to pass via queue (normal for big sites)" % (len(tasks),data['directory']))
		else:
			tasks = data['tasklist']
	
	if createLock(DIRECTORY):
		if not directoryAlreadyCompleted(DIRECTORY):
			#sequential order (errors will print clearly)
			#for task in tasks:
			#	compress(task)
			#threaded mode
			with concurrent.futures.ProcessPoolExecutor() as executor:
				results = executor.map(compress, tasks)
				for value in results:
					if value is False:
						print("Compress returned False, it's either already complete or a blank file")
						continue
					with WRITE_LOCK:
						print("Updating the status file now in the parent process with %s"%value)
						updateStatusFile(value)
			with WRITE_LOCK:
				src_path = "%s%s" % (DST_PATH.split("/", 1)[1], DIRECTORY)
				validateStatusFile(src_path, tasks) #completion is here if all checks out... we assume that the scheduler has all the tasks for a day in this list
		else:
			print("The directory \"%s\" is already completed (marked in status file)" % DIRECTORY)
		releaseLock(DIRECTORY)
	else:
		print("Exiting without doing work, couldn't acquire a lock for processing the date associated with %s." % tasks[0]);
	DIRECTORY = ""
	if (len(manual_dirlist) > 0) and (matchdir is not False):
		print("Manual Directory \"%s\" has been processed, exiting now." % matchdir)
		os.system('kill $PPID')
