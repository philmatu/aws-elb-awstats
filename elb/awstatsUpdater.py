#!/usr/bin/python3
'''
***
AWStats Updater - This script looks for the latest processed directory (via scripts here) on DST S3 Directory, pulls each day in chronological order, and updates AWStats with it


For back processing, this script takes a time range of directories to pull
For example, if you enter ./awstatsUpdater.py config.ini 201605, you'll process ONLY the month of May, 2016
For example, if you enter ./awstatsUpdater.py config.ini 20160502, you'll begin processing at May 2, 2016 (and proceed into the next month)

If there is a previous status file... this overrides all the above parameters, and it's assumed you aren't back processing.
Awstats only supports moving forward, not backwards, although it partitions data by month, you can use this to your advantage with this script on many machines (merging the data together)
awstats data is in /var/lib/awstats

***

Author: Philip Matuskiewicz - philip.matuskiewicz@nyct.com       

Changes:
	5/16/16 - Initial version for cloudfront of this script
	6/9/16 - Initial version of elb logs for this script

Acknowledgements:
	previous script acknowledgements
		
'''

import os
import sys
import boto
from boto.s3.key import Key
import smart_open
import shutil
import datetime
import zlib
import configparser

CONFIG = configparser.ConfigParser()
START_MONTH = False #YYYYMM
START_DAY = False #DD

if len(sys.argv) == 2 or len(sys.argv) == 3 or len(sys.argv) == 4:
	inputini = sys.argv[1];
	if inputini.endswith(".ini"):
		CONFIG.read(inputini)
	else:
		print ("usage: awstatsUpdater.py <configfile> [<YearAndMonthToProcessOverride_YYYYMM> <DayToProcessOverride_DD>]")
		sys.exit(0)
	
	if len(sys.argv) > 2:
		START_MONTH = str(sys.argv[2]).strip()
		if (len(START_MONTH) is not 6) or (not START_MONTH.isdigit()):
			print("Please enter the month/year to handle as YYYYMM (6 integers), you entered %s" % START_MONTH)
			sys.exit(0)
		if len(sys.argv) == 4:
			START_DAY = str(sys.argv[3]).strip()
			if (len(START_DAY) is not 2) or (not START_DAY.isdigit()):
				print("Please enter the day to handle as DD (2 integers), you entered %s" % START_DAY)
				sys.exit(0)
else:
	print ("usage: awstatsUpdater.py <configfile> [<YearAndMonthToProcessOverride_YYYYMM> <DayToProcessOverride_DD>]")
	sys.exit(0)

#pull the configuration values ahead of time
DST_PATH = CONFIG.get('main', 'DST_PATH')
DST_AWS_ACCESS_KEY = CONFIG.get('main', 'DST_AWS_ACCESS_KEY')
DST_AWS_SECRET_KEY = CONFIG.get('main', 'DST_AWS_SECRET_KEY')
PROCESSING_STATUS_FILE = CONFIG.get('main', 'PROCESSING_STATUS_FILE') # contains all files that are finished, contains DONE if all processing is done
PROCESSING_STATUS_FILE_COMPLETE_TXT = CONFIG.get('main', 'PROCESSING_STATUS_FILE_COMPLETE_TXT').strip()
PROCESSING_LOCK_FILE = CONFIG.get('main', 'PROCESSING_LOCK_FILE')
DOMAIN = CONFIG.get('awstats', 'DOMAIN')
LOGRESOLV = CONFIG.get('awstats', 'LOGRESOLV')
AWSTATS_LAST_ADDED_FILE = CONFIG.get('awstats', 'AWSTATS_LAST_ADDED_FILE')

def stream_gzip_decompress(stream):
	dec = zlib.decompressobj(32 + zlib.MAX_WBITS)  # offset 32 to skip the header
	for chunk in stream:
		data = dec.decompress(chunk)
		if data:
			yield data

def downloadFile(bucket, path, key):
	#tmpdata/ will be the first bit always, so remove it
	thefilename = "%s%s" % (path[path.index('/'):], key)
	k = bucket.get_key(thefilename)
	outfilename = "%s%s" % (path,key)
	
	#NOTE: download locally to machine, ensure enough disk space exists!
	with open(outfilename, "wb") as outfile:
		with smart_open.smart_open(k) as stream:
			#gzip uncompress / write, if you wish to manually 
			#for chunk in stream_gzip_decompress(stream):

			#just write compressed file
			for chunk in stream:
				outfile.write(chunk)
	print ("finished downloading the file %s" % outfilename)

def download(bucket, path, keys):
	out = list()
	for key in keys:
		downloadFile(bucket, path, key)

def runStats(directory):
	command = "awstats -update -config=%s -LogFile=\"%s %s*.gz |\"" % (DOMAIN, LOGRESOLV, directory)
	os.system(command)

def updateLastPositionFile(newdatestamp):
	with open(AWSTATS_LAST_ADDED_FILE, "w") as statusfile:
		statusfile.write(newdatestamp)

def isDirectoryReadyForProcessing(statusFilePath):
	dst_s3conn = boto.connect_s3(DST_AWS_ACCESS_KEY, DST_AWS_SECRET_KEY)
	dst_bucket = dst_s3conn.get_bucket(DST_PATH[:DST_PATH.index('/')])
	
	status_file_key = Key(dst_bucket, statusFilePath)
	status_file_text = bytes(status_file_key.get_contents_as_string()).decode(encoding='UTF-8')
	firstlinearr = status_file_text.split("\n", 2)

	dst_s3conn.close()

	if PROCESSING_STATUS_FILE_COMPLETE_TXT in firstlinearr[0]:
		return True
	return False

#Begin main code

START_PROCESS = False
if os.path.exists(AWSTATS_LAST_ADDED_FILE):
	with open(AWSTATS_LAST_ADDED_FILE, "r") as startfile:
		for line in startfile:
			if len(line) > 3:
				START_PROCESS = line.strip()
				print ("starting after we see %s" % START_PROCESS)
				break

sd = False
MONTH_ONLY = False
if (START_MONTH is not False) and (START_PROCESS is False):
	#handle commands only if START_PROCESS IS FALSE (nothing has been done)
	if START_DAY is not False:
		sd = "%s%s" % (START_MONTH,START_DAY)
		print("Handling command parameters for date, entry in YYYYMM DD is \"%s %s\", starting here"%(START_MONTH,START_DAY))
	else:
		sd = "%s01" % (START_MONTH)
		MONTH_ONLY = START_MONTH
		print("Handling command parameters for SINGLE MONTH, entry in YYYYMM is %s, starting here"%sd)
else:
	if START_PROCESS is not False:
		sd = START_PROCESS #start from the point left off at
		print("Starting ONE DAY after the day in YYYYMMDD %s" %sd)
	else:
		#start from the beginning
		print("Starting from the earliest date we see on S3")

#creating useful variables
today = datetime.datetime.now().strftime('%Y%m%d')
if sd is not False:
	startdate = datetime.datetime.strptime(sd, "%Y%m%d").date()
	enddate = datetime.datetime.strptime(today, "%Y%m%d").date()

work = {}

lastdateseen = None
s3conn = boto.connect_s3(DST_AWS_ACCESS_KEY,DST_AWS_SECRET_KEY)
bucket = s3conn.get_bucket(DST_PATH[:DST_PATH.index('/')])
bucketList = bucket.list(prefix=DST_PATH, delimiter='/')
for year in bucket.list(prefix=DST_PATH[DST_PATH.index('/')+1:], delimiter='/'):
	yearint = year.name[-5:-1]
	for month in bucket.list(prefix=year.name, delimiter='/'):
		monthint = month.name[-3:-1]
		for day in bucket.list(prefix=month.name, delimiter='/'):
			digitsum = sum(c.isdigit() for c in day.name)
			if digitsum != 8:
				#first directory... no files in it
				continue
			dayint = day.name[-3:-1]
			dirkey = "%s%s%s" % (yearint, monthint, dayint)
			monthkey = "%s%s" % (yearint, monthint)
			procdate = datetime.datetime.strptime(dirkey, "%Y%m%d").date()
			
			#don't process an incomplete day
			if dirkey in today:
				print("Found today's directory, going to skip")
				continue

			if MONTH_ONLY:
				if monthkey not in START_MONTH:
					continue #don't process anything outside of this month
			
			#get directory listing of files (except status file and lock file)
			files = list()
			allowed = False
			for fileWithPath in bucket.list(prefix=day.name, delimiter='/'):
				fname = fileWithPath.name.split('/')[-1]
				if PROCESSING_STATUS_FILE in fname:
					#check to make sure complete is there, otherwise block process write
					if isDirectoryReadyForProcessing(fileWithPath):
						allowed = True
						continue
				if PROCESSING_LOCK_FILE in fname:
					print("WARNING, a lock file is present in directory YYYYMMDD %s"%dirkey)
					allowed = False #if a directory is locked, don't allow awstats processing of it!
					break #get out of the for loop looking at files in this directory
				files.append(fname)
			
			if not allowed:
				if lastdateseen is None:
					lastdateseen = procdate
				else:
					dtseen = list()
					dtseen.append(lastdateseen)
					dtseen.append(procdate)
					lastdateseen = min(dtseen)
				continue #next day

			items = {"path":day.name,"files":files}

			if sd is False:
				work[procdate] = items
			
			#if there is a start date specified somewhere, make sure to adhere to it -> now
			if MONTH_ONLY:
				if startdate <= procdate <= enddate:
					work[procdate] = items
			else:
				if startdate < procdate <= enddate:
					work[procdate] = items

mypythonscript = os.path.realpath(__file__)
mypath = mypythonscript[:mypythonscript.rindex('/')]

#sort the dictionary of work items
for key in sorted(work):
	if lastdateseen is not None:
		if not key < lastdateseen:
			print("Last Date seen is %s, and our current proc date is %s... skipping processing" % (lastdateseen, key))
			continue
	data = work[key]
	print("Processing for Date: %s, Directory: %s" % (key, data['path']))
	direc = "tmpdata/%s"%data['path']
	gziplogpath = "%s/%s" % (mypath,direc)
	
	os.makedirs(direc)
	download(bucket, direc, data['files'])
	runStats(gziplogpath)
	updateLastPositionFile(key.strftime('%Y%m%d'))#string in format YYYYMMDD
	shutil.rmtree(direc[:direc.index('/')])

s3conn.close()
