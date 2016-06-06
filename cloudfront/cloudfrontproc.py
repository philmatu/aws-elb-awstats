#!/usr/bin/python3
'''
***
S3 from cloudfront Stats Retrieval and Cleanup
This script uses gzipped s3 logs from aws cloudfront in format: 2016-05-24/E2WEIWNEZ6BS32.2016-05-24-01.328b329c.gz.log
***

Author: Philip Matuskiewicz - philip.matuskiewicz@nyct.com       

Changes:
	5/14/16 - Initial Script
	5/25/16 - Initial commit to repository
	6/6/16 - Timezone conversion
Acknowledgements:
	gzip - http://stackoverflow.com/questions/12571913/python-unzipping-stream-of-bytes
	s3 streaming - https://github.com/piskvorky/smart_open
	
'''

import os
import sys
import time
import boto
from boto.s3.key import Key
import smart_open
import shutil
import re
import datetime
import pytz
import shlex
import zlib
import urllib.parse
import configparser

CONFIG = configparser.ConfigParser()

if len(sys.argv) == 2:
	inputini = sys.argv[1];
	if inputini.endswith(".ini"):
		CONFIG.read(inputini)
	else:
		print ("usage: ./cloudfrontproc.py <configfile>")
		sys.exit(0)
else:
	print ("usage: ./cloudfrontproc.py <configfile>")
	sys.exit(0)

#pull the configuration values ahead of time
ROOTBUCKET = CONFIG.get('main', 'ROOTBUCKET')
PATH = CONFIG.get('main', 'PATH')
LOCAL_PYTZ_TIMEZONE = CONFIG.get('main', 'LOCAL_PYTZ_TIMEZONE')
AWS_ACCESS_KEY = CONFIG.get('main', 'AWS_ACCESS_KEY')
AWS_SECRET_KEY = CONFIG.get('main', 'AWS_SECRET_KEY')
DOMAIN = CONFIG.get('main', 'DOMAIN')
LOGRESOLV = CONFIG.get('main', 'LOGRESOLV')
PROCFILE = CONFIG.get('main', 'PROCFILE')

#static values
ET_TZ=pytz.timezone(LOCAL_PYTZ_TIMEZONE)
UTC_TZ = pytz.timezone('UTC')
def stream_gzip_decompress(stream):
	dec = zlib.decompressobj(32 + zlib.MAX_WBITS)  # offset 32 to skip the header
	for chunk in stream:
		data = dec.decompress(chunk)
		if data:
			yield data

#takes full line of log file in format (tab separated) 2016-06-06    00:07:05    JFK5    5072    70.214.107.212    GET... and converts the time into a local timezone as configured
#logresolvmerge will automatically merge this data together on the day of the year where there is a time change / overlap
def convertTimeToLocal(line):
	parts = line.split("\t", 2)
	nonspec_dt = datetime.datetime.strptime("%s %s" % (parts[0], parts[1]), "%Y-%m-%d %H:%M:%S")
	gmt_dt = nonspec_dt.replace(tzinfo=UTC_TZ)
	et_dt = gmt_dt.astimezone(ET_TZ)
	return ("%s %s" % (et_dt.strftime('%Y-%m-%d %H:%M:%S'), parts[2]))

def downloadFile(bucket, path, key):
	cont = 0
	ind = key.name.rindex('/') + 1
	thefilename = "%s/%s" % (path, key.name[ind:])
	k = bucket.get_key(key)
	outfilename = "%s.log" % thefilename

	with open(thefilename, "wb") as outfile:
		with smart_open.smart_open(k) as stream:
			for chunk in stream_gzip_decompress(stream):
				outfile.write(chunk)
	with open(outfilename, "w") as outfile:
		with open(thefilename, "r") as infile:
			for line in infile:
				cont = cont + 1	
				if cont > 2:
					line = line.strip()
					if line.startswith("#"):
						continue #ignore comments
					line = convertTimeToLocal(line)
					data = shlex.split(line)
					if len(data) < 12:
						continue
					for i in range(0, 11):
						if i is 10:
							url=urllib.parse.unquote(data[i])
							url=urllib.parse.unquote(url)
							outfile.write("\"%s\"" % url)
						else:
							outfile.write(data[i])
							outfile.write(" ")
					outfile.write("\n")
	os.remove(thefilename)
	print ("finished downloading and decompressing and cleaning file %s" % outfilename)

def download(bucket, path, keys):
	for key in keys:
		downloadFile(bucket, path, key)

def runStats(directory):
	command = "awstats -update -config=%s -LogFile=\"%s %s/*.log |\"" % (DOMAIN, LOGRESOLV, directory)
	os.system(command)

def updateLastPositionFile(newdatestamp):
	with open(PROCFILE, "w") as statusfile:
		statusfile.write(newdatestamp)

#Begin main code
START_PROCESS = ""
with open(PROCFILE, "r") as startfile:
	for line in startfile:
		if len(line) > 3:
			START_PROCESS = line.strip()
			break
processed = False
if len(START_PROCESS) < 2:
	print ("no starting point, Begining from the start")
	processed = True
else:
	print ("starting after we see %s" % START_PROCESS)

fixTime = re.compile('([0-9]{4}-[0-9]{2}-[0-9]{2})T([0-9]{2}:[0-9]{2}:[0-9]{2})\.[0-9]*Z')

s3conn = boto.connect_s3(AWS_ACCESS_KEY, AWS_SECRET_KEY)
bucket = s3conn.get_bucket(ROOTBUCKET)
currentdateprocessing = ""
datafiles = list()
bucketList = bucket.list(prefix=PATH, delimiter='/')
orderedList = sorted(bucketList, key=lambda k: k.last_modified)
for gzkey in orderedList:
	thedate = fixTime.sub("\\1", gzkey.last_modified)
	today = datetime.datetime.now().strftime('%Y-%m-%d')
	if (START_PROCESS in thedate) and (len(START_PROCESS) > 1):
		processed = True
	elif processed == True:
		if len(currentdateprocessing) < 2:
			print ("Pointer is empty, filling for first run for date %s" % thedate)
			currentdateprocessing = thedate
		if currentdateprocessing not in thedate:
			print ("Processing data files for date %s" % currentdateprocessing)
			os.mkdir(currentdateprocessing)
			download(bucket, currentdateprocessing, datafiles)
			runStats(currentdateprocessing)
			updateLastPositionFile(currentdateprocessing)
			shutil.rmtree(currentdateprocessing)
			if thedate in today:
				print ("Today's date reached, quitting due to log overlaps %s" % thedate)
				sys.exit(0)
			else:
				datafiles = list()
				currentdateprocessing = thedate
		datafiles.append(gzkey)
