#!/usr/bin/python
'''
***
ELB Log Compressor script to be run on spot instances (NOT FINISHED YET)
Takes logs from a given directory and places a cleaned gzip of them in another s3 directory (which will also be compatible with awstats)
Works across accounts also!
***

Author: Philip Matuskiewicz - philip.matuskiewicz@nyct.com       

Changes:
	5/14/16 - Initial Script

	
'''

import concurrent.futures
import os
import sys
import time
import signal
import atexit
from boto.s3.key import Key
import smart_open
import boto
import threading
import shutil
from urlparse import urlparse, parse_qsl, urlunparse
from urllib import urlencode
import datetime
import shlex
import re

#CONFIG
SRC_PATH = "example-bucket/data/"
SRC_AWS_ACCESS_KEY = ""
SRC_AWS_SECRET_KEY = ""

DST_PATH = "example-bucket/data-processed/"
DST_AWS_ACCESS_KEY = ""
DST_AWS_SECRET_KEY = ""

REMOVE_QUERY_STRING_KEYS = ["callback"]
#ENDCONFIG

#compiled regex for threading, these compiled bits are thread safe
spacePorts = re.compile('( \d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):([0-9][0-9]*)')
removeHost = re.compile('(http|https)://.*:(80|443)')
fixTime = re.compile('([0-9]{4}-[0-9]{2}-[0-9]{2})T([0-9]{2}:[0-9]{2}:[0-9]{2})\.[0-9]*Z')
DSTDIR = ""

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
				print finalLine

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
