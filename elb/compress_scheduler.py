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

Author: Philip Matuskiewicz - philip.matuskiewicz@nyct.com       

Changes:
	5/28/16 - Initial Logic / Script
	
'''

import sys
import boto
from boto.s3.key import Key
import configparser

CONFIG = configparser.ConfigParser()

if len(sys.argv) == 2:
        inputini = sys.argv[1];
        if inputini.endswith(".ini"):
                CONFIG.read(inputini)
        else:
                print ("usage: ./compress_scheduler.py <configfile>")
                sys.exit(0)
else:
        print ("usage: ./compress_scheduler.py <configfile>")
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
	
	#make sure that we can get the data file from the directory if it exists
	if dst_path_exists:
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
		status_file_text = status_file_key.get_contents_as_string()
		
		firstLine = True
		#create a list of all the files in the meta data file that it claims are completed
		completedFiles = list()
		if len(status_file_text) > 0:
			status_file_as_string = str(status_file_text)[2:-1]
			for line in status_file_as_string.split("\n"):
				if firstLine:
					firstLine = False
					if PROCESSING_STATUS_FILE_COMPLETE_TXT in line:
						print("The directory %s is completed already, exiting this directory")
						return
				completedFiles.append(line)
				#find any file in the meta data that isn't present in the source directory, warn on these
				#as it may be a corrupted meta data file
				if line not in dirlist: #file not in source
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
				if srcfilename in df:
					print ("Warn, <srcdir> file \"%s\" exists, but not marked completed, NOW deleting and add for reprocessing" % srcfilename)
					REMOVEFILE.append(srcfilename)
					ToBeProcessedFiles.append(srcfilename)
					
		#finally, find rogue files and warn about them (files in the dest directory, but on on meta data or source directory)
		for dstfilename in filesInDstDirectory:
			if PROCESSING_STATUS_FILE not in dstfilename:#except for the status file, which would be present
				if dstfilename.split("/")[-1] not in completedFiles:
				#the file isn't in the meta data or slated for completetion (from source directory)
					if dstfilename.split("/")[-1] not in ToBeProcessedFiles:
						print ("WARNING: The rogue file \"%s\" is present in the destination directory, you might want to delete this." % dstfilename)
		for remkey in REMOVEFILE:
			#TODO: ONLY DO THIS IF THERE ISN'T a LOCK FILE IN THE DIRECTORY
			print ("Deleting incomplete / unwanted file \"%s\"" % remkey)
			rogue_deletion_file_key = Key(destbucket, "%s/%s" % (dst_path,remkey))
			destbucket.delete_key(rogue_deletion_file_key)
	
	else:
		print ("The directory %s does not exist, creating task to process this directory" % dst_path)
		res = status_file_key.set_contents_from_string('')
		ToBeProcessedFiles = list()
		for srcfilename in dirlist:
			ToBeProcessedFiles.append(srcfilename)
	
	if len(ToBeProcessedFiles) > 0:
		#TODO pass on ToBeProcessedFiles to an external server for processing!
		print("Root directory: %s/" % dst_path)
		print(ToBeProcessedFiles)
			
			
		
	
	sys.exit(0)
	#for remoteFile in bucket.list(prefix=src, delimiter='/'):
	#	if remoteFile.name[-3:] in "log":
	#		remoteFilePath = remoteFile.name.strip()
	#		fileList.append(remoteFilePath)

#Begin main code
s3conn = boto.connect_s3(SRC_AWS_ACCESS_KEY, SRC_AWS_SECRET_KEY)
bucket = s3conn.get_bucket(SRC_PATH[:SRC_PATH.index('/')])
for year in bucket.list(prefix=SRC_PATH[SRC_PATH.index('/')+1:], delimiter='/'): 
	yearint = year.name[-5:-1]
	for month in bucket.list(prefix=year.name, delimiter='/'):
		monthint = month.name[-3:-1]
		for day in bucket.list(prefix=month.name, delimiter='/'):
			dirlist = list()
			dayint = day.name[-3:-1]
			srcdir = day.name
			dstdir = "%s/%s/%s" % (yearint, monthint, dayint)
			for fileWithPath in bucket.list(prefix=srcdir, delimiter='/'):
				fname = fileWithPath.name.split('/')[-1]
				dirlist.append(fname)
			processDirectory(dstdir, dirlist)
s3conn.close()

