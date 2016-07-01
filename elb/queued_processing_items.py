#!/usr/bin/python3
'''
***
Waiting for Log Processing Queue Items Lister... this just lists items that are waiting on a worker to pick up
***

Author: Philip Matuskiewicz - philip.matuskiewicz@nyct.com       

Changes:
	6/9/16 - Finalized Script
'''

import sys
import boto
import boto.sqs
from boto.sqs.message import RawMessage
import configparser
import json

CONFIG = configparser.ConfigParser()

if len(sys.argv) == 2:
	inputini = sys.argv[1];
	if inputini.endswith(".ini"):
		CONFIG.read(inputini)
	else:
		print ("usage: ./queue_processing_items.py <configfile>")
		sys.exit(0)
else:
	print ("usage: ./queue_processing_items.py <configfile>")
	sys.exit(0)

#Load configuration from ini file
QUEUE_NAME = CONFIG.get('main', 'QUEUE_NAME')
QUEUE_AWS_ACCESS_KEY = CONFIG.get('main', 'QUEUE_AWS_ACCESS_KEY')
QUEUE_AWS_SECRET_KEY = CONFIG.get('main', 'QUEUE_AWS_SECRET_KEY')

def readProcessingQueueReadOnly():
	qconn = boto.sqs.connect_to_region("us-east-1", aws_access_key_id=QUEUE_AWS_ACCESS_KEY, aws_secret_access_key=QUEUE_AWS_SECRET_KEY)
	logProcQueue = qconn.get_queue(QUEUE_NAME)
	if logProcQueue is None:
		print("Checked the processing queue %s, nothing was there", QUEUE_NAME)
		return list()
	out = set()#ensures unique values
	#NOTE: calling this will cause all messages to disappear for about 30 seconds, workers cycle, so they'll pick up a minute or so later
	messages = logProcQueue.get_messages(wait_time_seconds=2, visibility_timeout=30, num_messages=10)
	while len(messages) > 0:
		for message in messages:
			raw_json = message.get_body()
			data = json.loads(raw_json)
			if len(data['directory']) > 0:
				out.add(data['directory'][:-1])#remove final / which is in directory
		messages = logProcQueue.get_messages(wait_time_seconds=2, visibility_timeout=30, num_messages=10)#continue reading
	qconn.close()
	return out

#Begin main code
QUEUED_LIST = readProcessingQueueReadOnly()
print("The following tasks are awaiting a worker to pick up...")
for task in QUEUED_LIST:
	print(task)
