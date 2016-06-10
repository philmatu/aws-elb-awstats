#!/usr/bin/python3
'''
***
Incomplete Queue Items Lister... this just lists the incomplete items that were sent to a worker but didn't finish
***

Author: Philip Matuskiewicz - philip.matuskiewicz@nyct.com       

Changes:
	6/2/16 - Finalized Script
'''

import sys
import boto
import boto.sqs
from boto.sqs.message import Message
import configparser
import json

CONFIG = configparser.ConfigParser()

if len(sys.argv) == 2:
	inputini = sys.argv[1];
	if inputini.endswith(".ini"):
		CONFIG.read(inputini)
	else:
		print ("usage: ./incomplete_queued_items.py <configfile>")
		sys.exit(0)
else:
	print ("usage: ./incomplete_queued_items.py <configfile>")
	sys.exit(0)

#Load configuration from ini file
INCOMPLETE_TASKS_QUEUE_NAME = CONFIG.get('main', 'INCOMPLETE_TASKS_QUEUE_NAME')
QUEUE_AWS_ACCESS_KEY = CONFIG.get('main', 'QUEUE_AWS_ACCESS_KEY')
QUEUE_AWS_SECRET_KEY = CONFIG.get('main', 'QUEUE_AWS_SECRET_KEY')

def readIncompleteQueue():
	qconn = boto.sqs.connect_to_region("us-east-1", aws_access_key_id=QUEUE_AWS_ACCESS_KEY, aws_secret_access_key=QUEUE_AWS_SECRET_KEY)
	logProcQueue = qconn.get_queue(INCOMPLETE_TASKS_QUEUE_NAME)
	if logProcQueue is None:
		print("Checked the incomplete queue %s, nothing was there")
		return list()
	out = set()#ensures unique values
	messages = logProcQueue.get_messages(visibility_timeout=30, wait_time_seconds=2, num_messages=10)
	while len(messages) > 0:
		for message in messages:
			raw_json = message.get_body()
			data = json.loads(raw_json)
			if len(data['directory']) > 0:
				out.add(data['directory'][:-1])#remove final / which is in directory
		messages = logProcQueue.get_messages(visibility_timeout=30, wait_time_seconds=2, num_messages=10)#continue reading
	qconn.close()
	return out

#Begin main code
INCOMPLETE_LIST = readIncompleteQueue()
print("The following tasks are queued up...")
for task in INCOMPLETE_LIST:
	print(task)
