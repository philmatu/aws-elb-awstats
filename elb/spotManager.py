#!/usr/bin/python3
'''
***
Spot Instance Manager

./spotManager.py <configfile> <command> [<cmd1> <cmd2>]

Commands:
ListAll - List all instances and locks, note if instance is active, note if instance has a job running
ListOrphanedInstances - List all instances that are running but have no jobs attached to them (if cmd1="delete", listed instances are deleted)
ListMismatchedLocks - List all locks that mismatch with what the instance says it's doing (Lock.pid != Instance Description URL)
ListOrphanedLocks - List all locks that don't have an associated instance that is running (if cmd1="delete", listed instances are deleted)
DeleteLock - Deletes the lock file for a given directory (cmd1=MMDDYYYY as 8 digit integer)
DeleteInstance - Deletes the instance specified (cmd1=instance id)
CreateInstance - Creates a worker node (spot instance) from a specified image (cmd1 = max bid price, cmd2 = availability zone)
GetSpotPrice - List all availability zones and the min/max/current spot price for making a decision on what to run

This application manages spot instances and management of log processing through log files and worker http requests

***

Author: Philip Matuskiewicz - philip.matuskiewicz@nyct.com       

Changes:
	6/6/16 - Initial script

Acknowledgements:
	http://stackoverflow.com/questions/7936572/python-call-a-function-from-string-name
	http://stackoverflow.com/questions/19029588/how-to-auto-assign-public-ip-to-ec2-instance-with-boto

'''

import random
import sys
import boto
import boto.ec2
import boto.vpc
import boto.ec2.networkinterface
from boto.s3.key import Key
import configparser

#global variables
CMD1 = False
CMD2 = False

CONFIG = configparser.ConfigParser()
if len(sys.argv) == 3 or len(sys.argv) == 4 or len(sys.argv) == 5 or len(sys.argv) == 6:
	inputini = sys.argv[1];
	if inputini.endswith(".ini"):
		CONFIG.read(inputini)
	else:
		print ("Config file not specified, look at my header for how to use this script")
		sys.exit(0)
	
	if len(sys.argv) > 3:
		c = str(sys.argv[3]).strip().lower()
		if "delete" in c:
			CMD1 = True
		elif "i-" in c:
			CMD1 = c #will be an AWS instance ID to delete
		elif c.replace(".","").isdigit():
			CMD1 = float(str(sys.argv[3]).strip())
			CMD2 = str(sys.argv[4]).strip()
		else:
			print("The parameters weren't good, try again")
			sys.exit(0)
	c = sys.argv[2]
	method_name = "do_" + str(c.strip().lower())
else:
	print ("usage listed in this file")
	sys.exit(0)

#Load configuration from ini file
DST_PATH = CONFIG.get('main', 'DST_PATH')
DST_AWS_ACCESS_KEY = CONFIG.get('main', 'DST_AWS_ACCESS_KEY')
DST_AWS_SECRET_KEY = CONFIG.get('main', 'DST_AWS_SECRET_KEY')
PROCESSING_LOCK_FILE = CONFIG.get('main', 'PROCESSING_LOCK_FILE')
VPC_ID = CONFIG.get('spot', 'VPC_ID')
ALLOWED_SUBNETS = CONFIG.get('spot', 'ALLOWED_SUBNETS') #what subnets can I launch instances in, comma separated list (AZ set via cmd line)
EC2_REGION = CONFIG.get('spot', 'EC2_REGION')
EC2_AWS_ACCESS_KEY = CONFIG.get('spot', 'EC2_AWS_ACCESS_KEY')
EC2_AWS_SECRET_KEY = CONFIG.get('spot', 'EC2_AWS_SECRET_KEY')
EC2_INSTANCE_TYPE = CONFIG.get('spot', 'EC2_INSTANCE_TYPE')
EC2_KEY_PAIR = CONFIG.get('spot', 'EC2_KEY_PAIR')
EC2_SECURITY_GROUP = CONFIG.get('spot', 'EC2_SECURITY_GROUP')
EC2_WORKER_AMI = CONFIG.get('spot', 'EC2_WORKER_AMI')

#main implementation 
def releaseLock(filePath):
	dst_s3conn = boto.connect_s3(DST_AWS_ACCESS_KEY, DST_AWS_SECRET_KEY)
	dst_bucket = dst_s3conn.get_bucket(DST_PATH[:DST_PATH.index('/')])
	lock_file_path = "%s%s%s" % (DST_PATH[DST_PATH.index('/'):],filePath,PROCESSING_LOCK_FILE)
	lock_file_key = Key(dst_bucket, lock_file_path)
	if not lock_file_key.exists():
		print("There was no lock... hoping nothing went wrong!  You may want to verify this.")
	else:
		instanceid = bytes(lock_file_key.get_contents_as_string()).decode(encoding='UTF-8')
		dst_bucket.delete_key(lock_file_key)
		print("The lock file has been removed, the instance it was running on is \"%s\"" % instanceid)
	dst_s3conn.close()

def getLocks():
	out = list()
	dst_s3conn = boto.connect_s3(DST_AWS_ACCESS_KEY, DST_AWS_SECRET_KEY)
	bucket = dst_s3conn.get_bucket(DST_PATH[:DST_PATH.index('/')])
	for year in bucket.list(prefix=DST_PATH[DST_PATH.index('/')+1:], delimiter='/'):
		yearint = year.name[-5:-1]
		for month in bucket.list(prefix=year.name, delimiter='/'):
			monthint = month.name[-3:-1]
			for day in bucket.list(prefix=month.name, delimiter='/'):
				dayint = day.name[-3:-1]
				dstdir = "%s%s%s" % (monthint, dayint, yearint)
				for filePath in bucket.list(prefix=day.name, delimiter='/'):
					if PROCESSING_LOCK_FILE in filePath.name:
						instance = bytes(filePath.get_contents_as_string()).decode(encoding='UTF-8')
						out.append("%s~%s" % (filePath.name, instance))
	dst_s3conn.close()
	return out

#we dont need to cancel instances, only spot requests associated with the instance
def cancelSpotRequest(spotobject):
	if "sir-" in spotobject["spotid"]:
		print ("Canceling %s " % spotobject["spotid"])

def getSpotRequests(conn): # connection is connect_to_region of 3c2
	#using EC2_WORKER_AMI, get all running spot requests and any running instances with them
	req = conn.get_all_spot_instance_requests()
	state_open = list()
	state_active = list()
	for r in req:
		if r.launch_specification is not None:
			if EC2_WORKER_AMI.lower().strip() not in str(r.launch_specification).lower():
				continue
		if "active" in r.state:
			#active instances
			state_active.append({"instance":r.instance_id,"spotid":r.id})
		elif "open" in r.state:
			# waiting for the request to be fulfilled
			state_open.append({"spotid":r.id})
		elif "failed" in r.state:
			print("The spot request %s is in a failure state..., you should cancel it" % r.id)
			continue
		elif "closed" in r.state:
			print("The spot request %s was closed... you should cancel it" % r.id)
			continue
		else:
			continue
	return {"open":state_open, "active": state_active}


def randomword(length):
	return ''.join(random.choice(string.lowercase) for i in range(length))

#command implementation 
def do_getspotprice():
	conn = boto.ec2.connect_to_region(EC2_REGION, aws_access_key_id=EC2_AWS_ACCESS_KEY, aws_secret_access_key=EC2_AWS_SECRET_KEY)
	zones = conn.get_all_zones()
	for zone in zones:
		prices = conn.get_spot_price_history(instance_type=EC2_INSTANCE_TYPE,availability_zone=zone.name,max_results=500)
		if len(prices) < 1:
			continue
		p = prices[0].price
		maxp = -1
		minp = 99999999999999
		for price in prices:
			if price.price > maxp:
				maxp = price.price
			elif price.price < minp:
				minp = price.price
		print("Availability Zone: %s has current price %s for instance type %s, min was %s, max was %s" % (zone.name, p, EC2_INSTANCE_TYPE, minp, maxp))
	conn.close()

def do_createinstance():
	if CMD1 is False or CMD2 is False:
		print ("The bid price %s and/or zone id %s is bad." % (CMD1, CMD2))
		return
	print("The user requested ami %s with a max bid of %f in zone %s" % (EC2_WORKER_AMI, CMD1, CMD2))
	print("Additionally, we will use SG: %s, EC2 Instance Type: %s, Keypair: %s" % (EC2_SECURITY_GROUP, EC2_INSTANCE_TYPE, EC2_KEY_PAIR))
	
	vpcc = boto.vpc.VPCConnection(aws_access_key_id=EC2_AWS_ACCESS_KEY, aws_secret_access_key=EC2_AWS_SECRET_KEY)
	vpcid = {"vpcId":VPC_ID, "availabilityZone":CMD2}
	available_subnets = vpcc.get_all_subnets(filters=vpcid)
	use_subnet = False
	for asub in available_subnets:
		if asub.id in ALLOWED_SUBNETS.split(","):
			use_subnet = asub.id
	vpcc.close()

	if use_subnet is False:
		print("No ALLOWED_SUBNETS found under configuration, please make sure list is csv with format subnet-12a34123")
		print("This script only runs inside of VPC, you need a VPCID and Subnet configured for newer instances anyways.")
		return
	
	sg = list()
	sg.append(EC2_SECURITY_GROUP)
	interface = boto.ec2.networkinterface.NetworkInterfaceSpecification(subnet_id=use_subnet, groups=sg, associate_public_ip_address=True)
	interfaces = boto.ec2.networkinterface.NetworkInterfaceCollection(interface)

	conn = boto.ec2.connect_to_region(EC2_REGION, aws_access_key_id=EC2_AWS_ACCESS_KEY, aws_secret_access_key=EC2_AWS_SECRET_KEY)
	succ = conn.request_spot_instances(
		price = float(CMD1),
		image_id = EC2_WORKER_AMI,
		#we will use the EC2 Key Pair to differentiate these instances from others
		key_name = EC2_KEY_PAIR,
		instance_type = str(EC2_INSTANCE_TYPE).strip(),
		placement = CMD2,
		network_interfaces=interfaces
	)
	
	print("Request success: %s" % succ)
	conn.close()

	

#TODO: List all instances and locks, note if instance is active, note if instance has a job running
def do_listall():
	conn = boto.ec2.connect_to_region(EC2_REGION, aws_access_key_id=EC2_AWS_ACCESS_KEY, aws_secret_access_key=EC2_AWS_SECRET_KEY)
	reqs = getSpotRequests(conn)
	print(reqs)
	
	conn.close()

#TODO: List all instances that are running but have no jobs attached to them
def do_listorphanedinstances():
	return "True"

#TODO List all locks that mismatch with what the instance says it's doing
def do_listmismatchedlocks():
	return "True"

#TODO List all locks that don't have an associated instance that is running
def do_listorphanedlocks():
	locks = getLocks()
	for lock in locks:
		parts = lock.split("~")
		lockfilepath = parts[0]
		instanceid = parts[1]
		if CMD1:
			print("Not implemented, delete requested")	

#TODO only allow lock delete if it is not associated with a running instance, otherwise make force option
def do_deletelock():
	if CMD1.isdigit():
		#MMDDYYYY
		m = CMD1[:2]
		d = CMD1[2:4]
		y = CMD1[4:]
		releaselockdir = "%s/%s/%s/" % (y,m,d)
		releaseLock(releaselockdir)

#TODO delete the instance listed
def do_deleteinstance():
	return "True"

# run teh application by invoking the correct method	
possibles = globals().copy()
possibles.update(locals())
method = possibles.get(method_name)
if not method:
	raise NotImplementedError("Command %s is not implemented" % method_name)
method()
