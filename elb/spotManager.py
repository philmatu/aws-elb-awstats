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
ListDuplicates - List all locks that concurrently have the same instance, also list all instances that currently have the same work directory
DeleteLock - Deletes the lock file for a given directory (cmd1=MMDDYYYY as 8 digit integer)
DeleteInstance - Deletes the instance specified (cmd1=spot request id or instance id associated with a spot request)
CreateInstance - Creates a worker node (spot instance) from a specified image (cmd1 = max bid price, cmd2 = availability zone)
GetSpotPrice - List all availability zones and the min/max/current spot price for making a decision on what to run

This application manages spot instances and management of log processing through log files and worker http requests

Helpful URLS:
Spot Bid Advisor: https://aws.amazon.com/ec2/spot/bid-advisor/
Spot Bid Pricing: https://aws.amazon.com/ec2/spot/pricing/
On Demand Pricing: https://aws.amazon.com/ec2/pricing/

***

Author: Philip Matuskiewicz - philip.matuskiewicz@nyct.com       

Changes:
	6/6/16 - Initial script
	6/9/16 - Implementation completed

Acknowledgements:
	http://stackoverflow.com/questions/7936572/python-call-a-function-from-string-name
	http://stackoverflow.com/questions/19029588/how-to-auto-assign-public-ip-to-ec2-instance-with-boto

'''

import random
import sys
import urllib.request
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
		elif c.startswith("i-") or c.startswith("sir-"):
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
WORKER_STATUS_FILE_NAME = CONFIG.get('main', 'WORKER_STATUS_FILE_NAME')

#main implementation 
def releaseLock(filePath):#in format YYYY/MM/DD/
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
				testdirname = ""
				if day.name.endswith('/'):
					testdirname = day.name[len(day.name)-11:-1]
				else:
					testdirname = day.name[len(day.name)-11:-1]
				digitsum = sum(c.isdigit() for c in testdirname)
				if digitsum != 8:
					#first directory... no files in it
					continue
				dayint = day.name[-3:-1]
				dstdir = "%s/%s/%s/" % (monthint, dayint, yearint)
				for filePath in bucket.list(prefix=day.name, delimiter='/'):
					if PROCESSING_LOCK_FILE in filePath.name:
						instance = bytes(filePath.get_contents_as_string()).decode(encoding='UTF-8')
						if instance.count("i-") > 1:
							print("WARNING: file \"%s\" has more than 1 instance (i-) in it, you need to correct this manually! Contents: \"%s\"" % (filePath.name,instance))
						elif instance.count("i-")  == 1:
							out.append("%s~%s" % (dstdir, instance))
						else:
							print("WARNING: Empty lock file detected in dir \"%s\"... you might want to delete this!" % filePath.name)
							out.append("%s~%s" % (dstdir, instance))
							
	dst_s3conn.close()
	return out

#we dont need to cancel instances, only spot requests associated with the instance
def cancelSpotRequest(conn,spotobject):
	if "sir-" in spotobject["spotid"]:
		print ("Canceling %s " % spotobject["spotid"])
		ids = list()
		ids.append(spotobject["spotid"])
		res = conn.cancel_spot_instance_requests(ids)
		print("Result for spot instance request cancellation: %s" % res)
	else:
		print("%s is not a valid spot instance containing object" % spotobject)
	if "instance" in spotobject:
		if "i-" in spotobject["instance"]:
			ids = list()
			ids.append(spotobject["instance"])
			res = conn.terminate_instances(ids)
			print("Instance %s was terminated"%(spotobject["instance"]))

def getSpotRequests(conn): # connection is connect_to_region of 3c2
	#using EC2_WORKER_AMI, get all running spot requests and any running instances with them
	req = conn.get_all_spot_instance_requests()
	state_open = list()
	state_active = list()
	state_cancelled = list()
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
		elif "cancelled" in r.state:
			if r.instance_id is not None:
				dns = getDNSFromInstanceID(r.instance_id, conn)
				if len(dns) > 2:
					print("Warning, Cancelled Spot Request \"%s\" has a running instance still, it is \"%s\"" % (r.id, r.instance_id))
					state_cancelled.append({"spotid":r.id,"instance":r.instance_id})
			continue
		else:
			continue
	return {"open":state_open, "active": state_active, "cancelled": state_cancelled}

#pass in the instance id to get the IP address of the instance
def getDNSFromInstanceID(instanceid, ec2connection):
	reservations = ec2connection.get_all_reservations(filters={'instance-id' : instanceid})
	if len(reservations) < 1:
		print("There are no spot instances available, did you launch any?")
		return ""
	instance = reservations[0].instances[0]
	if "running" in instance.state:
		return instance.public_dns_name
	return ""

def getRunningTaskOnInstance(instanceid, ec2connection):
	instancedns = getDNSFromInstanceID(instanceid, ec2connection)	
	if len(instancedns) < 2:
		#no dns means the instance isn't running
		return ""
	url = "http://%s/%s" % (instancedns,WORKER_STATUS_FILE_NAME)
	try:
		resource = urllib.request.urlopen(url, timeout=5)
	except urllib.error.HTTPError as e:
		print("WARN: The URL wasn't found (404) on instance \"%s\" via url \"%s\", this instance is likely defunct" % (instanceid,url))
		return ""
	except urllib.error.URLError as e:
		print("WARN: The instance \"%s\" had another error or some type, url was \"%s\", this instance is likely defunct" % (instanceid,url))
		return ""
	except timeout:
		print("WARN: The instance \"%s\" timed out... did it start yet?  Url: \"%s\"" % (instanceid,url))
		return ""
	data = resource.read().decode('utf-8').strip()
	if data.count("/") != 3:
		if data.count("/") != 0:
			print("\tWARNING: There were not 0 nor 3 \"/\"'s in the instance's status file, the instance is %s with web query %s and the data from it was %s" % (instanceid,url,data))
	return data

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

#List all instances and locks, note if instance is active, note if instance has a job running
def do_listall():
	conn = boto.ec2.connect_to_region(EC2_REGION, aws_access_key_id=EC2_AWS_ACCESS_KEY, aws_secret_access_key=EC2_AWS_SECRET_KEY)
	reqs = getSpotRequests(conn)
	out = list()
	out.append("Unfulfilled Spot Requests")
	for item in reqs["open"]:
		out.append("-- SpotID: \"%s\"" % item["spotid"])
	out.append("\nRunning Spot Requests")
	for item in reqs["active"]:
		instanceIP = getDNSFromInstanceID(item["instance"], conn)
		instanceWorkDir = getRunningTaskOnInstance(item["instance"], conn)
		out.append("-- SpotID: \"%s\" InstanceID: \"%s\" WorkingOn (based on instance data): \"%s\" InstanceDNSName: \"%s\"" % (item["spotid"],item["instance"],instanceWorkDir,instanceIP))
	out.append("\nCanceled Spot Requests with running instances, you should probably cancel these (this is likely a manual execution)!")
	for item in reqs["cancelled"]:
		instanceIP = getDNSFromInstanceID(item["instance"], conn)
		if len(instanceIP) < 2:
			out.append("-- SpotID: \"%s\" InstanceID: \"%s\" is already shut down / canceled" % (item["spotid"],item["instance"]))
			continue
		instanceWorkDir = getRunningTaskOnInstance(item["instance"], conn)
		out.append("-- SpotID: \"%s\" InstanceID: \"%s\" WorkingOn (based on instance data): \"%s\" InstanceDNSName: \"%s\"" % (item["spotid"],item["instance"],instanceWorkDir,instanceIP))
	out.append("\nList of locks with instance ids (based on S3 directory search)")
	for lock in getLocks():
		parts = lock.split("~")
		lockfilepath = parts[0]
		instanceid = parts[1]
		out.append("--Directory \"%s\" claimed by instance \"%s\"" % (lockfilepath,instanceid))
	conn.close()
	print("")
	for line in out:
		print(line)

#List all instances that are running but have no jobs attached to them
def do_listorphanedinstances():
	out = list()
	conn = boto.ec2.connect_to_region(EC2_REGION, aws_access_key_id=EC2_AWS_ACCESS_KEY, aws_secret_access_key=EC2_AWS_SECRET_KEY)
	reqs = getSpotRequests(conn)
	out.append("Orphaned (Not working on anything) Active Spot Instances:")
	for item in reqs["active"]:
		instanceWorkDir = getRunningTaskOnInstance(item["instance"], conn)
		instanceIP = getDNSFromInstanceID(item["instance"], conn)
		if len(instanceWorkDir) > 1:
			continue
		if CMD1:
			cancelSpotRequest(conn, item)
		else:
			out.append("-- SpotID: \"%s\" InstanceID: \"%s\" InstanceDNSNAME: \"%s\"" % (item["spotid"],item["instance"],instanceIP))
	out.append("\nCanceled Spot Requests with running instances, you should probably cancel these (this is likely a manual execution)!")
	for item in reqs["cancelled"]:
		instanceIP = getDNSFromInstanceID(item["instance"], conn)
		instanceWorkDir = getRunningTaskOnInstance(item["instance"], conn)
		if len(instanceWorkDir) > 1:
			continue
		if CMD1:
			cancelSpotRequest(conn, item)
		else:
			out.append("-- SpotID: \"%s\" InstanceID: \"%s\" WorkingOn (based on instance data): \"%s\" InstanceDNSName: \"%s\"" % (item["spotid"],item["instance"],instanceWorkDir,instanceIP))
	conn.close()
	print("")
	for line in out:
		print(line)

#List all locks that mismatch with what the instance says it's doing
def do_listmismatchedlocks():
	locks = getLocks()
	conn = boto.ec2.connect_to_region(EC2_REGION, aws_access_key_id=EC2_AWS_ACCESS_KEY, aws_secret_access_key=EC2_AWS_SECRET_KEY)
	reqs = getSpotRequests(conn)

	for item in reqs["active"]:
		instanceWorkDir = getRunningTaskOnInstance(item["instance"], conn)# in YYYY/MM/DD/
		for lock in locks:
			parts = lock.split("~")
			lockfilepath = parts[0]# in MM/DD/YYYY/
			lockfilepathmatched = "%s/%s/%s/" % (lockfilepath[6:10],lockfilepath[:2],lockfilepath[3:5])
			instanceid = parts[1].strip()
			if item["instance"] in instanceid:
				if lockfilepathmatched.strip().lower() in instanceWorkDir.strip().lower():
					print("INFO: Instance \"%s\" stated work directory \"%s\" matches the lock file on S3" % (instanceid,instanceWorkDir.strip()))
				else:
					print("WARN: Mismatch for instance \"%s\", instance reports lock on \"%s\" but the lock file there reports \"%s\" has the lock" % (item["instance"],instanceWorkDir,instanceid))
	
	conn.close()

def do_listduplicates():
	out = list()

	locks = getLocks()
	
	d = {}
	for lock in locks:
		parts = lock.split("~")
		instanceid = parts[1].strip()
		lockfilepath = parts[0]
		d.setdefault(instanceid, set())
		d[instanceid].add(lockfilepath)
	out.append("\nLooking for duplicate directories listed:")	
	for key in d:
		if len(key) < 2:
			for val in d[key]:
				out.append("\t--WARNING: Empty Lock File (no instances listed) in directory(s) \"%s\"" % val)
		else:
			if len(d[key]) > 1:
				out.append("\t--WARNING: Duplicate instance \"%s\" found in directories \"%s\"" % (key, d[key]))

	conn = boto.ec2.connect_to_region(EC2_REGION, aws_access_key_id=EC2_AWS_ACCESS_KEY, aws_secret_access_key=EC2_AWS_SECRET_KEY)
	reqs = getSpotRequests(conn)
	e = {}
	for item in reqs["active"]:
		lockfilepath = getRunningTaskOnInstance(item["instance"], conn)
		instanceid = item["instance"]
		e.setdefault(lockfilepath, set())
		e[lockfilepath].add(instanceid)
	out.append("\nLooking for work directories that are being processed by more than 1 instance simultaneously:")	
	for key in e:
		if len(key) < 2:
			for val in e[key]:
				out.append("\t--WARNING: Empty Status File (not doing any work) on instance \"%s\"" % val)
		else:
			if len(e[key]) > 1:
				out.append("\t--WARNING: Duplicate workdirectory \"%s\" found on instances \"%s\"" % (key, e[key]))
	conn.close()
	print("")
	for line in out:
		print(line)

#List all locks that don't have an associated instance that is running
def do_listorphanedlocks():
	locks = getLocks()
	conn = boto.ec2.connect_to_region(EC2_REGION, aws_access_key_id=EC2_AWS_ACCESS_KEY, aws_secret_access_key=EC2_AWS_SECRET_KEY)
	reqs = getSpotRequests(conn)
	for lock in locks:
		parts = lock.split("~")
		lockfilepath = parts[0]
		instanceidlockfile = parts[1]
		ok = False
		for item in reqs["active"]:
			instanceid = item["instance"]
			if instanceid.strip() in instanceidlockfile:
				ok = True
				break
		if not ok:
			if CMD1:
				print("INFO: Lock on directory \"%s\" didn't have a running, matching instance, deleting the lock now" % lockfilepath)
				releaseLock(lockfilepath)
			else:
				print("INFO: Lock on directory \"%s\" doesn't have an associated instance running, maybe it was prematurely killed?  Try deleting this and requeuing?" % lockfilepath)

#NOTE: might be a good idea to only allow lock delete if it is not associated with a running instance, otherwise make force option
def do_deletelock():
	if CMD1.isdigit():
		#MMDDYYYY
		m = CMD1[:2]
		d = CMD1[2:4]
		y = CMD1[4:]
		releaselockdir = "%s/%s/%s/" % (y,m,d)
		releaseLock(releaselockdir)

def do_deleteinstance():
	conn = boto.ec2.connect_to_region(EC2_REGION, aws_access_key_id=EC2_AWS_ACCESS_KEY, aws_secret_access_key=EC2_AWS_SECRET_KEY)
	reqs = getSpotRequests(conn)
	for state in reqs:
		for item in reqs[state]:
			if CMD1.strip().lower() in str(item):
				cancelSpotRequest(conn,item)
	conn.close()

# run teh application by invoking the correct method	
possibles = globals().copy()
possibles.update(locals())
method = possibles.get(method_name)
if not method:
	raise NotImplementedError("Command %s is not implemented" % method_name)
method()
