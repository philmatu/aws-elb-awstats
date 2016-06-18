#/bin/sh
if [ ! -f /root/aws-elb-awstats/elb/localupdatelock ]; then
	/usr/bin/touch /root/aws-elb-awstats/elb/localupdatelock
	/usr/bin/python3 /root/aws-elb-awstats/elb/compress_scheduler.py /root/aws-elb-awstats/elb/config.ini file
	/usr/bin/python3 /root/aws-elb-awstats/elb/elb_compress.py /root/aws-elb-awstats/elb/config.ini
	/usr/bin/python3 /root/aws-elb-awstats/elb/awstatsUpdater.py /root/aws-elb-awstats/elb/config.ini 
	/bin/rm /root/aws-elb-awstats/elb/localupdatelock
fi
