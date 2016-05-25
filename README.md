Author: Philip Matuskiewicz
Date: 5/24/2016

Acknowledgements:
https://github.com/rohit01/awstats_for_elb/tree/master/roles/awstats
http://serverfault.com/questions/625088/awstats-logformat-for-aws-elastic-load-balancer

Tips/Tricks since I don't have time to write out a full readme...

elb folder includes what you need to handle ELB logs on AWS... this isn't finished yet
cloudfront folder includes what you need to handle Cloudfront logs on aws

Run the awstats crunching process with ulimit to avoid file problems on large sites:
ulimit -n 16384
awstats -update -config=example.com -LogFile="/root/logresolvemerge.pl datafiles/*.log |"

Also, we need pip, smart_open, and boto modules on ubuntu prior to starting (>16.10)
apt-get install python-boto
pip install smart_open

A sample ElasticLoadBalancer log line looks like:
2016-05-12T21:48:58.253468Z appelb-pr-ElasticL-3M29U6FNWKZ7 62.114.132.221:32658 10.167.134.188:80 0.000024 0.029101 0.000023 200 200 0 839 "GET http://api.example.com:80/api/data.json?key=nebwh37443&LineRef=8 HTTP/1.1" "Server/1 myLib/17 Server/1 Device/Server" - -

