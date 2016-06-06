<pre>
Author: Philip Matuskiewicz
Date: 5/24/2016

Acknowledgements:
https://github.com/rohit01/awstats_for_elb/tree/master/roles/awstats
http://serverfault.com/questions/625088/awstats-logformat-for-aws-elastic-load-balancer

Tips/Tricks since I don't have time to write out a full readme...

I started with ami 13be557e (16.04-lts)
ran: apt-get update && apt-get upgrade -y && apt-get dist-upgrade -y
changed lts to normal: vim /etc/update-manager/release-upgrades
ran: do-release-upgrade -d
ran: apt-get install python3-boto python3-pip apache2 libgeo-ipfree-perl libnet-ip-perl python3-iso8601 python3-tz libnet-dns-perl
ran: pip3 install smart_open
ran: pip3 install configparser
ran: apt-get install awstats
ran: a2enmod cgi
ran: cp awstats.conf /etc/apache2/conf-enabled/ && service apache2 restart

copy cloudfront/awstats.domain.conf to /etc/awstats
create config.ini and then run python3 cloudfrontproc.py config.ini

elb folder includes what you need to handle ELB logs on AWS... this isn't finished yet
cloudfront folder includes what you need to handle Cloudfront logs on aws

Run the awstats crunching process with ulimit to avoid file problems on large sites:
ulimit -n 16384
awstats -update -config=example.com -LogFile="/root/logresolvemerge.pl datafiles/*.log |"

A sample ElasticLoadBalancer log line looks like:
2016-05-12T21:48:58.253468Z appelb-pr-ElasticL-3M29U6FNWKZ7 62.114.132.221:32658 10.167.134.188:80 0.000024 0.029101 0.000023 200 200 0 839 "GET http://api.example.com:80/api/data.json?key=nebwh37443&LineRef=8 HTTP/1.1" "Server/1 myLib/17 Server/1 Device/Server" - -

IAM Policy:
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:ListAllMyBuckets",
            "Resource": "arn:aws:s3:::*"
        },
        {
            "Effect": "Allow",
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::example-bucket",
                "arn:aws:s3:::example-bucket/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": "sqs:*",
            "Resource": [
                "arn:aws:sqs:*"
            ]
        }
    ]
}

</pre>
