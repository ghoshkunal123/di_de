#!/usr/bin/python -tt
##########################################################################
#
#      PROGRAM: common_function.py
#      PURPOSE: Use this script to utilize common functions for all jobs
#
#                                  ****** NOTE ******
#
#                                  ****** NOTE ******
#
#       AUTHOR: Kunal Ghosh
#         DATE: 03/29/2017
#
#   Change Log:
#
#         DATE      PGMR              DESCRIPTION
#      ---------- -----------  ---------------------------------
#      03/29/2017 Kunal Ghosh   Initial Code.
#
##########################################################################

#********************************************
# All the import statements go here
#********************************************
import sys
import json
import boto3
import datetime
from dateutil import tz

#********************************************
# Send sns notifications from any job
#********************************************
#subject='Hi'
#message={"default": "Hi, \n My name is Kunal. \n Thanks and regards, \n Kunal Ghosh","email": "Hi, \n My name is Kunal. \n Thanks and regards, \n Kunal Ghosh"}


def send_sns_email(subject,message):
	client = boto3.client('sns',region_name='us-west-1')
	response = client.publish(
	TargetArn='arn:aws:sns:us-west-1:224919220385:finr-ml-ds-poc-email-sns',
	Message=json.dumps(message),
	Subject=subject,
	MessageStructure='json')
	retcode=response["ResponseMetadata"]["HTTPStatusCode"]
	if retcode != 200:
		print "Error: unable to send email"
		sys.exit(1)
	else:
		print "Successfully sent email"
#send_sns_email(subject,message)

#********************************************
# Convert local time to utc
#********************************************
def local_to_utc(dt,fmt):
	from_zone=tz.tzlocal()
	to_zone=tz.tzutc()
	local=datetime.datetime.strptime(dt,fmt)
	local=local.replace(tzinfo=from_zone)
	utc=local.astimezone(to_zone)
	return utc
