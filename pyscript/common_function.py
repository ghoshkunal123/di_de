#!/usr/bin/python3 -tt
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
#      07/31/2017 Kunal Ghosh   Added the send_email module.
#      09/12/2017 Kunal Ghosh	Using the logging module.
#      11/04/2017 Kunal Ghosh   Python3 compatible.
#      11/06/2017 Kunal Ghosh	Create directory tree.  	
#      11/06/2017 Kunal Ghosh   Change permission of directory tree.	
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
import smtplib
import logging
import os

#********************************************
# Send sns notifications from any job
#********************************************
#subject='Hi'
#message={"default": "Hi, \n My name is Kunal. \n Thanks and regards, \n Kunal Ghosh","email": "Hi, \n My name is Kunal. \n Thanks and regards, \n Kunal Ghosh"}


def send_sns_email(subject,message):
	client = boto3.client('sns',region_name='us-west-1')
	response = client.publish(
	TargetArn='arn:aws:sns:us-west-1:224919220385:FINR_Admin_Alerts',
	Message=json.dumps(message),
	Subject=subject,
	MessageStructure='json')
	retcode=response["ResponseMetadata"]["HTTPStatusCode"]
	if retcode != 200:
		#print "Error: unable to send email"
		logging.error("unable to send email")
		sys.exit(1)
#	else:
#		print "Successfully sent email"
#		logging.info("Successfully sent email")
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


#********************************************
# Send email notifications from any job
#********************************************
#sender='Mystry'
#receivers=['kghosh@financialengines.com']
#subject='Hello'
#body='Hi, \n My name is Kunal. \n Thanks and regards, \n Kunal Ghosh'

def send_email(sender,receivers,subject,body):
	message = """From: %s
To: %s
Subject: %s
%s
""" % (sender,', '.join(receivers),subject,body)

	try:
		server = smtplib.SMTP('email.fefinr.io')
		server.sendmail(sender, receivers, message)
#		logging.info("Successfully sent email")
	except smtplib.SMTPException:
		logging.error("unable to send email")
	finally:
		server.quit()
#send_email(sender,receivers,subject,body)

#********************************************
# Create directory tree
#********************************************
def create_dir_tree(dir_path):
	os.makedirs(dir_path,exist_ok=True)

#********************************************
# Change directory tree permission
#********************************************
def change_dir_tree_perm(dir_path,perm):
	os.chmod(dir_path,perm)
	for r,d,f in os.walk(dir_path):
		for di in d:
			os.chmod(os.path.join(r,di),perm)
		for fi in f:
			os.chmod(os.path.join(r,fi),perm)

