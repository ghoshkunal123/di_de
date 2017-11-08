#!/usr/bin/python3 -tt
##########################################################################
#
#      PROGRAM: copy_from_ec2_to_s3.py
#      PURPOSE: Copy files from ec2 to s3 bucket using multipart upload
#
#                                  ****** NOTE ******
#
#                                  ****** NOTE ******
#
#       AUTHOR: Kunal Ghosh
#         DATE: 05/03/2017
#
#   Change Log:
#
#         DATE      PGMR              DESCRIPTION
#      ---------- -----------  ---------------------------------
#      05/03/2017 Kunal Ghosh   Initial Code.
#      09/19/2017 Kunal Ghosh	Including the logging module.
#      09/19/2017 Kunal Ghosh	Added new function send_sns_email_alert.
#      11/04/2017 Kunal Ghosh	Python3 compatible.
#      11/08/2017 Kunal Ghosh	Call initial setup.
#
##########################################################################


#********************************************
# All the import statements go here
#********************************************
import boto3
import botocore
import sys
import os
import datetime
import json
import logging


#********************************************
# All custom imports go here
#********************************************
import common_config
import common_function
from common_function import send_sns_email as send_sns_email
from common_function import create_dir_tree,change_dir_tree_perm

#********************************************
# Get current date in proper format
#********************************************
def get_curr_date_time():
        return datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S_%f')

#********************************************
# Send job email
#********************************************
def send_sns_email_alert(email_subject,email_body):
	msg={}
	msg['default'] = email_body
	msg_json=json.loads(json.dumps(msg))
	logger.info('email_subject :{}'.format(email_subject))
	logger.info('email_body :{}'.format(email_body))
	logger.info('msg :{}'.format(msg))
	logger.info('msg_json :{}'.format(msg_json))
	try:
		send_sns_email(email_subject,msg_json)
	except:
		logger.error('Failed sending {} email.'.format(email_subject))

#********************************************
# Initial Setup
#********************************************
def init_setup(some_dir):
	if not os.path.exists(some_dir):
		create_dir_tree(some_dir)
		change_dir_tree_perm(some_dir,0o775)


#********************************************
# This is the main function
#********************************************

if __name__ == '__main__':
#********************************************
# Setting up all environment variables
#********************************************
	log_dir=common_config.log_dir
	current_date=datetime.datetime.strftime(datetime.datetime.now(),'%Y%m%d%H%M%S')
	if (os.path.dirname(sys.argv[0]) == '.'):
		job_dir=os.getcwd()
	else:
		job_dir=os.path.dirname(sys.argv[0])
	py_job_name=os.path.basename(sys.argv[0]).split('.')[0]

	if len(sys.argv[1:]) == 0 or len(sys.argv[1:]) != 3:
		print('Usage Error: <py_job_name> <bucket_name> <file_name_with_full_path> <bucket_prefix>')
		print('Example: ./copy_from_ec2_to_s3.py fe-finr-da /mnt/data/Dev/bulk/mssql/prodcopy/account_plan_instrument/account_plan_instrument.csv Dev/bulk/mssql/prodcopy/account_plan_instrument/')
		sys.exit(1)

	bucket_name=sys.argv[1]
	file_name_with_full_path=sys.argv[2]
	bucket_prefix=sys.argv[3]
	py_job_name=py_job_name + '_' + '_'.join(os.path.basename(file_name_with_full_path).split('.'))
	logfile=py_job_name + '_' +  current_date + '.log'
	data_dir=common_config.data_dir
	trigger_dir=common_config.trigger_dir
	increment_dir=common_config.increment_dir
	bulk_dir=common_config.bulk_dir
	script_dir=common_config.script_dir
	pyscript_dir=common_config.pyscript_dir
	s3_region=common_config.s3_region
	s3_extra_args=common_config.s3_extra_args

#********************************************
# Setting up initial directories
#********************************************
	init_setup(data_dir)
	init_setup(log_dir)
	init_setup(trigger_dir)
	init_setup(increment_dir)
	init_setup(bulk_dir)

#********************************************
# Start writing log and error
#********************************************
#	sys.stdout = open(log_dir + logfile,'w')
#	sys.stderr = open(error_dir + errorfile,'w')

#********************************************
# Setting up the logger
#********************************************
	logger = logging.getLogger(__name__)
	logger.setLevel(logging.DEBUG)

#********************************************
# Setting up a file handler
#********************************************
	log_file_handle = logging.FileHandler(log_dir + logfile)
	log_file_handle.setLevel(logging.DEBUG)

#********************************************
# Create formatter
#********************************************
	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

#********************************************
# Add formatter to handler
#********************************************
	log_file_handle.setFormatter(formatter)

#********************************************
# Add handler to logger
#********************************************
	logger.addHandler(log_file_handle)
#********************************************
# Send job start email
#********************************************
	email_subject = 'Started! ' + py_job_name
	email_body = py_job_name + ' job has been started at ' + current_date
	try:
		send_sns_email_alert(email_subject,email_body)
	except:
		ogger.error('Failed sending the job start email.')

	logger.info('log_dir :{}'.format(log_dir))
	logger.info('current_date :{}'.format(current_date))
	logger.info('job_dir :{}'.format(job_dir))
	logger.info('bucket_name :{}'.format(bucket_name))
	logger.info('file_name_with_full_path :{}'.format(file_name_with_full_path))
	logger.info('bucket_prefix :{}'.format(bucket_prefix))
	logger.info('py_job_name :{}'.format(py_job_name))
	logger.info('logfile :{}'.format(logfile))
	logger.info('data_dir :{}'.format(data_dir))
	logger.info('trigger_dir :{}'.format(trigger_dir))
	logger.info('increment_dir :{}'.format(increment_dir))
	logger.info('bulk_dir :{}'.format(bulk_dir))
	logger.info('script_dir :{}'.format(script_dir))
	logger.info('pyscript_dir :{}'.format(pyscript_dir))
	logger.info('s3_region :{}'.format(s3_region))
	logger.info('s3_extra_args :{}'.format(s3_extra_args))

#********************************************
# Check if file is present to be copied to s3
#********************************************
	if not os.path.exists(file_name_with_full_path):
		logger.error('Failed to find the file :{}'.format(file_name_with_full_path))
#********************************************
# Send job failure email
#********************************************
		email_subject = 'Error! ' + py_job_name
		email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' finding the file :' + file_name_with_full_path
		try:
			send_sns_email_alert(email_subject,email_body)
		except:
			logger.error('Failed sending the job failure email.')
		sys.exit(2)
	else:
		bucket_file_name = os.path.basename(file_name_with_full_path)
		logger.info('bucket_file_name :{}'.format(bucket_file_name))

#********************************************
# Check for bucket existance
#********************************************
	s3_resource=boto3.resource('s3')
	if s3_resource.Bucket(bucket_name) not in s3_resource.buckets.all():
		logger.error('Failed to find the bucket :{}'.format(bucket_name))
#********************************************
# Send job failure email
#********************************************
		email_subject = 'Error! ' + py_job_name
		email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' finding the bucket :' + bucket_name
		try:
			send_sns_email_alert(email_subject,email_body)
		except:
			logger.error('Failed sending the job failure email.')
		sys.exit(3)

#********************************************
# Delete if file exists in bucket
#********************************************
	s3_object = s3_resource.Object(bucket_name,bucket_prefix + bucket_file_name)
	try:
		bucket_file_content_type = s3_object.content_type
		logger.info('File exists in s3 bucket and the content type is, {}'.format(bucket_file_content_type))
		logger.info('Deleting the file : {}{}'.format(bucket_prefix,bucket_file_name))
		s3_object.delete()
	except botocore.exceptions.ClientError as e:
		logger.error('{}: {}'.format(e.response['Error']['Code'],e.response['Error']['Message']))
		logger.error('File does not exist in s3 bucket and hence transferring the file :{}{}'.format(bucket_prefix,bucket_file_name))

#********************************************
# Transfer the file to s3 bucket
#********************************************
	s3_client = boto3.client('s3',s3_region)
	transfer = boto3.s3.transfer.S3Transfer(s3_client)
	try:
		transfer.upload_file(file_name_with_full_path,bucket_name,bucket_prefix + bucket_file_name,extra_args=s3_extra_args)
		logger.info('Completed s3 transfer for the file :{} to s3://{}{}{}'.format(file_name_with_full_path,bucket_name,bucket_prefix,bucket_file_name))
	except botocore.exceptions.ClientError as e:
		logger.error('{}: {}'.format(e.response['Error']['Code'],e.response['Error']['Message']))
		logger.error('Failed s3 transfer for the file :{} to s3://{}{}{}'.format(file_name_with_full_path,bucket_name,bucket_prefix,bucket_file_name))
#********************************************
# Send job failure email
#********************************************
		email_subject = 'Error! ' + py_job_name
		email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' s3 transfer for the file :' + file_name_with_full_path
		try:
			send_sns_email_alert(email_subject,email_body)
		except:
			logger.error('Failed sending the job failure email.')
		sys.exit(4)
			
#********************************************
# Send job end email
#********************************************
	email_subject = 'Success! ' + py_job_name + ' completed at: ' + current_date
	email_body = py_job_name + ' succeeded at ' + current_date
	try:
		send_sns_email_alert(email_subject,email_body)
	except:
		logger.error('Failed sending the job success email.')
