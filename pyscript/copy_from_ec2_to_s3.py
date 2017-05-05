#!/usr/bin/python -tt
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

#********************************************
# All custom imports go here
#********************************************
import common_config
import common_function
from common_function import send_sns_email as send_sns_email

#********************************************
# Get current date in proper format
#********************************************
def get_curr_date_time():
        return datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S_%f')


#********************************************
# This is the main function
#********************************************

if __name__ == '__main__':
#********************************************
# Setting up all environment variables
#********************************************
	log_dir=common_config.log_dir
	error_dir=common_config.error_dir
	current_date=datetime.datetime.strftime(datetime.datetime.now(),'%Y%m%d%H%M%S')
	if (os.path.dirname(sys.argv[0]) == '.'):
		job_dir=os.getcwd()
	else:
		job_dir=os.path.dirname(sys.argv[0])
	py_job_name=os.path.basename(sys.argv[0]).split('.')[0]

	if len(sys.argv[1:]) == 0 or len(sys.argv[1:]) != 3:
		print 'Usage Error: <py_job_name> <bucket_name> <file_name_with_full_path> <bucket_prefix>'
		print 'Example: ./copy_from_ec2_to_s3.py fe-finr-da /mnt/data/Dev/bulk/mssql/prodcopy/account_plan_instrument/account_plan_instrument.csv Dev/bulk/mssql/prodcopy/account_plan_instrument/'
		sys.exit(1)

	bucket_name=sys.argv[1]
	file_name_with_full_path=sys.argv[2]
	bucket_prefix=sys.argv[3]
	py_job_name=py_job_name + '_' + '_'.join(os.path.basename(file_name_with_full_path).split('.'))
	logfile=py_job_name + '_' +  current_date + '.log'
	errorfile=py_job_name + '_' + current_date + '.err'
	data_dir=common_config.data_dir
	trigger_dir=common_config.trigger_dir
	increment_dir=common_config.increment_dir
	bulk_dir=common_config.bulk_dir
	script_dir=common_config.script_dir
	pyscript_dir=common_config.pyscript_dir
	s3_region=common_config.s3_region
	s3_extra_args=common_config.s3_extra_args

#********************************************
# Start writing log and error
#********************************************
	sys.stdout = open(log_dir + logfile,'w')
	sys.stderr = open(error_dir + errorfile,'w')
#********************************************
# Send job start email
#********************************************
	email_subject = 'Started! ' + py_job_name
	msg={}
	email_body = py_job_name + ' job has been started at ' + current_date
	msg['default'] = email_body
	msg_json=json.loads(json.dumps(msg))
	print get_curr_date_time(), ': email_subject :',email_subject
	print get_curr_date_time(), ': email_body :',email_body
	print get_curr_date_time(), ': msg :',msg
	print get_curr_date_time(), ': msg_json :',msg_json
	try:
		send_sns_email(email_subject,msg_json)
	except:
		print get_curr_date_time(), ' Failed sending the job start email.'

	print get_curr_date_time(), ': log_dir :',log_dir
	print get_curr_date_time(), ': error_dir :',error_dir
	print get_curr_date_time(), ': current_date :',current_date
	print get_curr_date_time(), ': job_dir :',job_dir
	print get_curr_date_time(), ': bucket_name :',bucket_name
	print get_curr_date_time(), ': file_name_with_full_path :',file_name_with_full_path
	print get_curr_date_time(), ': bucket_prefix :',bucket_prefix
	print get_curr_date_time(), ': py_job_name :',py_job_name
	print get_curr_date_time(), ': logfile :',logfile
	print get_curr_date_time(), ': errorfile :',errorfile
	print get_curr_date_time(), ': data_dir :',data_dir
	print get_curr_date_time(), ': trigger_dir :',trigger_dir
	print get_curr_date_time(), ': increment_dir :',increment_dir
	print get_curr_date_time(), ': bulk_dir :',bulk_dir
	print get_curr_date_time(), ': script_dir :',script_dir
	print get_curr_date_time(), ': pyscript_dir :',pyscript_dir
	print get_curr_date_time(), ': s3_region :',s3_region
	print get_curr_date_time(), ': s3_extra_args :',s3_extra_args

#********************************************
# Check if file is present to be copied to s3
#********************************************
	if not os.path.exists(file_name_with_full_path):
		print get_curr_date_time(), ' Failed to find the file :' + file_name_with_full_path
#********************************************
# Send job failure email
#********************************************
		email_subject = 'Error! ' + py_job_name
		msg={}
		email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' finding the file :' + file_name_with_full_path
		msg['default'] = email_body
		msg_json=json.loads(json.dumps(msg))
		print get_curr_date_time(), ': email_subject :',email_subject
		print get_curr_date_time(), ': email_body :',email_body
		print get_curr_date_time(), ': msg :',msg
		print get_curr_date_time(), ': msg_json :',msg_json
		try:
			send_sns_email(email_subject,msg_json)
		except:
			print get_curr_date_time(), ' Failed sending the job failure email.'
		sys.exit(2)
	else:
		bucket_file_name = os.path.basename(file_name_with_full_path)
		print get_curr_date_time(), ': bucket_file_name :',bucket_file_name

#********************************************
# Check for bucket existance
#********************************************
	s3_resource=boto3.resource('s3')
	if s3_resource.Bucket(bucket_name) not in s3_resource.buckets.all():
		print get_curr_date_time(), ' Failed to find the bucket :' + bucket_name 
#********************************************
# Send job failure email
#********************************************
		email_subject = 'Error! ' + py_job_name
		msg={}
		email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' finding the bucket :' + bucket_name
		msg['default'] = email_body
		msg_json=json.loads(json.dumps(msg))
		print get_curr_date_time(), ': email_subject :',email_subject
		print get_curr_date_time(), ': email_body :',email_body
		print get_curr_date_time(), ': msg :',msg
		print get_curr_date_time(), ': msg_json :',msg_json
		try:
			send_sns_email(email_subject,msg_json)
		except:
			print get_curr_date_time(), ' Failed sending the job failure email.'
		sys.exit(3)

#********************************************
# Delete if file exists in bucket
#********************************************
	s3_object = s3_resource.Object(bucket_name,bucket_prefix + bucket_file_name)
	try:
		bucket_file_content_type = s3_object.content_type
		print get_curr_date_time(), ': File exists in s3 bucket and the content type is, ' ,bucket_file_content_type
		print get_curr_date_time(), ': Deleting the file :', bucket_prefix + bucket_file_name
		s3_object.delete()
	except botocore.exceptions.ClientError as e:
		print get_curr_date_time() + e.response['Error']['Code'] + ': ' + e.response['Error']['Message']
		print get_curr_date_time(), ': File does not exist in s3 bucket and hence transferring the file, ', bucket_prefix + bucket_file_name

#********************************************
# Transfer the file to s3 bucket
#********************************************
	s3_client = boto3.client('s3',s3_region)
	transfer = boto3.s3.transfer.S3Transfer(s3_client)
	try:
		transfer.upload_file(file_name_with_full_path,bucket_name,bucket_prefix + bucket_file_name,extra_args=s3_extra_args)
		print get_curr_date_time(), ': Completed s3 transfer for the file, ', file_name_with_full_path, ' to s3://' + bucket_name + '/' + bucket_prefix + bucket_file_name
	except botocore.exceptions.ClientError as e:
		print get_curr_date_time() + e.response['Error']['Code'] + ': ' + e.response['Error']['Message']
		print get_curr_date_time(), ': Failed s3 transfer for the file, ', file_name_with_full_path, ' to s3://' + bucket_name + '/' + bucket_prefix + bucket_file_name
#********************************************
# Send job failure email
#********************************************
		email_subject = 'Error! ' + py_job_name
		msg={}
		email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' s3 transfer for the file :' + file_name_with_full_path
		msg['default'] = email_body
		msg_json=json.loads(json.dumps(msg))
		print get_curr_date_time(), ': email_subject :',email_subject
		print get_curr_date_time(), ': email_body :',email_body
		print get_curr_date_time(), ': msg :',msg
		print get_curr_date_time(), ': msg_json :',msg_json
		try:
			send_sns_email(email_subject,msg_json)
		except:
			print get_curr_date_time(), ' Failed sending the job failure email.'
		sys.exit(4)
			
#********************************************
# Send job end email
#********************************************
	email_subject = 'Success! ' + py_job_name + ' completed at: ' + current_date
	msg={}
	email_body = py_job_name + ' succeeded at ' + current_date
	msg['default'] = email_body
	msg_json=json.loads(json.dumps(msg))
	print get_curr_date_time(), ': email_subject :',email_subject
	print get_curr_date_time(), ': email_body :',email_body
	print get_curr_date_time(), ': msg :',msg
	print get_curr_date_time(), ': msg_json :',msg_json
	try:
		send_sns_email(email_subject,msg_json)
	except:
		print get_curr_date_time(), ' Failed sending the job success email.'
