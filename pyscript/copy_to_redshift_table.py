#!/usr/bin/python3 -tt
##########################################################################
#
#      PROGRAM: copy_to_redshift_table.py
#      PURPOSE: Run copy to Redshift tables from s3 bucket
#
#                                  ****** NOTE ******
#
#                                  ****** NOTE ******
#
#       AUTHOR: Kunal Ghosh
#         DATE: 05/09/2017
#
#   Change Log:
#
#         DATE      PGMR              DESCRIPTION
#      ---------- -----------  ---------------------------------
#      05/09/2017 Kunal Ghosh   Initial Code.
#      08/17/2017 Kunal Ghosh   Include mandatory option to accept acceptinvchars
#      08/18/2017 Kunal Ghosh   Ask option to accept acceptinvchars
#      09/20/2017 Kunal Ghosh	Including the logging module.
#      09/20/2017 Kunal Ghosh	Added new function send_sns_email_alert.
#      11/04/2017 Kunal Ghosh	Python3 compatible.
#
##########################################################################

#********************************************
# All the import statements go here
#********************************************
import sys
import os
import sys
import datetime
import logging
import pyodbc
import csv
import multiprocessing
import json
import boto3
import botocore
import subprocess


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
		logger.error('Failed sending the job start email.')

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

	if len(sys.argv[1:]) == 0 or len(sys.argv[1:]) != 10:
		print('Usage Error: <py_job_name> <db_name> <schema_name> <table_name> <bucket_name> <bucket_prefix> <file_name> <delimiter> <file_format> <truncate_before y|n> <accept_inv_chars y|n>')
		print('Example: ./copy_to_redshift_table.py prodcopy stage account_plan_instrument fe-finr-da Dev/bulk/mssql/prodcopy/account_plan_instrument/ na \'|\' csv y y')
		print('OR')
		print('Example: ./copy_to_redshift_table.py prodcopy stage account_plan_instrument fe-finr-da Dev/bulk/mssql/prodcopy/account_plan_instrument/ account_plan_instrument.csv \'|\' csv y y')
		sys.exit(1)

	db_name=sys.argv[1].lower()
	schema_name=sys.argv[2].lower()
	table_name=sys.argv[3].lower()
	bucket_name=sys.argv[4]
	bucket_prefix=sys.argv[5]
	file_name='' if ( sys.argv[6].lower() == 'na' ) else sys.argv[6]
	delimiter=sys.argv[7]
	file_format=sys.argv[8]
	truncate_before=sys.argv[9].lower()
	accept_inv_chars=sys.argv[10].lower()
	py_job_name=py_job_name + '_' + db_name + '_' + schema_name + '_' + table_name
	logfile=py_job_name + '_' +  current_date + '.log'
	data_dir=common_config.data_dir
	trigger_dir=common_config.trigger_dir
	increment_dir=common_config.increment_dir
	bulk_dir=common_config.bulk_dir
	project_dir=common_config.project_dir
	script_dir=common_config.script_dir
	pyscript_dir=common_config.pyscript_dir
	sql_dir=common_config.sql_dir
	redshift_dsn=common_config.redshift_dsn
	redshift_user=common_config.redshift_user
	redshift_password=common_config.redshift_password
	redshift_database=common_config.redshift_database
	redshift_copy_credentials=common_config.redshift_copy_credentials
	s3_full_path=os.path.join('s3://',bucket_name,bucket_prefix,file_name)
	s3_list_cmd='aws s3 ls ' + s3_full_path
	truncate_sql='truncate table ' + db_name + '.' + schema_name + '.' + table_name + ';'
	if ( accept_inv_chars == 'y' ):
		copy_sql='copy ' + db_name + '.' + schema_name + '.' + table_name + ' from \'' + s3_full_path + '\' credentials \'' + redshift_copy_credentials + '\' delimiter \'' + delimiter + '\' ' + file_format + ' acceptinvchars;'
	else:
		copy_sql='copy ' + db_name + '.' + schema_name + '.' + table_name + ' from \'' + s3_full_path + '\' credentials \'' + redshift_copy_credentials + '\' delimiter \'' + delimiter + '\' ' + file_format + ';'

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
		logger.error('Failed sending the job start email.')

	logger.info('log_dir :{}'.format(log_dir))
	logger.info('current_date :{}'.format(current_date))
	logger.info('job_dir :{}'.format(job_dir))
	logger.info('db_name :{}'.format(db_name))
	logger.info('schema_name :{}'.format(schema_name))
	logger.info('table_name :{}'.format(table_name))
	logger.info('bucket_name :{}'.format(bucket_name))
	logger.info('bucket_prefix :{}'.format(bucket_prefix))
	logger.info('file_name :{}'.format(file_name))
	logger.info('delimiter :{}'.format(delimiter))
	logger.info('file_format :{}'.format(file_format))
	logger.info('truncate_before :{}'.format(truncate_before))
	logger.info('accept_inv_chars :{}'.format(accept_inv_chars))
	logger.info('py_job_name :{}'.format(py_job_name))
	logger.info('logfile :{}'.format(logfile))
	logger.info('data_dir :{}'.format(data_dir))
	logger.info('trigger_dir :{}'.format(trigger_dir))
	logger.info('increment_dir :{}'.format(increment_dir))
	logger.info('bulk_dir :{}'.format(bulk_dir))
	logger.info('project_dir :{}'.format(project_dir))
	logger.info('script_dir :{}'.format(script_dir))
	logger.info('pyscript_dir :{}'.format(pyscript_dir))
	logger.info('sql_dir :{}'.format(sql_dir))
	logger.info('redshift_dsn :{}'.format(redshift_dsn))
	logger.info('redshift_user :{}'.format(redshift_user))
	logger.info('redshift_database :{}'.format(redshift_database))
	logger.info('redshift_copy_credentials :{}'.format(redshift_copy_credentials))
	logger.info('s3_full_path :{}'.format(s3_full_path))
	logger.info('s3_list_cmd :{}'.format(s3_list_cmd))
	logger.info('truncate_sql :{}'.format(truncate_sql))
	logger.info('copy_sql :{}'.format(copy_sql))

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
		sys.exit(2)

#********************************************
# Check for the presence of s3 objects
#********************************************
	sub_process=subprocess.Popen(s3_list_cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
	sub_process.wait()
	(std_out,std_err)=sub_process.communicate()
	retcode=sub_process.returncode
	if(retcode > 0):
		logger.error('Failed to s3 objects in location :{}'.format(s3_full_path))
#********************************************
# Send job failure email
#********************************************
		email_subject = 'Error! ' + py_job_name
		email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' finding s3 objects in location :' + s3_full_path
		try:
			send_sns_email_alert(email_subject,email_body)
		except:
			logger.error('Failed sending the job failure email.')
		sys.exit(3)

#********************************************
# Connecting to redshift
#********************************************
	logger.info('Starting to connect.')
	cnx_string = 'DSN=%s;UID=%s;PWD=%s;DATABASE=%s;' % (redshift_dsn, redshift_user, redshift_password, redshift_database)
	try:
		cnx=pyodbc.connect(cnx_string)
	except:
		logger.error('Failed connecting to Redshift.')
#********************************************
# Send job failure email
#********************************************
		email_subject = 'Error! ' + py_job_name
		email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' trying to connect to Redshift.'
		try:
			send_sns_email_alert(email_subject,email_body)
		except:
			logger.error('Failed sending the job failure email.')
		sys.exit(4)

#********************************************
# Copy the file to Redshift from s3
#********************************************
	try:
		curr=cnx.cursor()
		if ( truncate_before == 'y' ):
			curr.execute(truncate_sql)
		curr.execute(copy_sql)
		curr.execute('commit;')
	except:
		logger.error('Failed Copy SQL against Redshift : {}'.format(copy_sql))
		email_subject = 'Error! ' + py_job_name
		email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' trying to run ' + copy_sql + ' against Redshift.'
		try:
			send_sns_email_alert(email_subject,email_body)
		except:
			logger.error('Failed sending the job failure email.')
		sys.exit(5)
#********************************************
# Close the connection
#********************************************
	finally:
		cnx.close()
	logger.info('Completed  running the Copy SQL :{}'.format(copy_sql))
#********************************************
# Send job end email
#********************************************
	email_subject = 'Success! ' + py_job_name
	email_body = py_job_name + ' succeeded at ' + current_date
	try:
		send_sns_email_alert(email_subject,email_body)
	except:
		logger.error('Failed sending the job success email.')
