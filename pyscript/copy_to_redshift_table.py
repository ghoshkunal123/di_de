#!/usr/bin/python -tt
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
#
##########################################################################

#********************************************
# All the import statements go here
#********************************************
# encoding=utf8
import sys
reload(sys)
sys.setdefaultencoding('utf8')
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

	if len(sys.argv[1:]) == 0 or len(sys.argv[1:]) != 10:
		print 'Usage Error: <py_job_name> <db_name> <schema_name> <table_name> <bucket_name> <bucket_prefix> <file_name> <delimiter> <file_format> <truncate_before y|n> <accept_inv_chars y|n>'
		print 'Example: ./copy_to_redshift_table.py prodcopy stage account_plan_instrument fe-finr-da Dev/bulk/mssql/prodcopy/account_plan_instrument/ na \'|\' csv y y'
		print 'OR'
		print 'Example: ./copy_to_redshift_table.py prodcopy stage account_plan_instrument fe-finr-da Dev/bulk/mssql/prodcopy/account_plan_instrument/ account_plan_instrument.csv \'|\' csv y y'
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
	errorfile=py_job_name + '_' + current_date + '.err'
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
	print get_curr_date_time(), ': db_name :',db_name
	print get_curr_date_time(), ': schema_name :',schema_name
	print get_curr_date_time(), ': table_name :',table_name
	print get_curr_date_time(), ': bucket_name :',bucket_name
	print get_curr_date_time(), ': bucket_prefix :',bucket_prefix
	print get_curr_date_time(), ': file_name :',file_name
	print get_curr_date_time(), ': delimiter :',delimiter
	print get_curr_date_time(), ': file_format :',file_format
	print get_curr_date_time(), ': truncate_before :',truncate_before
	print get_curr_date_time(), ': accept_inv_chars :',accept_inv_chars
	print get_curr_date_time(), ': py_job_name :',py_job_name
	print get_curr_date_time(), ': logfile :',logfile
	print get_curr_date_time(), ': errorfile :',errorfile
	print get_curr_date_time(), ': data_dir :',data_dir
	print get_curr_date_time(), ': trigger_dir :',trigger_dir
	print get_curr_date_time(), ': increment_dir :',increment_dir
	print get_curr_date_time(), ': bulk_dir :',bulk_dir
	print get_curr_date_time(), ': project_dir :',project_dir
	print get_curr_date_time(), ': script_dir :',script_dir
	print get_curr_date_time(), ': pyscript_dir :',pyscript_dir
	print get_curr_date_time(), ': sql_dir :',sql_dir
	print get_curr_date_time(), ': redshift_dsn :',redshift_dsn
	print get_curr_date_time(), ': redshift_user :',redshift_user
	print get_curr_date_time(), ': redshift_database :',redshift_database
	print get_curr_date_time(), ': redshift_copy_credentials :',redshift_copy_credentials
	print get_curr_date_time(), ': s3_full_path :',s3_full_path
	print get_curr_date_time(), ': s3_list_cmd :',s3_list_cmd
	print get_curr_date_time(), ': truncate_sql :',truncate_sql
	print get_curr_date_time(), ': copy_sql :',copy_sql

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
		sys.exit(2)

#********************************************
# Check for the presence of s3 objects
#********************************************
	sub_process=subprocess.Popen(s3_list_cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
	sub_process.wait()
	(std_out,std_err)=sub_process.communicate()
	retcode=sub_process.returncode
	if(retcode > 0):
		print get_curr_date_time(), ' Failed to s3 objects in location :' + s3_full_path
#********************************************
# Send job failure email
#********************************************
		email_subject = 'Error! ' + py_job_name
		msg={}
		email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' finding s3 objects in location :' + s3_full_path
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
# Connecting to redshift
#********************************************
	print get_curr_date_time(), ': Starting to connect.'
	cnx_string = 'DSN=%s;UID=%s;PWD=%s;DATABASE=%s;' % (redshift_dsn, redshift_user, redshift_password, redshift_database)
	try:
		cnx=pyodbc.connect(cnx_string)
	except:
		print get_curr_date_time(), ' Failed connecting to Redshift.'
#********************************************
# Send job failure email
#********************************************
		email_subject = 'Error! ' + py_job_name
		msg={}
		email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' trying to connect to Redshift.'
		msg['default'] = email_body
		msg_json=json.loads(json.dumps(msg))
		print get_curr_date_time(), ': email_subject :',email_subject
		print get_curr_date_time(), ': email_body :',email_body
		print get_curr_date_time(), ': msg :',msg
		print get_curr_date_time(), ': msg_json :',msg_json
		try:
			send_email(email_subject,msg_json)
		except:
			print get_curr_date_time(), ' Failed sending the job failure email.'
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
		print get_curr_date_time(), ' Failed Copy SQL against Redshift : ',copy_sql
		email_subject = 'Error! ' + py_job_name
		msg={}
		email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' trying to run ' + copy_sql + ' against Redshift.'
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
		sys.exit(5)
#********************************************
# Close the connection
#********************************************
	finally:
		cnx.close()
	print get_curr_date_time(), ': Completed  running the Copy SQL :',copy_sql
#********************************************
# Send job end email
#********************************************
	email_subject = 'Success! ' + py_job_name
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
