#!/usr/bin/python -tt
##########################################################################
#
#      PROGRAM: dml_against_redshift_table.py
#      PURPOSE: Run DML operations against Redshift table
#
#                                  ****** NOTE ******
#
#                                  ****** NOTE ******
#
#       AUTHOR: Kunal Ghosh
#         DATE: 05/16/2017
#
#   Change Log:
#
#         DATE      PGMR              DESCRIPTION
#      ---------- -----------  ---------------------------------
#      05/16/2017 Kunal Ghosh   Initial Code.
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

	if len(sys.argv[1:]) == 0 or len(sys.argv[1:]) != 1:
		print 'Usage Error: <py_job_name> <sql_file_with_path>'
		print 'Example: ./dml_against_redshift_table.py /mnt/project/di_de/sql/redshift_prodcopy_ods_insert_account_plan_instrument.sql'
		sys.exit(1)

	sql_file_with_path=sys.argv[1]
	py_job_name=py_job_name + '_' + '_'.join(os.path.basename(sql_file_with_path).split('.'))
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
	print get_curr_date_time(), ': sql_file_with_path :',sql_file_with_path
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

#********************************************
# Open the sql file to read content
#********************************************
	try:
		sql_file=open(sql_file_with_path,'r')
		line_count=sum(1 for line in sql_file)
#		sql_file_content=sql_file.read()
	except:
		print get_curr_date_time(), ' Failed to open file or read contents of :' + sql_file_with_path
#********************************************
# Send job failure email
#********************************************
		email_subject = 'Error! ' + py_job_name
		msg={}
		email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' trying to open file or read contents of :' + sql_file_with_path
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
		sys.exit(3)

#********************************************
# Sequential DML operations against Redshift
#********************************************
	sql_file=open(sql_file_with_path,'r')
	for sql_file_content in sql_file.readlines():
		try:
			curr=cnx.cursor()
			curr.execute(sql_file_content)
			curr.execute('commit;')
		except:
			print get_curr_date_time(), ' Failed SQL against Redshift : ',sql_file_content
			email_subject = 'Error! ' + py_job_name
			msg={}
			email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' trying to run ' + sql_file_content + ' against Redshift.'
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
		finally:
			print get_curr_date_time(), ': Completed  running the SQL :' + sql_file_content + ' against Redshift.'
#********************************************
# Close the connection
#********************************************
	cnx.close()
	print get_curr_date_time(), ': Completed  running all the SQLs.'
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
