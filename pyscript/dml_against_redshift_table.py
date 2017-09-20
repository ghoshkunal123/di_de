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
#      09/20/2017 Kunal Ghosh	Including the logging module.
#      09/20/2017 Kunal Ghosh	Added new function send_sns_email_alert.
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

	if len(sys.argv[1:]) == 0 or len(sys.argv[1:]) != 1:
		print 'Usage Error: <py_job_name> <sql_file_with_path>'
		print 'Example: ./dml_against_redshift_table.py /mnt/project/di_de/sql/redshift_prodcopy_ods_insert_account_plan_instrument.sql'
		sys.exit(1)

	sql_file_with_path=sys.argv[1]
	py_job_name=py_job_name + '_' + '_'.join(os.path.basename(sql_file_with_path).split('.'))
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
	logger.info('sql_file_with_path :{}'.format(sql_file_with_path))
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

#********************************************
# Open the sql file to read content
#********************************************
	try:
		sql_file=open(sql_file_with_path,'r')
		line_count=sum(1 for line in sql_file)
#		sql_file_content=sql_file.read()
	except:
		logger.error('Failed to open file or read contents of :{}'.format(sql_file_with_path))
#********************************************
# Send job failure email
#********************************************
		email_subject = 'Error! ' + py_job_name
		email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' trying to open file or read contents of :' + sql_file_with_path
		try:
			send_sns_email_alert(email_subject,email_body)
		except:
			logger.error('Failed sending the job failure email.')
		sys.exit(2)

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
			logger.error('Failed SQL against Redshift : {}'.format(sql_file_content))
			email_subject = 'Error! ' + py_job_name
			email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' trying to run ' + sql_file_content + ' against Redshift.'
			try:
				send_sns_email_alert(email_subject,email_body)
			except:
				logger.error('Failed sending the job failure email.')
			sys.exit(4)
		finally:
			logger.info('Completed  running the SQL :{} against Redshift.'.format(sql_file_content))
#********************************************
# Close the connection
#********************************************
	cnx.close()
	logger.info('Completed  running all the SQLs.')
#********************************************
# Send job end email
#********************************************
	email_subject = 'Success! ' + py_job_name
	email_body = py_job_name + ' succeeded at ' + current_date
	try:
		send_sns_email_alert(email_subject,email_body)
	except:
		logger.error('Failed sending the job success email.')
