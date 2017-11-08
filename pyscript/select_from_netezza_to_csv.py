#!/usr/bin/python3 -tt
##########################################################################
#
#      PROGRAM: select_from_netezza_to_csv.py
#      PURPOSE: Extract tables from netezza to csv
#
#                                  ****** NOTE ******
#
#                                  ****** NOTE ******
#
#       AUTHOR: Kunal Ghosh
#         DATE: 04/14/2017
#
#   Change Log:
#
#         DATE      PGMR              DESCRIPTION
#      ---------- -----------  ---------------------------------
#      04/14/2017 Kunal Ghosh   Initial Code.
#      09/19/2017 Kunal Ghosh	Including the logging module.
#      09/19/2017 Kunal Ghosh	Added new function send_sns_email_alert.
#      11/04/2017 Kunal Ghosh   Python3 compatible.
#      11/07/2017 Kunal Ghosh	Create directory if not exists.
#      11/07/2017 Kunal Ghosh   Change directory tree permission.
#      11/07/2017 Kunal Ghosh	Call initial setup.
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
# Iterate over a huge table records 
# (DB memory optimization)
#********************************************

def result_iter(cursor,arraysize):
	while True:
		results=cursor.fetchmany(arraysize)
		if len(results) == 0:
			break
		for result in results:
			yield result

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
# The worker job or function to run each sql
#********************************************
def run_each_sql(sql_line):
	pcurrent_date=datetime.datetime.strftime(datetime.datetime.now(),'%Y%m%d%H%M%S')
	subpname=multiprocessing.current_process().name
#********************************************
# Connecting to netezza
#********************************************
	logger.info('Starting  :{}'.format(multiprocessing.current_process().name))
	cnx_string = 'DSN=%s;UID=%s;PWD=%s;DATABASE=%s;' % (netezza_dsn, netezza_user, netezza_password, netezza_database)
	try:
		cnx=pyodbc.connect(cnx_string)
	except:
		logger.error('Failed connecting to NETEZZA.')
#********************************************
# Send job failure email
#********************************************
		email_subject = 'Error! ' + py_job_name
		email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' trying to connect to NETEZZA for subjob ' + subpname + '.'
		try:
			send_sns_email_alert(email_subject,email_body)
		except:
			logger.error('Failed sending the job failure email.')
		sys.exit(4)

#********************************************
# Run the extract sql and create csv
#********************************************
	try:
		curr=cnx.cursor()
		curr.execute(sql_line)
#		count=curr.rowcount
#		print(get_curr_date_time(), ': Number of rows :',count)
#********************************************
# Changing the delimiter to pipe(|)
#********************************************
		csv_file=open(os.path.join(extract_data_dir, subpname + '.csv'),'w')
#		csv_writer=csv.writer(csv_file,delimiter='|',quoting=csv.QUOTE_ALL)
#		csv_writer=csv.writer(csv_file,delimiter='|',quoting=csv.QUOTE_NONNUMERIC)
		csv_writer=csv.writer(csv_file,delimiter='|',quoting=csv.QUOTE_MINIMAL)
#********************************************
# DB optimization, fetchmany instead of 
# fetchall
#********************************************
#		result=curr.fetchall()
#		for res in result:
#		for res in curr:
		for res in result_iter(curr,10000):
			csv_writer.writerow(res)
		csv_file.close()
	except:
		logger.error('Failed extracting data from netezza.')
		email_subject = 'Error! ' + py_job_name
		email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' trying to run ' + sql_line + ' against netezza for subjob ' + subpname + '.'
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
	logger.info('Completed  :{}'.format(subpname))



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

	if len(sys.argv[1:]) == 0 or len(sys.argv[1:]) != 4:
		print('Usage Error: <py_job_name> <table_name> <file_location> <sql_file_with_path> <one_split? y|n>')
		print('Example: ./select_from_netezza_to_csv.py interaction_fact /mnt/data/Dev/bulk/netezza/analytics/interaction_fact/ /mnt/project/di_de/sql/netezza_analytics_select_interaction_fact.sql y')
		sys.exit(1)

	table_name=sys.argv[1].lower()
	extract_data_dir=sys.argv[2]
	sql_file_with_path=sys.argv[3]
	one_split=sys.argv[4].lower()
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
	netezza_dsn=common_config.netezza_dsn
	netezza_user=common_config.netezza_user
	netezza_password=common_config.netezza_password
	netezza_database=common_config.netezza_database

#********************************************
# Setting up initial directories
#********************************************
	init_setup(data_dir)
	init_setup(log_dir)
	init_setup(trigger_dir)
	init_setup(increment_dir)
	init_setup(bulk_dir)
	init_setup(extract_data_dir)

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
	logger.info('table_name :{}'.format(table_name))
	logger.info('extract_data_dir :{}'.format(extract_data_dir))
	logger.info('sql_file_with_path :{}'.format(sql_file_with_path))
	logger.info('one_split :{}'.format(one_split))
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
	logger.info('netezza_dsn :{}'.format(netezza_dsn))
	logger.info('netezza_user :{}'.format(netezza_user))
	logger.info('netezza_database :{}'.format(netezza_database))

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
# Check if extract data directory exists
#********************************************
	if not os.path.exists(extract_data_dir):
		logger.error('Cannot access :{}'.format(extract_data_dir))
#********************************************
# Send job failure email
#********************************************
		email_subject = 'Error! ' + py_job_name
		email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' cannot access :' + extract_data_dir
		try:
			send_sns_email_alert(email_subject,email_body)
		except:
			logger.error('Failed sending the job failure email.')
		sys.exit(3)

#********************************************
# Parallely extract data from netezza table
#********************************************
	sql_file=open(sql_file_with_path,'r')
	jobs=[]
	count=0
	logger.info('Starting to extract from table {}.'.format(table_name))
	for sql_file_content in sql_file.readlines():
		count = count + 1
		if line_count == 1:
			pname=table_name
		else:
			pname=table_name + '_' + str(count)
		logger.info('Starting :{}'.format(pname))
		j=multiprocessing.Process(name=pname,target=run_each_sql,args=(sql_file_content,))
		jobs.append(j)
		j.start()

	for job in jobs:
		job.join()
		job_name=job.name
		job_exitcode=job.exitcode
		if job_exitcode > 0:
			logger.info('{} was terminated by signal: {}'.format(job_name,job_exitcode))
#********************************************
# Send job failure email
#********************************************
			email_subject = 'Error! ' + job_name
			email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' trying to run: ' + job_name
			try:
				send_sns_email_alert(email_subject,email_body)
			except:
				logger.error('Failed sending the job failure email.')
			sys.exit(6)
		else:
			logger.info('{} returned: {}'.format(job_name,job_exitcode))

	logger.info('Completed extracting from table {}.'.format(table_name))
#********************************************
# Append multiple files to one file
#********************************************
	if one_split == 'y' and line_count > 1:
		try:
			final_csv_file=open(os.path.join(extract_data_dir, table_name + '.csv'),'w')
			for num in range(1,line_count+1):
				csv_file=open(os.path.join(extract_data_dir, table_name + '_' + str(num) + '.csv'),'r')
				for line in csv_file:
					final_csv_file.write(line)
				csv_file.close()
				os.remove(os.path.join(extract_data_dir, table_name + '_' + str(num) + '.csv'))
			final_csv_file.close()
		except:
			logger.error('Failed writing to the final csv file.')
#********************************************
# Send job failure email
#********************************************
			email_subject = 'Error! ' + job_name
			email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' trying to combine multiple files to one.'
			try:
				send_sns_email_alert(email_subject,email_body)
			except:
				logger.error('Failed sending the job failure email.')
			sys.exit(7)

#********************************************
# Send job end email
#********************************************
	email_subject = 'Success! ' + py_job_name
	email_body = py_job_name + ' succeeded at ' + current_date
	try:
		send_sns_email_alert(email_subject,email_body)
	except:
		logger.error('Failed sending the job success email.')
