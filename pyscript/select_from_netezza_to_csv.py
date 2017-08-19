#!/usr/bin/python -tt
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
# The worker job or function to run each sql
#********************************************
def run_each_sql(sql_line):
#********************************************
# Setting up the netezza logging 
# (commented it out as it is not writing need to investigate)
#********************************************
	pcurrent_date=datetime.datetime.strftime(datetime.datetime.now(),'%Y%m%d%H%M%S')
	subpname=multiprocessing.current_process().name
	dblogfile=py_job_name + '_netezza_py_connect_' + subpname + '_' + pcurrent_date + '.log'
	print get_curr_date_time(), ': current_date :',current_date
	print get_curr_date_time(), ': subpname :',subpname
	print get_curr_date_time(), ': dblogfile :',dblogfile
	try:
		logging.basicConfig(filename=log_dir + dblogfile,level=logging.INFO)
	except:
		print get_curr_date_time(), ' Failed setting up logging.'
#********************************************
# Send job failure email
#********************************************
		email_subject = 'Error! ' + py_job_name
		msg={}
		email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' trying to set up logging for netezza for job_name.' + job_name
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
# Connecting to netezza
#********************************************
	print get_curr_date_time(), ': Starting  :',multiprocessing.current_process().name
	cnx_string = 'DSN=%s;UID=%s;PWD=%s;DATABASE=%s;' % (netezza_dsn, netezza_user, netezza_password, netezza_database)
	try:
		cnx=pyodbc.connect(cnx_string)
	except:
		print get_curr_date_time(), ' Failed connecting to NETEZZA.'
#********************************************
# Send job failure email
#********************************************
		email_subject = 'Error! ' + py_job_name
		msg={}
		email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' trying to connect to NETEZZA for subjob ' + subpname + '.'
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
# Run the extract sql and create csv
#********************************************
	try:
		curr=cnx.cursor()
		curr.execute(sql_line)
#		count=curr.rowcount
#		print get_curr_date_time(), ': Number of rows :',count
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
		print get_curr_date_time(), ' Failed extracting data from netezza.'
		email_subject = 'Error! ' + py_job_name
		msg={}
		email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' trying to run ' + sql_line + ' against netezza for subjob ' + subpname + '.'
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
	print get_curr_date_time(), ': Completed  :',subpname



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

	if len(sys.argv[1:]) == 0 or len(sys.argv[1:]) != 4:
		print 'Usage Error: <py_job_name> <table_name> <file_location> <sql_file_with_path> <one_split? y|n>'
		print 'Example: ./select_from_netezza_to_csv.py interaction_fact /mnt/data/Dev/bulk/netezza/analytics/interaction_fact/ /mnt/project/di_de/sql/netezza_analytics_select_interaction_fact.sql y'
		sys.exit(1)

	table_name=sys.argv[1].lower()
	extract_data_dir=sys.argv[2]
	sql_file_with_path=sys.argv[3]
	one_split=sys.argv[4].lower()
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
	netezza_dsn=common_config.netezza_dsn
	netezza_user=common_config.netezza_user
	netezza_password=common_config.netezza_password
	netezza_database=common_config.netezza_database

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
	print get_curr_date_time(), ': table_name :',table_name
	print get_curr_date_time(), ': extract_data_dir :',extract_data_dir
	print get_curr_date_time(), ': sql_file_with_path :',sql_file_with_path
	print get_curr_date_time(), ': one_split :',one_split
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
	print get_curr_date_time(), ': netezza_dsn :',netezza_dsn
	print get_curr_date_time(), ': netezza_user :',netezza_user
	print get_curr_date_time(), ': netezza_database :',netezza_database

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
# Check if extract data directory exists
#********************************************
	if not os.path.exists(extract_data_dir):
		print get_curr_date_time(), ' Cannot access :' + extract_data_dir
#********************************************
# Send job failure email
#********************************************
		email_subject = 'Error! ' + py_job_name
		msg={}
		email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' cannot access :' + extract_data_dir
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
# Parallely extract data from netezza table
#********************************************
	sql_file=open(sql_file_with_path,'r')
	jobs=[]
	count=0
	print get_curr_date_time(), ': Starting to extract from table ' + table_name + '.'
	for sql_file_content in sql_file.readlines():
		count = count + 1
		if line_count == 1:
			pname=table_name
		else:
			pname=table_name + '_' + str(count)
		print get_curr_date_time(), ': Starting :',pname
		j=multiprocessing.Process(name=pname,target=run_each_sql,args=(sql_file_content,))
		jobs.append(j)
		j.start()

	for job in jobs:
		job.join()
		job_name=job.name
		job_exitcode=job.exitcode
		if job_exitcode > 0:
			print get_curr_date_time(), ': ' + job_name + ' was terminated by signal: ',job_exitcode
#********************************************
# Send job failure email
#********************************************
			email_subject = 'Error! ' + job_name
			msg={}
			email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' trying to run: ' + job_name
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
			sys.exit(6)
		else:
			print get_curr_date_time(), ': ' + job_name + ' returned: ', job_exitcode

	print get_curr_date_time(), ': Completed extracting from table ' + table_name + '.'
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
			print get_curr_date_time(), ' Failed writing to the final csv file.'
#********************************************
# Send job failure email
#********************************************
			email_subject = 'Error! ' + job_name
			msg={}
			email_body = py_job_name + ' job has failed at ' + get_curr_date_time() + ' trying to combine multiple files to one.'
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
			sys.exit(7)

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
