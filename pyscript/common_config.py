#!/usr/bin/python -tt
##########################################################################
#
#      PROGRAM: common_config.py
#      PURPOSE: Common configuration for the python jobs
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
#      08/01/2017 Kunal Ghosh	Blank strings for connection details.
#
##########################################################################

#********************************************
# All the import statements go here
#********************************************
import os
 
#********************************************
# Setting up common variable values
#********************************************
env='Dev'
data_dir=os.path.join('/path/to/data',env)
log_dir=os.path.join(data_dir,'log','')
error_dir=os.path.join(data_dir,'error','')
trigger_dir=os.path.join(data_dir,'trigger','')
increment_dir='increment'
bulk_dir='bulk'
#project_dir='/mnt/project/di_de'
project_dir='/'.join(os.getcwd().split('/')[:-1])
script_dir=os.path.join(project_dir,'script','')
pyscript_dir=os.path.join(project_dir,'pyscript','')
sql_dir=os.path.join(project_dir,'sql','')
mssql_dsn='Mssql_Prodcopy'
mssql_user=''
mssql_password=''
mssql_database=''
redshift_dsn='Amazon_Redshift_x64'
redshift_user=''
redshift_password=''
redshift_database=''
f_redshift_copy_credentials=lambda x: 'aws_iam_role=arn:aws:iam::224919220385:role/eRedshiftFinrAdmin' if(x == 'Dev') else ''
redshift_copy_credentials=f_redshift_copy_credentials(env)
netezza_dsn='NZPRDSQL'
netezza_user=''
netezza_password=''
netezza_database=''
s3_region='us-west-1'
s3_extra_args={'ServerSideEncryption': "AES256"}
