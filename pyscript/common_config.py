#!/usr/bin/python3 -tt
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
#      11/04/2017 Kunal Ghosh   Python3 compatible.
#      12/06/2017 Kunal Ghosh   Windows platform Compatible.
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
data_dir=os.path.join(os.sep.join(re.split('/|\\\\+',os.getcwd())[:-3]),'data',env)
#data_dir=os.path.join(os.sep.join(os.getcwd().split('/')[:-3]),'data',env)
#data_dir=os.path.join('/'.join(os.getcwd().split('/')[:-3]),'data',env)
#data_dir=os.path.join('/home/kghosh/data',env)
log_dir=os.path.join(data_dir,'log','')
error_dir=os.path.join(data_dir,'error','')
trigger_dir=os.path.join(data_dir,'trigger','')
increment_dir=os.path.join(data_dir,'increment','')
bulk_dir=os.path.join(data_dir,'bulk','')
#project_dir='/mnt/project/di_de'
#project_dir='/'.join(os.getcwd().split('/')[:-1])
project_dir=os.sep.join(re.split('/|\\\\+',os.getcwd())[:-1])
script_dir=os.path.join(project_dir,'script','')
pyscript_dir=os.path.join(project_dir,'pyscript','')
sql_dir=os.path.join(project_dir,'sql','')
mssql_dsn='Mssql_Prodcopy'
mssql_user='hdpuser'
mssql_password='bigdata'
mssql_database='prodcopy'
redshift_dsn='Amazon_Redshift_x64'
redshift_user='admin'
redshift_password='DevAdmin1'
redshift_database='prodcopy'
f_redshift_copy_credentials=lambda x: 'aws_iam_role=arn:aws:iam::224919220385:role/eRedshiftFinrAdmin' if(x == 'Dev') else ''
redshift_copy_credentials=f_redshift_copy_credentials(env)
netezza_dsn='NZPRDSQL'
netezza_user='kghosh'
netezza_password='Atrayee_07'
netezza_database='ANALYTICS'
s3_region='us-west-1'
s3_extra_args={'ServerSideEncryption': "AES256"}
