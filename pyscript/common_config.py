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
data_dir='/mnt/data' + '/' + env
log_dir=data_dir + '/log/'
error_dir=data_dir + '/error/'
trigger_dir=data_dir + '/trigger/'
increment_dir='increment'
bulk_dir='bulk'
project_dir='/'.join(os.getcwd().split('/')[:-1])
script_dir=project_dir + '/script/'
pyscript_dir=project_dir + '/pyscript/'
sql_dir=project_dir + '/sql/'
mssql_dsn='Mssql_Prodcopy'
mssql_user=
mssql_password=
mssql_database='prodcopy'
redshift_dsn='Amazon_Redshift_x64'
redshift_user=
redshift_password=
redshift_database='prodcopy'
netezza_dsn='NZPRDSQL'
netezza_user=
netezza_password=
netezza_database='ANALYTICS'
s3_region='us-west-1'
s3_extra_args={'ServerSideEncryption': "AES256"}
