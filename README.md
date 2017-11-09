# [Generic Data Pipeline using Python and SQL](https://confluence.fngn.com/display/DA/Generic+Data+Pipeline+using+Python+and+SQL#?lucidIFH-viewer-2f899c6a=1)

## Get Started
### [Clone project](https://github.com/ghoshkunal123/di_de)
### Update the di_de/pyscript/common_config.py file with DB connection and data_dir

### Setup Assumptions
| Assumptions | How To |
|-------------|--------|
|Execution in UNIX environment||
|ODBC Setup for MSSQL,Netezza, Redshift| [Comming Soon] |
|Necessary IAM role setup for S3 and Redshift operations||
|DB Access||
|Python3||
|Python3 libraries sys,os,datetime,logging,pyodbc,csv,multiprocessing,json,logging,boto3,botocore,subprocess|sudo apt install python3-pip\; sudo -H pip3 install **library**|
|Encrypted s3 bucket with AES256 bit encryption|[Comming Soon]|

### [Common configurations and functions](https://confluence.fngn.com/display/DA/Common+configurations+and+functions)
| Script Name | Script Location | Sub Module | Purpose |
| ------------|-----------------|------------|---------|
| common_config.py|di_de/pyscript||Generic script to set environment for data, script, sql, trigger, aws region, encryption method, DSN for databases and others|
| common_function.py | di_de/pyscript | send_sns_email | Send email alerts to end points subscribed in AWS SNS service |
| common_function.py | di_de/pyscript | local_to_utc | Convert local time to utc time |
| common_function.py | di_de/pyscript | send_email | Send email using the corporate email server |
| common_function.py | di_de/pyscript | create_dir_tree | Create a directory tree just mkdir -p in unix |
| common_function.py | di_de/pyscript | change_dir_tree_perm | Change the directory tree and its content permission |

## Building a data pipeline essentially involves 4 basic steps which are.
1. [Extraction or Consumption](../master/Extraction_README.md)
2. [Copy to s3](../master/CopyTos3_README.md)
3. [Copy to Redshift stage](../master/CopyToRSStage_README.md)
4. [Redshift DML operations](../master/RedshiftDML_README.md)
