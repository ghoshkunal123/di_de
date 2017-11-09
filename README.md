# [Generic Data Pipeline using Python and SQL](https://confluence.fngn.com/display/DA/Generic+Data+Pipeline+using+Python+and+SQL#?lucidIFH-viewer-2f899c6a=1)
## Building a data pipeline essentially involves 4 basic steps which are.
1. Extraction or Consumption
2. Copy to s3
3. Copy to Redshift stage
4. Redshift DML operations

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


### [Common configurations and functions](https://confluence.fngn.com/display/DA/Common+configurations+and+functions)
| Script Name | Script Location | Sub Module | Purpose |
| ------------|-----------------|------------|---------|
| common_config.py|di_de/pyscript||Generic script to set environment for data, script, sql, trigger, aws region, encryption method, DSN for databases and others|
| common_function.py | di_de/pyscript | send_sns_email | Send email alerts to end points subscribed in AWS SNS service |
| common_function.py | di_de/pyscript | local_to_utc | Convert local time to utc time |
| common_function.py | di_de/pyscript | send_email | Send email using the corporate email server |
| common_function.py | di_de/pyscript | create_dir_tree | Create a directory tree just mkdir -p in unix |
| common_function.py | di_de/pyscript | change_dir_tree_perm | Change the directory tree and its content permission |

### [Extraction or Consumption](https://confluence.fngn.com/display/DA/Extraction+or+Consumption)
| Script Name | Script Location | Sub Module | Purpose |
| ------------|-----------------|------------|---------|
| select_from_mssql_to_csv.py | di_de/pyscript | from common_function import send_sns_email,create_dir_tree,change_dir_tree_perm | Extract any table from MSSQL to the specified directory in csv format with '\|' delimiter |
| select_from_netezza_to_csv.py | di_de/pyscript |from common_function import send_sns_email,create_dir_tree,change_dir_tree_perm| Extract any table from Netezza to the specified directory in csv format with '\|' delimiter |
| select_from_redshift_to_csv.py | di_de/pyscript | from common_function import send_sns_email,create_dir_tree,change_dir_tree_perm| Extract any table from Redshift to the specified directory in csv format with '\|' delimiter |

#### Extraction Script Usage
```
<py_job_name> <table_name> <file_location> <sql_file_with_path> <one_split? y|n>
Example: ./select_from_mssql_to_csv.py table_name /path/to/data/ /path/to/sql/mssql_dbname_select_table_name.sql y
```
