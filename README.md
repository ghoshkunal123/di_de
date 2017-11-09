# Generic data pipeline (ELT) using Python and SQL to Redshift
## Building a data pipeline essentially involves 4 basic steps which are.
1. Extraction or Consumption
2. Copy to s3
3. Copy to Redshift stage
4. Redshift DML operations

[Read about Generic Data Pipeline using Python and SQL](https://confluence.fngn.com/display/DA/Generic+Data+Pipeline+using+Python+and+SQL#?lucidIFH-viewer-2f899c6a=1)

### Common configurations and functions
| Script Name | Script Location | Sub Module | Purpose |
| ------------|-----------------|------------|---------|
| common_config.py|di_de/pyscript||Generic script to set environment for data, script, sql, trigger, aws region, encryption method, DSN for databases and others|
| common_function.py | di_de/pyscript | send_sns_email | Send email alerts to end points subscribed in AWS SNS service |
| common_function.py | di_de/pyscript | local_to_utc | Convert local time to utc time |
| common_function.py | di_de/pyscript | send_email | Send email using the corporate email server |
| common_function.py | di_de/pyscript | create_dir_tree | Create a directory tree just mkdir -p in unix |
| common_function.py | di_de/pyscript | change_dir_tree_perm | Change the directory tree and its content permission |

[Read about Common configurations and functions](https://confluence.fngn.com/display/DA/Common+configurations+and+functions)