# [Copy to Redshift stage](https://confluence.fngn.com/display/DA/Copy+to+Redshift+stage)
After the [Copy to s3](../master/CopyTos3_README.md) the next step would be copy them over to Redshift stage. 
Following section would demonstrate the process of copying of file or files from s3 bucket to Redshift stage. 
The script can copy one or multiple files to achieve concurrency if desired it also uses instance profile instead of hard coded keys while the files in s3 and data in Redshift is also encrypted.

| Script Name | Script Location | Sub Module | Purpose |
| ------------|-----------------|------------|---------|
| copy_to_redshift_table.py | di_de/pyscript | from common_function import send_sns_email,create_dir_tree,change_dir_tree_perm | Copy a file or files of specified delimiter and format from s3 to Redshift table |

## Copy to Redshift table
```
<py_job_name> <db_name> <schema_name> <table_name> <bucket_name> <bucket_prefix> <file_name> <delimiter> <file_format> <truncate_before y|n> <accept_inv_chars y|n>
Example: ./copy_to_redshift_table.py prodcopy stage account_plan_instrument fe-finr-da Dev/bulk/mssql/prodcopy/account_plan_instrument/ na '|' csv y y
OR
Example: ./copy_to_redshift_table.py prodcopy stage account_plan_instrument fe-finr-da Dev/bulk/mssql/prodcopy/account_plan_instrument/ account_plan_instrument.csv '|' csv y n
```
