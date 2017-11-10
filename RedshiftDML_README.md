# [Copy to Redshift stage](https://confluence.fngn.com/display/DA/Copy+to+Redshift+stage)
After the [Copy to Redshift stage](../master/CopyToRSStage_README.md),this step will transform or load the data to target schema by the use of SQL statements. 
Following section would demonstrate the process of firing DML statements from a SQL file against Redshift table in order to full fill  the transform and load process. 
The script will execute the provided SQLs sequentially in a SQL file in the mentioned location.

| Script Name | Script Location | Sub Module | Purpose |
| ------------|-----------------|------------|---------|
| dml_against_redshift_table.py | di_de/pyscript | from common_function import send_sns_email,create_dir_tree,change_dir_tree_perm | Run the SQL or SQL commands sequentially mentioned in the SQL file against Redshift table |

## Copy to Redshift table
```
<py_job_name> <sql_file_with_path>
Example: ./dml_against_redshift_table.py /mnt/project/di_de/sql/redshift_prodcopy_ods_insert_account_plan_instrument.sql
```
