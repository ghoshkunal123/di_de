# [Extraction or Consumption](https://confluence.fngn.com/display/DA/Extraction+or+Consumption)
| Script Name | Script Location | Sub Module | Purpose |
| ------------|-----------------|------------|---------|
| select_from_mssql_to_csv.py | di_de/pyscript | from common_function import send_sns_email,create_dir_tree,change_dir_tree_perm | Extract any table from MSSQL to the specified directory in csv format with '\|' delimiter |
| select_from_netezza_to_csv.py | di_de/pyscript |from common_function import send_sns_email,create_dir_tree,change_dir_tree_perm| Extract any table from Netezza to the specified directory in csv format with '\|' delimiter |
| select_from_redshift_to_csv.py | di_de/pyscript | from common_function import send_sns_email,create_dir_tree,change_dir_tree_perm| Extract any table from Redshift to the specified directory in csv format with '\|' delimiter |

## Extraction Script Usage for MSSQL
```
<py_job_name> <table_name> <file_location> <sql_file_with_path> <one_split? y|n>
Example: ./select_from_mssql_to_csv.py table_name /path/to/data/ /path/to/sql/mssql_dbname_select_table_name.sql y
```
## Extraction Script Usage for Netezza
```
<py_job_name> <table_name> <file_location> <sql_file_with_path> <one_split? y|n>
Example: ./select_from_netezza_to_csv.py table_name /path/to/data/ /path/to/sql/netezza_dbname_select_table_name.sql y
```

## Extraction Script Usage for Redshift
```
<py_job_name> <table_name> <file_location> <sql_file_with_path> <one_split? y|n>
Example: ./select_from_redshift_to_csv.py table_name /path/to/data/ /path/to/sql/redshift_dbname_select_table_name.sql y
```
