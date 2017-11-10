# [Extraction or Consumption](https://confluence.fngn.com/display/DA/Extraction+or+Consumption)
The first step would be extract or consume data from sources. Following is a demonstration for extraction from data bases from different DB technologies like MSSQL, Redshift and Netezza. The extraction logic would be SQL. Profiling the data would give a good idea on which key / keys the data distribution would be even for parallel extraction. The trick for big table extraction is to achieve an evenly distributed parallel extract or select process to save it into files.

| Script Name | Script Location | Sub Module | Purpose |
| ------------|-----------------|------------|---------|
| select_from_mssql_to_csv.py | di_de/pyscript | from common_function import send_sns_email,create_dir_tree,change_dir_tree_perm | Extract any table from MSSQL to the specified directory in csv format with pipe delimiter |
| select_from_netezza_to_csv.py | di_de/pyscript |from common_function import send_sns_email,create_dir_tree,change_dir_tree_perm| Extract any table from Netezza to the specified directory in csv format with pipe delimiter |
| select_from_redshift_to_csv.py | di_de/pyscript | from common_function import send_sns_email,create_dir_tree,change_dir_tree_perm| Extract any table from Redshift to the specified directory in csv format with pipe delimiter |

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
