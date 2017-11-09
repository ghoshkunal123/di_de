# Generic data pipeline (ELT) using Python and SQL to Redshift
## Building a data pipeline essentially involves 4 basic steps which are.
1. Extraction or Consumption
2. Copy to s3
3. Copy to Redshift stage
4. Redshift DML operations

### Common configurations and functions
| Script Name | Script Location | Sub Module | Purpose |
| ------------|-----------------|------------|---------|
| common_config.py|di_de/pyscript||Generic script to set environment for data, script, sql, trigger, aws region, encryption method, DSN for databases and others|
