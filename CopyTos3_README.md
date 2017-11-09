# Copy to s3
After the [Extraction or Consumption](../master/Extraction_README.md) the next step is to copy the files over to s3. 
Following section would demonstrate the process of moving the files to s3 bucket. 
The highlight of this script is the ability to use the [multi-part upload utility](https://aws.amazon.com/blogs/aws/amazon-s3-multipart-upload/) to over come each part upload limitations of AWS. 
Essential to note that s3 buckets can look and act like a file system but in essence they are key value store.

| Script Name | Script Location | Sub Module | Purpose |
| ------------|-----------------|------------|---------|
| copy_from_ec2_to_s3.py | di_de/pyscript | from common_function import send_sns_email,create_dir_tree,change_dir_tree_perm | Copy any file or directory from ec2 file system to the specified bucket location in s3 |

## Copy to s3 usage
```
<py_job_name> <bucket_name> <file_name_with_full_path> <bucket_prefix>
Example: ./copy_from_ec2_to_s3.py some-s3-bucket /path/to/some_file.csv prefix/path/
OR
<py_job_name> <bucket_name> <dir_name_with_full_path> <bucket_prefix>
Example: ./copy_from_ec2_to_s3.py some-s3-bucket /path/to/ prefix/path/
```
