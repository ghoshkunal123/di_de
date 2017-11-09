# Copy to s3
After the [Extraction or Consumption](../master/) the next step is to copy the files over to s3. 
Following section would demonstrate the process of moving the files to s3 bucket. 
The highlight of this script is the ability to use the multi-part upload utility to over come each part upload limitations of AWS. 
Essential to note that s3 buckets can look and act like a file system but in essence they are key value store.
