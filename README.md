## aws_utils

Lightweight python wrapper for AWS utilities. Currently only S3 and EMR can be used through this script. Contains two methods - 
 - AwsS3Helper : Amazon Simple Storage Service (S3) utility to upload and download files to and from S3.
 - AwsEmrHelper :  Amazon Elastic Map Reduce (EMR) utility to create clusters, add job steps and run job flows programatically.


### Requirements

Both the methods require `boto3` to execute. To download boto3: 

```bash
$ pip install boto3
```

### Usage

Refer to `example.py` and docstrings in `boto3_utils.py` for usage.

### Note

This work is free. You can redistribute it and/or modify it under the terms of the Do Whatever You Want To Public License.