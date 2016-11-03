## AwsUtils

Lightweight python wrapper for AWS utilities. Currently only S3 and EMR can be used through this script. Contains two methods - 
 - AwsS3Helper : Amazon Simple Storage Service (S3) utility to upload and download files to and from S3, can also be used to list files from a S3 path.
 - AwsEmrHelper :  Amazon Elastic Map Reduce (EMR) utility to create clusters, add job steps and run Map Reduce job flows programatically. This is only the wrapper to deploy MR jobs. To see, how to write a MR job in python, [click here](https://github.com/amberm291/MatrixMultiplyMR)

### Requirements

Both the methods require `boto3` to execute. To download boto3: 

```bash
$ pip install boto3
```

### Usage

#### AwsS3Helper

To instantiate this method, pass the access key and secret key provided by the AWS account to the method as parameters. Also, providing the region name is optional, by default it is set to 'ap-southeast-1'. The below snippet shows how to create an instance of AwsS3Helper.

```bash
>>> AWS_ACCESS_KEY_ID = 'abc'
>>> AWS_SECRET_ACCESS_KEY = 'xyz'
>>> region_name = 'us-southwest-1'
>>> conn_s3 = AwsS3Helper(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, region_name=region_name)
```

Files can be uploaded, downloaded, deleted and copied from one location to another like this

```bash
>>> conn_s3.upload_file('s3_bucket/s3_path/file.txt','/home/abc/local_dir/file.txt')
>>> conn_s3.download_file('s3_bucket/s3_path/file.txt','/home/abc/local_dir/file.txt')
>>> conn_s3.del_file('s3_bucket/s3_path/file.txt')
>>> copy_key('s3_bucket1/some_path/fname.txt','s3_bucket2/some_path2/fname2.txt')
```

To list files in a S3 path, use the following function

```bash
>>> conn_s3.list_keys('s3_bucket/s3_path/')
```

will output this

```bash
['s3_bucket/s3_path/','s3_bucket/s3_path/1.txt','s3_bucket/s3_path/2.txt']
```

The output is all the files and folders inside the path, including the current path also.

#### AwsEmrHelper

This method is instantiated the same way as AwsS3Helper above. To use this method, input files and output path should be in S3.

```bash
>>> conn_emr = AwsEmrHelper(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, region_name=region_name)
```

A MR job can be launched on EMR in three simple steps using this method

- **Specify input, output paths, mappers and reducers**: Specify the S3 input and output paths. The following code snippet gives an example of this:

```bash
>>> input_path = 'input_bucket/input_path/'   
>>> output_path = 'output_bucket/output_bucket/'   
>>> mapper_fname = 'map.py'
>>> reducer_fname = 'reduce.py'
>>> local_src_path = '/home/path/src/'  
>>> code_path = 'code_bucket/src_files/'    
>>> step_name = 'My Step'  
>>> conn_emr.add_job_step(step_name, input_path, output_path, code_path + mapper_fname, local_src_path + mapper_fname, \\
... reducer_path=code_path + reducer_fname, reducer_fname=code_path + reducer_fname, del_existing_path=True)
```

`input_path` could either be a string containing a single S3 path or it could be a list of paths. The mapper source file needs to stored on the local machine and a S3 path should be provided where it can be uploaded to. The argument `code_path + mapper_fname` in `add_job_step` is the S3 path where the mapper will be uploaded. The argument `code_path + mapper_fname` is the local path where the mapper is stored. `reducer_path` and `reducer_fname` are optional parameters similar to the ones for mapper. They are optional since a reducer is not always required. `del_existing_path` is a boolean value, by default set to `False`. Hadoop requires that the output path for a job, should not exist, before the job starts. So setting the `del_existing_path` flag to `True` deletes the output path. Be careful with this flag, since it will delete all the contents of the output path.

Cache files can also be passed to `add_job_step`:

```bash
>>> cache_files = ['/home/cache_files/dict.p','/home/cache_files/list.p'] 
>>> cache_loc = 'code_bukcet/cache_files/'
>>> conn_emr.add_job_step(step_name, input_path, output_path, code_path + mapper_fname, local_src_path + mapper_fname,\\
... reducer_path=code_path + reducer_fname, reducer_fname=code_path + reducer_fname, cache_files=cache_files, \\
... cache_loc=cache_loc, del_existing_path=True)
```

`cache_loc` and `cache_files` are optional parameters. `cache_files` should be a list of local paths of cache files, `cache_loc` is the S3 path where the cache files will be uploaded.

- **Specify number of machines and the type of machines**: AWS has the concept of instance types to classify the computing power of their machines. To know, more about them, check out their instance type [page](https://aws.amazon.com/ec2/instance-types/).

The following is an example to add instance types, using the function `add_instance` of `AwsEmrHelper`:

```bash
>>> instance_config = {"MASTER":{"instance_type":"m1.medium","num_instances":1,"market":"ON_DEMAND","name":"Main Nodes"},\\
... "CORE":{"instance_type":"r3.xlarge","num_instances":1,"market":"ON_DEMAND","name":"Worker Nodes"},\\
... "TASK":{"instance_type":"r3.xlarge","num_instances":1,"market":"SPOT","name":"Worker Nodes","bid_multiplier":1.3}}
>>> conn_emr.add_instance(instance_config)
``` 

### Note

This work is free. You can redistribute it and/or modify it under the terms of the Do Whatever You Want To Public License.