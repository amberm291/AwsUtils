#!/usr/bin/env python

import boto3
import time
import os
import requests
import json

class AwsS3Helper:
    
    def __init__(self,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,region_name="ap-southeast-1"):
        '''
             Method to programatically access S3 objects.

             PARAMETERS:
                AWS_ACCESS_KEY_ID (string): Access Key associated with an AWS account.
                AWS_SECRET_ACCESS_KEY (string): Corresponding Secret Key associated to the account.
                (OPTIONAL) region_name (string): Region for the account. Set default to 'ap-southeast-1'. 
        '''

        self.conn = boto3.client("s3", region_name = region_name, aws_access_key_id = AWS_ACCESS_KEY_ID, \
            aws_secret_access_key = AWS_SECRET_ACCESS_KEY)

    def set_key(self, path):
        '''
            Utility function to get bucket and key from a S3 path

            PARAMETERS:
                path (string): s3 path

            OUTPUT:
                bucket (string): bucket in which the s3 path is
                key (string): s3 path without the bucket.

            USAGE:
                set_key('s3_bucket/s3_path/file.txt')

        '''
        bucket = path.split("/")[0]
        key = "/".join(path.split("/")[1:])
        return bucket, key

    def upload_file(self, path, inpfname):
        '''
            To upload a S3 object

            PARAMETERS:
                path (string): s3 path where file to be uploaded. Should be specified as bucket + key.
                inpfname (string): local path of file to be uploaded

            RETURNS: None

            USAGE:
                upload_file('s3_bucket/s3_path/file.txt','/home/abc/local_dir/file.txt')
        '''
        bucket, key = self.set_key(path)
        self.conn.upload_file(inpfname, bucket, key)
    
    def download_file(self, path, outfname):
        '''
            To download a S3 object.

            PARAMETERS:
                path (string): s3 object to be downloaded. Should be specified as bucket + key.
                inpfname (string): local path of where the object is to be downloaded.

            RETURNS: None

            USAGE:
                download_file('s3_bucket/s3_path/file.txt','/home/abc/local_dir/file.txt')
        '''
        bucket, key = self.set_key(path)
        self.conn.download_file(bucket, key, outfname)

    def del_file(self, path):
        '''
            To delete a s3 object.

            PARAMETERS: 
                path (string): s3 path of object to be deleted.

            RETURNS: None

            USAGE:
                del_file('s3_bucket/s3_path/file.txt')
        '''
        bucket, key = self.set_key(path)
        self.conn.delete_object(Bucket=bucket,Key=key)

    def list_keys(self, prefix_name):
        ''' 
            Used to list files in a S3 object/folder.

            PARAMETERS:
                prefix_name (string): s3 path of folder/object

            OUPTUT:
                A list of strings containing s3 paths.

            USAGE:
                list_keys('s3_bucket/s3_path/')
        '''
        bucket, key = self.set_key(prefix_name)
        is_truncated = True
        result = []
        marker = ""
        while is_truncated:
            response = self.conn.list_objects(Bucket=bucket,Prefix=key,Delimiter=",",Marker=marker)
            if "Contents" not in response: break
            result.extend(response["Contents"])
            is_truncated = response["IsTruncated"]
            if "NextMarker" not in response: break
            marker = response["NextMarker"]
        return map(lambda x:bucket + "/" + x["Key"], result)
                
    def copy_key(self, src_key, dst_key):
        '''
            Used to copy file from one s3 path to another

            PARAMETERS:
                src_key (string): s3 path from where file to be copied
                dst_key (string): s3 path where file to be copied

            OUTPUT: None

            USAGE:
                copy_key('s3_bucket1/some_path/fname.txt','s3_bucket2/some_path2/fname2.txt')

        '''
        bucket ,key = self.set_key(dst_key)
        self.conn.copy_object(Bucket=bucket,CopySource=src_key,Key=key)

class AwsEmrHelper:
    def __init__(self,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,region_name="ap-southeast-1",):
        '''
            Method to create a cluster on EMR and run a jobflow on it.

            PARAMETERS:
                AWS_ACCESS_KEY_ID (string): Access Key associated with an AWS account.
                AWS_SECRET_ACCESS_KEY (string): Corresponding Secret Key associated to the account.
                (OPTIONAL) region_name (string): Region for the account. Set default to 'ap-southeast-1'. 
        '''

        self.conn_s3 = AwsS3Helper(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        self.conn_emr = boto3.client("emr", region_name = region_name, aws_access_key_id = AWS_ACCESS_KEY_ID, \
            aws_secret_access_key = AWS_SECRET_ACCESS_KEY)
        self.config_bootstrapper = []
        self.s3n = "s3n://"
        self.instance_list = []
        self.steps= []

    def get_spot_price(self,instance_dict):
        '''
            Utility function to get bidprice for an instance type. If no bid multiplier is mentioned, 1.5 is taken as the default multiplier.

            PARAMETERS: 
                instance_dict (dictionary): Dictionary containing bid multiplier and instance type. The value of 'Market' key in instance_dict should be set to 'SPOT'.

            OUTPUT:
                bidprice (string)

            USAGE:
                instance_dict = {"instance_type":"c3.xlarge","num_instances":2,"market":"SPOT","name":"Worker Nodes","bid_multiplier":1.5}
                spot_price = self.get_spot_price(instance_dict)
        '''
        if instance_dict["market"] == "SPOT":
            if "bid_multiplier" in instance_dict:
                bid_multiplier = instance_dict["bid_multiplier"]
            else:
                bid_multiplier = 1.5
            current_epoch = str(int(time.time()))
            url = "https://spot-price.s3.amazonaws.com/spot.js?callback=callback&_=" + current_epoch
            r = requests.get(url)
            data = r.text
            data = data.replace("callback(","")
            data = data.replace(")","")
            data = data.replace(";","")
            data = json.loads(data)
            result = None
            for region in data["config"]["regions"]:
                if region["region"] == "apac-sin":
                    for instances in region["instanceTypes"]:
                        for instance in instances["sizes"]:
                            if instance["size"] == instance_dict["instance_type"]:
                                result = float(instance["valueColumns"][0]["prices"]["USD"])

            if not result:
                raise ValueError("The specified instance type doesn't exist or is not available for spot use.")
            return str(round(bid_multiplier*result,3))
        else:
            return None

    def clear_s3_folder(self,s3_path):
        '''
            Utility function to delete a s3 path and it's subpaths.

            PARAMETERS:
                s3_path (string): s3 path to be cleared.

            OUPTUT: None

            USAGE:
                clear_s3_folder('s3_bucket/some_path/')
        '''
        file_keys = self.conn_s3.list_keys(s3_path)
        for fkey in file_keys:
            self.conn_s3.del_file(fkey)

    def set_input_path(self, input_path):
        '''
            Utility function to set path for input files

            PARAMETERS:
                input_path (string or list, in case of multiple inputs): s3 path of input files

            OUTPUT: None

            USAGE:
                set_input_path('s3_bucket/some_path/') or set_input_path(['s3_bucket1/some_path1/','s3_bucket2/some_path2/'])
        '''
        if isinstance(input_path,list):
            if len(input_path) == 0:
                raise ValueError("Length of input list is 0.")
            self.input_path = map(lambda x:self.s3n + x,input_path)
            self.input_path = ",".join(self.input_path)
        else:
            self.input_path = self.s3n + input_path

    def set_output_path(self, output_path, del_existing_path):
        '''
            Utility function to set path for job output. Note: This path shouldn't exist before the starting the job. Setting del_existing_path to True will delete the output path if it exists.

            PARAMETERS:
                output_path (string or list, in case of multiple inputs): s3 path for job output.
                del_existing_path (bool): Flag to delete output path if it already exists.

            OUTPUT: None

            USAGE:
                set_output_path('s3_bucket/some_path/')
        '''
        if del_existing_path:
            self.clear_s3_folder(output_path)

        if len(self.conn_s3.list_keys(output_path)) != 0: 
            raise ValueError("Output path already exists.")
        self.output_path = self.s3n + output_path

    def add_bootstrap_actions(self, bootstrap_path, bootstrap_params, bootstrap_title="Bootstrap Actions"):
        '''
            Utility function to add bootstrapping steps to cluster.

            PARAMETERS:
                bootstrap_path (path): s3 path where bootstrap file will be stored.
                bootstrap_params (list): list of bootstrapping parameters.
                (OPTIONAL) bootstrap_title (string): Title for bootstrapping actions  

            OUTPUT: None

            USAGE:
                params = ['-s','mapred.skip.mode.enabled=true',
                          '-s', 'mapred.skip.map.max.skip.records=1',
                          '-s', 'mapred.skip.attempts.to.start.skipping=2']
                add_bootstrap_actions('s3_bucket/some_path/',params,bootstrap_title="Parameters to handle Bad Input")
        '''
        bootstrap_action = {"Name":bootstrap_title, "ScriptBootstrapAction":{"Path":"s3://" + bootstrap_path, "Args":bootstrap_params}}
        self.config_bootstrapper.append(bootstrap_action)

    def set_mapper_loc(self, mapper_loc, mapper_fname):
        '''
            Utility to upload mapper source file and set it's s3 path.
            PARAMETERS:
                mapper_loc (string): s3 path to upload mapper source file.
                mapper_fname (string): local path of mapper source file.

            OUPTUT: None

            USAGE:
                set_mapper_loc('s3_bucket/src_path/mapper.py','/home/abc/mapper.py')

        '''
        if not os.path.exists(mapper_fname):
            raise IOError("Mapper doesn't exist at the specified location.")
        self.conn_s3.upload_file(mapper_loc,mapper_fname)
        self.mapper_loc = self.s3n + mapper_loc

    def set_reducer_loc(self, reducer_loc, reducer_fname):
        '''
            Utility to upload reducer source file and set it's s3 path.
            PARAMETERS:
                reducer_loc (string): s3 path to upload reducer source file.
                reducer_fname (string): local path of reducer source file.

            OUPTUT: None

            USAGE:
                set_reducer_loc('s3_bucket/src_path/reducer.py','/home/abc/reducer.py')

        '''
        if not os.path.exists(reducer_fname):
            raise IOError("Reducer doesn't exist at the specified location.")
        self.conn_s3.upload_file(reducer_loc,reducer_fname)
        self.reducer_loc = self.s3n + reducer_loc

    def get_debugging_step(self):
        '''
            Function to get debuggin step.
            PARAMETERS: None

            OUTPUT: dict containing debug step actions

            USAGE:
                get_debugging_step()
        '''
        return {
                "Name": "Setup Hadoop Debugging",
                "ActionOnFailure": "TERMINATE_JOB_FLOW",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": ["state-pusher-script"]
                }
            }

    def add_job_step(self, step_name, input_path, output_path, mapper_path, mapper_fname, reducer_path=None, 
                    reducer_fname=None, del_existing_path=False, cache_files=[], cache_loc=None):
        '''
            Function to add steps to a job flow. All steps needs to be added before job flow starts.

            PARAMETERS:
                step_name (string): Title for step.
                input_path (list or string): input s3 path for step.
                output_path (string): output s3 path for step
                mapper_path (string): s3 path to upload mapper source file
                mapper_fname (string): local path of mapper source file
                (OPTIONAL) reducer_path (string): s3 path to upload reducer source file
                (OPTIONAL) reducer_fname (string): local path of reducer source file
                (OPTIONAL) del_exisitng_path (bool): Flag to clear output s3 path if it exists. Default is set to False. Set to True to clear s3 path if it exists.
                (OPTIONAL) cache_files (list): list of cache files to be provided during runtime. If cache_loc is not provided, then cache_files need to be a list of s3 paths. If cache_loc is provided, then cache_files should be a list of local paths.
                (OPTIONAL) cache_loc (string): s3 path where cache files to be uploaded.

            OUTPUT: None

            USAGE:
                add_job_step('s3_bucket/input_path/','s3_bucket/input_path/','s3_bucket/src_path/map.py','/home/xyz/map.py',
                    reducer_path='s3_bucket/src_path/reduce.py',reducer_fname='/home/xyz/map.py',del_existing_path=True,
                    cache_files=['/home/xyz/dict.p'],cache_loc='s3_bucket/src_path/')
        '''
        files = []
        if len(cache_files) != 0:
            if cache_loc:
                for path in cache_files:
                    fname = path.split("/")[-1]
                    self.conn_s3.upload_file(cache_loc + fname, path)
                    files.append(self.s3n + cache_loc + fname + "#" + fname)
            else:
                cache_files = map(lambda x:s3n + cache_files + "#" + x.split("/")[-1],cache_files)
                files.extend(cache_files)

        self.reducer_loc = None
        self.set_input_path(input_path)
        self.set_output_path(output_path,del_existing_path)
        self.set_mapper_loc(mapper_path, mapper_fname)
        mapper_fname = mapper_fname.split("/")[-1]
        files.append(self.s3n + mapper_path + "#" + mapper_fname)

        if reducer_path and reducer_fname:
            self.set_reducer_loc(reducer_path, reducer_fname)
            reducer_fname = reducer_fname.split("/")[-1]
            files.append(self.s3n + reducer_path + "#" + reducer_fname)

        args = ["hadoop-streaming",
                 "-files", ",".join(files),
                 "-mapper", "python2.7 " + mapper_fname,
                 "-input", self.input_path,
                 "-output", self.output_path]

        if reducer_path and reducer_fname:
            args.extend(["-reducer","python2.7 " + reducer_fname])

        step_dict = {
                    "Name": step_name,
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": args},
                    "ActionOnFailure":"TERMINATE_JOB_FLOW"
        }
        self.steps.append(step_dict)

    def add_instance(self, instance_dict):
        '''
            Function to add instances to cluster

            PARAMETERS: instance_dict (dictionary): Dictionary containing instance parameters. The dictionary should only have three keys i.e. MASTER, CORE and TASK. These three keys are the instance roles specified to the cluster. Example of an instance dictionary:

                instance_dict = {"MASTER":{"instance_type":"m1.medium","num_instances":1,"market":"ON_DEMAND","name":"Main Nodes"},"CORE":{"instance_type":"c3.xlarge","num_instances":2,"market":"ON_DEMAND","name":"Worker Nodes"},"TASK":{"instance_type":"c3.xlarge","num_instances":2,"market":"SPOT","name":"Worker Nodes","bid_multiplier":1.2}}
                here,
                    instance_type (string): Type of instance. Refer AWS documentation for more information on instance type.
                    num_instances (int): Number of instances to be added to cluster for the specific role.
                    market (string): Only two options - ON_DEMAND or SPOT. 
                    name (string): Name for role instances
                    bid_multiplier (float): Only applicable if market is set to SPOT. Multiplies spot instances rate by this multiplier to get bid price.

            RETURNS: None

            USAGE:
                add_instance(instance_dict)
        '''
        if "MASTER" not in instance_dict or "CORE" not in instance_dict:
            raise ValueError("Master and Core nodes not defined in instance dictionary.")
        for key in instance_dict:
            if key not in ["MASTER","CORE","TASK"]:
                raise ValueError("Unknown Role specified. Role must be one of: MASTER,CORE,TASK.")
            role_dict = dict(
                    InstanceCount=instance_dict[key]["num_instances"],
                    InstanceRole=key,
                    InstanceType=instance_dict[key]["instance_type"],
                    Market=instance_dict[key]["market"],
                    Name=instance_dict[key]["name"])
            if instance_dict[key]["market"] == "SPOT":
                role_dict["BidPrice"] = self.get_spot_price(instance_dict[key])
            self.instance_list.append(role_dict)

    def run_job(self, cluster_name, log_path, tags_list=None, ami_version=None, release_label=None, enable_debugging=False):
        '''
            Function to start job flow.

            PARAMETERS:
                cluster_name (string): Name for the cluster.
                log_path (string): s3 path where logs will be stored for job
                tags_list (list): list of dictionary containing tags, if any, to be added to the cluster.
                ami_version (string): AMI version to be used. Should be used only for EMR 3 and below. At least one of ami_version or release_label should be specified.
                release_label (string): To be used for EMR 4 and above.
                enable_debugging (boolean): Flag to enable debugging. BY default set to False. 

            OUTPUT: None

            USAGE: conn_emr.run_job('Test Cluster','emr-logs/',tags_list=[{'Key':'abc','Value':'xyz'}],release_label='emr-5.0.0')

        '''
        if enable_debugging:
            self.steps = [self.get_debugging_step()] + self.steps
        if len(self.instance_list) == 0:
            raise ValueError("No instance configurations specified.")
        if len(self.steps) == 0:
            raise ValueError("No steps added to the job.")
        if not release_label and not ami_version:
            raise ValueError("Must specify either release label or ami version")
        if release_label:
            cluster_id = self.conn_emr.run_job_flow(Name=cluster_name,
                                     ReleaseLabel=release_label,
                                     Instances={"InstanceGroups":self.instance_list},
                                     LogUri=self.s3n + log_path,
                                     Steps=self.steps,
                                     BootstrapActions=self.config_bootstrapper,
                                     VisibleToAllUsers=True,
                                     JobFlowRole="EMR_EC2_DefaultRole",
                                     ServiceRole="EMR_DefaultRole",
                                     Tags=tags_list)
        if ami_version:
            cluster_id = self.conn_emr.run_job_flow(Name=cluster_name,
                                     Instances={"InstanceGroups":self.instance_list},
                                     LogUri=self.s3n + log_path,
                                     Steps=self.steps,
                                     AmiVersion=ami_version,
                                     BootstrapActions=self.config_bootstrapper,
                                     VisibleToAllUsers=True,
                                     JobFlowRole="EMR_EC2_DefaultRole",
                                     ServiceRole="EMR_DefaultRole",
                                     Tags=tags_list)


        self.job_id = cluster_id["JobFlowId"]
        print "JobId is : " + self.job_id

    def get_cluster_status(self):
        '''
            Function to get cluster status

            PARAMETERS: None

            OUTPUT: None

            USAGE: get_cluster_status()
        '''
        status = self.conn_emr.describe_cluster(ClusterId=self.job_id)
        jobstate = status['Cluster']['Status']['State']
        return jobstate



