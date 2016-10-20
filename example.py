"""
All paths and keys here are dummy. Add your own paths to test this example.
"""

from boto3_utils import AwsEmrHelper, AwsS3Helper

AWS_ACCESS_KEY_ID = 'abc'
AWS_SECRET_ACCESS_KEY = 'xyz'
region_name = 'us-southwest-1'

def example_s3():
    conn_s3 = AwsS3Helper(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, region_name=region_name)

    conn_s3.upload_file('s3_bucket/s3_path/file.txt','/home/abc/local_dir/file.txt')
    conn_s3.download_file('s3_bucket/s3_path/file.txt','/home/abc/local_dir/file.txt')
    conn_s3.del_file('s3_bucket/s3_path/file.txt')
    keys = list_keys('s3_bucket/s3_path/')
    copy_key('s3_bucket1/some_path/fname.txt','s3_bucket2/some_path2/fname2.txt')

def example_emr():
    conn_emr = AwsEmrHelper(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, region_name=region_name)
    input_path = 'input_bucket/input_path/'   #input_files can also be a list of paths
    output_path = 'output_bucket/output_bucket/'   #output s3 path
    mapper_fname = 'map.py'
    reducer_fname = 'reduce.py'
    local_src_path = '/home/path/src/'  #Provide local path of directory where map and reduce source files are stored.
    code_path = 'code_bucket/src_files/'    #s3 path where source files will be uploaded
    cache_files = ['dict.p','list.p']     #local path of cache files to be provided dring step execution
    cache_loc = 'code_bukcet/cache_files/'   #s3 path where cache files to be uploaded
    step_name = 'My Step'  

    conn_emr.add_job_step(step_name, input_path, output_path, code_path + mapper_fname, local_src_path + mapper_fname, reducer_path=code_path + reducer_fname, reducer_fname=code_path + reducer_fname, cache_files=cache_files, cache_loc=cache_loc, del_existing_path=True)     #refer documentation in source (boto3_utils.py) for each parameter

    cluster_name = 'My Job'
    instance_config = {"MASTER":{"instance_type":"m1.medium","num_instances":1,"market":"ON_DEMAND","name":"Main Nodes"},"CORE":{"instance_type":"r3.xlarge","num_instances":1,"market":"ON_DEMAND","name":"Worker Nodes"},"TASK":{"instance_type":"r3.xlarge","num_instances":1,"market":"SPOT","name":"Worker Nodes","bid_multiplier":1.3}}

    params = ['-s','mapred.skip.mode.enabled=true', 
              '-s', 'mapred.skip.map.max.skip.records=1',
              '-s', 'mapred.skip.attempts.to.start.skipping=2']

    conn_emr.add_bootstrap_actions('myntra-datasciences/cip/bootstrap-actions/configure-hadoop',params)     #optional step

    conn_emr.add_instance(instance_config)
    conn_emr.run_job(cluster_name,'emr-logs/',release_label='emr-5.0.0')
    jobstate = ''
    while jobstate != 'TERMINATED':
        jobstate = conn_emr.get_cluster_status()
        if jobstate == 'TERMINATED_WITH_ERRORS': break
        print jobstate
        time.sleep(60)

if __name__=="__main__"




