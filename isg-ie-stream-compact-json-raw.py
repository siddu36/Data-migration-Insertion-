import boto3
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import configparser
from io import StringIO
import logging_service
import math
import time
CONFIG_BUCKET_NAME_KEY = "config_bucket"
FEED_CONFIG_KEY = "feed_config_file"
SYS_CONFIG_KEY = "sys_config_file"
REGION_KEY = "region"
PROCESS_KEY = 'landing_to_raw'
GUID_KEY = "guid"

def main():
    global BOTO3_AWS_REGION

    args = getResolvedOptions(sys.argv,
                              ['JOB_NAME', GUID_KEY,
                               CONFIG_BUCKET_NAME_KEY,
                               SYS_CONFIG_KEY,
                               FEED_CONFIG_KEY,
                               REGION_KEY])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    guid = args[GUID_KEY]
    sys_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[SYS_CONFIG_KEY])
    feed_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[FEED_CONFIG_KEY])
    raw_bucket_name = sys_config.get(args[REGION_KEY], 'raw_bucket')
    boto3_aws_region = sys_config.get(args[REGION_KEY], 'boto3_aws_region')
    cloudwatch_log_group = sys_config.get(args[REGION_KEY], 'cloudwatch_log_group')	
    client = boto3.client('logs', region_name=boto3_aws_region)
    input_prefix_name = feed_config.get(args[REGION_KEY], 'input-prefix-name')
    compact_prefix_name = feed_config.get('transformation-rules', 'compact-prefix-name')
    log_manager = logging_service.LogManager(cw_loggroup=cloudwatch_log_group, cw_logstream=args[FEED_CONFIG_KEY].split('/')[-1].split('.')[0] + '_' + guid,
                                             process_key=PROCESS_KEY, client=client,
                                             job=args[REGION_KEY] + '-isg-ie-stream-ingest-snapshot-incr')
    log_manager.log(message="Starting the plus compact job",
                    args={"environment": args[REGION_KEY], 'raw_bucket': raw_bucket_name, 'Config File': args[FEED_CONFIG_KEY]})
    feed_config_split = args[FEED_CONFIG_KEY].split('_')
    load = feed_config_split[-2]
    if load == 'full':
        drop_columns = feed_config.get('transformation-rules', 'drop_columns',fallback='null')
        src_partition_path = 's3://' + raw_bucket_name + '/' + input_prefix_name
        dest_partition_path = 's3://' + raw_bucket_name + '/' + compact_prefix_name
        df = spark.read.json(src_partition_path)
        if drop_columns != 'null':
            lst_drop_columns = drop_columns.split(',')
            df = df.drop(*lst_drop_columns)
        cnt_parti = writetolog_getcnt(df,log_manager,src_partition_path,dest_partition_path)
        fdf = df.coalesce(int(math.ceil(cnt_parti/10000)))
        fdf.write.mode('overwrite').format("json").save(dest_partition_path)
    else:
        uncomp_part = getuncompactedprefix(raw_bucket_name,input_prefix_name,compact_prefix_name)
        log_manager.log(message="Uncompacted partitions",args={"partitions":uncomp_part})
        for item in uncomp_part:
            src_partition_path = 's3://' + raw_bucket_name + '/' + input_prefix_name + item
            dest_partition_path = 's3://' + raw_bucket_name + '/' + compact_prefix_name + item
            df = spark.read.json(src_partition_path)
            cnt_parti = writetolog_getcnt(df,log_manager,src_partition_path,dest_partition_path)
            fdf = df.coalesce(int(math.ceil(cnt_parti/10000)))
            fdf.write.mode('overwrite').format("json").save(dest_partition_path)
            time.sleep(5)

def read_config(bucket, file_prefix):
    s3 = boto3.resource('s3')
    i = 0
    bucket = s3.Bucket(bucket)
    for obj in bucket.objects.filter(Prefix=file_prefix):
        buf = StringIO(obj.get()['Body'].read().decode('utf-8'))
        config = configparser.ConfigParser()
        config.readfp(buf)
        return config

def writetolog_getcnt(df_inp,logmgr,src_prefix,dest_prefix):
    totcnt = 0
    group_cols = ['metadata.eventMessage.sourceSystem.correlationId','metadata.eventMessage.sourceSystem.eventName','metadata.eventMessage.sourceSystem.name']
    df_cnt = df_inp.groupBy(*group_cols).count()
    for row in df_cnt.collect():
        logmgr.log(message="USBIE Plus Stats",args={"sourceCorrelationId":row['correlationId'],"sourceSystemName":row['name'],"eventType":row['eventName'],"Source Prefix":src_prefix,"Destination Prefix":dest_prefix,"Count":row['count']})
        totcnt = totcnt + row['count']
    return totcnt

def getuncompactedprefix(s3_bucket,src_prefix,tgt_prefix):
    client = boto3.client('s3')
    loopprefix = True
    set_src_part = set()
    set_tgt_part = set()
    kwargs = {}
    kwargs['Bucket'] = s3_bucket
    kwargs['Delimiter'] = '/'
    kwargs['Prefix'] = src_prefix
    while loopprefix:
        result = client.list_objects_v2(**kwargs)
        for i in range(len(result['CommonPrefixes'])):
            set_src_part.add((result['CommonPrefixes'][i]['Prefix']).split('/')[-2])
        if result['IsTruncated']:
            kwargs['ContinuationToken'] = result['NextContinuationToken']
        else:
            loopprefix = False
    loopprefix = True
    kwargs['Prefix'] = tgt_prefix
    while loopprefix:
        result = client.list_objects_v2(**kwargs)
        if 'CommonPrefixes' not in result:
            loopprefix = False
        else:
            for i in range(len(result['CommonPrefixes'])):
                set_tgt_part.add((result['CommonPrefixes'][i]['Prefix']).split('/')[-2])
            if result['IsTruncated']:
                kwargs['ContinuationToken'] = result['NextContinuationToken']
            else:
                loopprefix = False
    return list(set_src_part-set_tgt_part)


if __name__ == '__main__':
    main()
