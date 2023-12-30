import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging_service
import boto3
import datetime
import uuid
from io import StringIO
import configparser
from pyspark.sql.functions import lit
from awsglue.dynamicframe import DynamicFrame

CONFIG_BUCKET_NAME_KEY = "config_bucket"
SYS_CONFIG_KEY = "sys_config_file"
FEED_CONFIG_KEY = "feed_config_file"
GUID_KEY = "guid"
REGION_KEY = "region"
BOTO3_AWS_REGION = ""
PROCESS_KEY = 'ingest-to-raw' #ingest-raw?
JOB_KEY = "ingest-to-raw" #ingest-raw?
FAST_EVENT_KEY = "fast_event"
JOB_BOOKMARK_OPTION_KEY="job-bookmark-option"
WRITE_TO_SINGLE_PARTITION_KEY="single_partition"
NUM_OF_PARTITIONS=5
#Read it from config
SOURCE_SYSTEM="FAST"
INCR_LOAD="src_table_type_incr"

args = getResolvedOptions(sys.argv,['JOB_NAME',
                        GUID_KEY,
                        CONFIG_BUCKET_NAME_KEY,
                        SYS_CONFIG_KEY,
                        FEED_CONFIG_KEY,
                        FAST_EVENT_KEY,
                        REGION_KEY,
                        WRITE_TO_SINGLE_PARTITION_KEY,
                        INCR_LOAD
                        ])

def main():
    global S3_PARTITION_KEY
    global ATHENA_RAW_DATABASE
    global REGION
    global S3_PARTITION_VALUE
    global RAW_BUCKET_NAME
    global FAST_EVENT
    global PREFIX_NAME
    global SOURCE_SYSTEM
    global S3_BASE_PATH
    
    sc = SparkContext()
    glueContext = GlueContext(sc)
    #spark = glueContext.spark_session
    job = Job(glueContext)

    #use Job name and event name combination as key to Bookmark entry
    bookmark_key=args["JOB_NAME"]+"_"+args[FAST_EVENT_KEY]
    if args[INCR_LOAD].upper() == 'Y':
        bookmark_key=bookmark_key+"_incr_load"
    job.init(bookmark_key, args)

    sys_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[SYS_CONFIG_KEY])
    feed_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[FEED_CONFIG_KEY])
    source = args[FEED_CONFIG_KEY].split('/')[-1].split('.')[0]  

    guid = args[GUID_KEY]
    REGION=args[REGION_KEY]
    single_s3_partition=args[WRITE_TO_SINGLE_PARTITION_KEY]
    ATHENA_RAW_DATABASE = sys_config.get(REGION, 'database') 
    RAW_BUCKET_NAME = sys_config.get(REGION, 'raw_bucket')
    cloudwatch_log_group = sys_config.get(REGION, 'cloudwatch_log_group')
    boto3_aws_region = sys_config.get(REGION, 'boto3_aws_region')
    client = boto3.client('logs', region_name=boto3_aws_region)
    
    FAST_EVENT = args[FAST_EVENT_KEY]
    additional_options_dict={}
    bookmark_keys_list = []
    bookmark_keys = feed_config.has_option('feed', 'bookmark-keys') and feed_config.get('feed', 'bookmark-keys') or None
    
    if bookmark_keys and bookmark_keys.strip():
        bookmark_keys_list = bookmark_keys.split(',')
        additional_options_dict["jobBookmarkKeys"]=bookmark_keys_list
        additional_options_dict["jobBookmarkKeysSortOrder"]="asc"
    # include additional options hashfield and hashpartitions for CVQ/fin trx/ non fin trx    
    sqlQuery = feed_config.has_option('feed', 'sampleQuery') and feed_config.get('feed', 'sampleQuery') or None
    enablePartitioningForQuery = feed_config.has_option('feed', 'enablePartitioningForSampleQuery') and feed_config.getboolean('feed', 'enablePartitioningForSampleQuery') or None
    
    source_partition_column = feed_config.has_option('feed', 'hashfield') and feed_config.get('feed', 'hashfield') or None
    no_of_parallel_threads = feed_config.has_option('feed', 'hashpartitions') and feed_config.get('feed', 'hashpartitions') or None
    s3_file_count = feed_config.has_option('feed', 's3-no-of-files-tobe-created-per-run') and feed_config.getint('feed', 's3-no-of-files-tobe-created-per-run') or None
    transformation_parti_cnt=feed_config.has_option('feed', 'xform-partition-count') and feed_config.getint('feed', 'xform-partition-count') or None
    
    if sqlQuery:
        additional_options_dict["sampleQuery"]=sqlQuery
    if source_partition_column:
        additional_options_dict["hashfield"]=source_partition_column
        if no_of_parallel_threads:
            additional_options_dict["hashpartitions"]=no_of_parallel_threads
    if sqlQuery and source_partition_column and enablePartitioningForQuery:
        additional_options_dict["enablePartitioningForSampleQuery"]=enablePartitioningForQuery
    
    PREFIX_NAME = feed_config.get('feed', 'prefix-name')
    source_table_name = feed_config.get('feed', 'source-table-name')
    S3_BASE_PATH=PREFIX_NAME+"/"+SOURCE_SYSTEM+"_"+FAST_EVENT.upper()+"_STAGE_ES"
    #In QA, the folder name ends with _QA
    if (REGION == 'qa'):
        S3_BASE_PATH=S3_BASE_PATH+"_"+REGION.upper()
    # get the partition key 
    S3_PARTITION_KEY = feed_config.get('feed', 's3-partition-key')
    s3_partition_key_list = []
    if S3_PARTITION_KEY and S3_PARTITION_KEY.strip():
        s3_partition_key_list = S3_PARTITION_KEY.split(',')   

    partition_value = feed_config.get('feed', 's3-partition-value')
    log_manager = logging_service.LogManager(cw_loggroup=cloudwatch_log_group, 
                            cw_logstream=source+"_"+guid,
                                            process_key=PROCESS_KEY, client=client,
                                            job=REGION + '-isg-ie-fast-json-ingest-history')
    if (single_s3_partition.upper() == 'Y'and partition_value and partition_value.strip()):
        S3_PARTITION_VALUE = partition_value
    else:
        currentDateFormatted = datetime.datetime.today().strftime ('%Y-%m-%d')
        S3_PARTITION_VALUE = currentDateFormatted
        
    log_manager.log(message="Starting the fast events history data ingestion ",
                    args={"environment": REGION,                    
                    "source_system": SOURCE_SYSTEM,"event":FAST_EVENT,                                        
                    "enablePartitioningForSampleQuery":enablePartitioningForQuery,
                    'hashfield':source_partition_column,'hashpartitions':no_of_parallel_threads,                    
                    'prefix': PREFIX_NAME, 
                    's3_basePath':S3_BASE_PATH,
                    "partition_keys": s3_partition_key_list,"single_s3_partition":single_s3_partition,
                    "partition_value": partition_value, "calc_partition_value": S3_PARTITION_VALUE,
                    "transformations_parti_count": transformation_parti_cnt,
                    "s3_files_per_run": s3_file_count,
                    "bookmark_keys": bookmark_keys,
                    "bookmark_name":args["JOB_NAME"]+"_"+args[FAST_EVENT_KEY],
                    "job": JOB_KEY})    
    # create dynamic frame using  the jdbc data store table
    DataCatalogtable_node1 = glueContext.create_dynamic_frame.from_catalog(
        database=ATHENA_RAW_DATABASE,
        table_name=source_table_name,
        transformation_ctx=source_table_name,
        additional_options=additional_options_dict
    )   
    log_manager.log(message="No. of records fetched from FAST source table ",
                    args={"environment": REGION,                    
                    "job": JOB_KEY,
                    "source_system": SOURCE_SYSTEM,"event":FAST_EVENT,
                    "count":DataCatalogtable_node1.count()})  
 
    log_manager.log(message="Converting payload json string to Dynamic frame",
                    args={"environment": REGION,                   
                    "source_system": SOURCE_SYSTEM,"event":FAST_EVENT,
                    "job": JOB_KEY})
    # Payload column contains json escaped string - convert the json string to Dynamic Frame
    Unboxing_node3 = Unbox.apply(frame=DataCatalogtable_node1, path="PAYLOAD", format="json",
        transformation_ctx="Unboxing_node3",info="Converting payload json string to dynamic frame failed") 
    Unboxing_node3.printSchema()
    checkErrors(Unboxing_node3,log_manager)
    # increase the number partitions to reduce the CPU load for CVQ and FinTrx
    if (transformation_parti_cnt and transformation_parti_cnt > 0  and 
        (no_of_parallel_threads and int(no_of_parallel_threads) < transformation_parti_cnt )):
        print("Repartitioning..")    
        Unboxing_node3=Unboxing_node3.repartition(transformation_parti_cnt)
     
    #extract Payload nested dynamic frame and  add event creation time column              
    log_manager.log(message="Extract Payload Dynamic frame and add event_creation_time column",
                args={"environment": REGION,                 
                "source_system": SOURCE_SYSTEM,"event":FAST_EVENT,
                "job": JOB_KEY})
                    
    Output_DyF =  Map.apply(frame = Unboxing_node3, f = get_payload,transformation_ctx = "Output_DyF")
    Output_DyF.printSchema()

    rec_count=Output_DyF.count()
    #print("No. of records writing to s3:",rec_count)    
    if rec_count > 0:
        #repartition data to avoid too many small files if s3_files_per_run property is specified
        # and no_of_partitions is more than s3_files_per_run
        if s3_file_count:
            no_of_partitions=Output_DyF.getNumPartitions()  
            print("No. of partitions:",no_of_partitions)
            if s3_file_count < no_of_partitions : 
                log_manager.log(message="Repartitioning data to reduce the no. of files written to s3",
                            args={"environment": REGION,
                            "source_system": SOURCE_SYSTEM,"event":FAST_EVENT,
                            "job": JOB_KEY})
                Output_DyF=Output_DyF.coalesce(s3_file_count)
                
        log_manager.log(message="Writing data to s3 bucket",
                    args={"environment": REGION, 
                    "source_system": SOURCE_SYSTEM,"event":FAST_EVENT,
                    "job": JOB_KEY})
        
        # write the DF to s3
        Output_DyF=glueContext.write_dynamic_frame.from_options(
            frame=Output_DyF,
            connection_type="s3",
            format="json",
            connection_options={
                "path": "s3://"+RAW_BUCKET_NAME+"/"+S3_BASE_PATH+"/",
                "partitionKeys": s3_partition_key_list,
                "compression": "gzip"
            },
            transformation_ctx=FAST_EVENT,
        )        
        log_manager.log(message="No. of records written to s3 ",
                args={"environment": REGION,
                "source_system": SOURCE_SYSTEM,"event":FAST_EVENT,
                "job": JOB_KEY,
                "count":(rec_count-Output_DyF.count())}) 
    checkErrors(Output_DyF,log_manager)                                                                                                              
    job.commit()

def get_payload(rec):
  df=rec["PAYLOAD"]
  # add the event_creation_time column to the df to be uses as partition key for s3  
  df[S3_PARTITION_KEY]=S3_PARTITION_VALUE
  return df
  
def read_config(bucket, file_prefix):
    s3 = boto3.resource('s3')   
    bucket = s3.Bucket(bucket)
    for obj in bucket.objects.filter(Prefix=file_prefix):
        buf = StringIO(obj.get()['Body'].read().decode('utf-8'))
        config = configparser.ConfigParser()
        config.read_file(buf)
        return config

def checkErrors(df,log_manager):   
    error_rec_count=df.errorsCount()
    if error_rec_count > 0:        
        errorDF = df.errorsAsDynamicFrame().toDF()
        #errorDF.show(truncate=False)
        errorDF.printSchema()
        errorDF.write.format('json').save('s3://'+RAW_BUCKET_NAME+S3_BASE_PATH+'_error/'+S3_PARTITION_VALUE+"/",mode='append')

        for i in errorDF.collect():
            call_site_info=i["error"]["callsite"]["info"]
            call_site=i["error"]["callsite"]["site"]
            error=i["error"]["msg"]
            stackTrace=i["error"]["stackTrace"]
            audit_guid=i["error"]["dynamicRecord"]["AUDIT_GUID"]
            audit_update_ts=i["error"]["dynamicRecord"]["AUDIT_UPDATE_TS"]
            audit_update_ts_str = audit_update_ts.strftime("%m/%d/%Y, %H:%M:%S") 
            policy_number=i["error"]["dynamicRecord"]["POLICYNUMBER"]
            log_manager.log_error(message="Error while converting json string to dynamic frame",
                args={"environment": REGION,                
                'count':error_rec_count,
                "job": JOB_KEY,
                "callsite_info":call_site_info,
                "callsite":call_site,
                "error_msg":error,
                "stackTrace":stackTrace,
                "policy_number":policy_number,
                "audit_guid":audit_guid,
                "audit_update_ts":audit_update_ts_str,
                "source_system": SOURCE_SYSTEM,"event":FAST_EVENT
                })       

# entry point for PySpark ETL application
if __name__ == '__main__':
    main() 