import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.context import DynamicFrame
import sys
import configparser
from io import StringIO
import os
from awsglue.job import Job
import boto3
import re
import logging_service

CONFIG_BUCKET_NAME_KEY = "config_bucket"
SYS_CONFIG_KEY = "sys_config_file"
FEED_CONFIG_KEY = "feed_config_file"
GUID_KEY = "guid"
REGION_KEY = "region"
BOTO3_AWS_REGION = ""
BATCH_RUN_DATE = 'batch_date'
PROCESS_KEY = 'raw_to_access'
JOB_KEY = "raw-to-access"


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

    sys_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[SYS_CONFIG_KEY])
    feed_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[FEED_CONFIG_KEY])

    guid = args[GUID_KEY]
    athena_raw_database = sys_config.get(args[REGION_KEY], 'database')
    athena_access_database = sys_config.get(args[REGION_KEY], 'access_athena_db')
    cloudwatch_log_group = sys_config.get(args[REGION_KEY], 'cloudwatch_log_group')
    boto3_aws_region = sys_config.get(args[REGION_KEY], 'boto3_aws_region')
    client = boto3.client('logs', region_name=boto3_aws_region)
    client_s3 = boto3.client('s3')

    log_manager = logging_service.LogManager(cw_loggroup=cloudwatch_log_group, cw_logstream=guid,
                                             process_key=PROCESS_KEY,client=client,
                                             job=args[REGION_KEY] + '-isg-ie-daily-batch-completion')
    log_manager.log(message="Starting the raw-to-access job",
                    args={"environment": args[REGION_KEY], 'raw_db': athena_raw_database,
                          'access_db': athena_access_database, "job": JOB_KEY})

    batch_sql = feed_config.get('transformation-rules', 'batch-sql')
    batch_write_sql = feed_config.get('transformation-rules', 'batch-write-sql')
    repair_batch_data = feed_config.get('transformation-rules', 'repair-batch-data')

    repair_batch_data = repair_batch_data.replace("\n", " ").replace("\t", " ").replace("\r", " ")
    repair_batch_data = re.sub('{athena_raw_db}', athena_raw_database, repair_batch_data)

    batch_sql = batch_sql.replace("\n", " ").replace("\t", " ").replace("\r", " ")
    batch_sql = re.sub('{athena_raw_db}', athena_raw_database, batch_sql)

    print(repair_batch_data)
    log_manager.log(message='executing mapping query', args={'repair_sql': repair_batch_data, "job": JOB_KEY})

    print(batch_sql)
    log_manager.log(message='executing mapping query', args={'mapping_sql': batch_sql, "job": JOB_KEY})

    repair_batch = spark.sql(repair_batch_data)
    batch_output_df = spark.sql(batch_sql)
    batch_output_count = batch_output_df.count()
    print("count is : " + str(batch_output_count))
    log_manager.log(message="SQL query", args={"mapped output count is : ": str(batch_output_count)})

    # mapped_output_df.show()
    batch_output_df.registerTempTable("batchOutputTable")

    batch_write_sql = batch_write_sql.replace("\n", " ").replace("\t", " ").replace("\r", " ")
    batch_write_sql = re.sub('{athena_raw_db}', athena_raw_database, batch_write_sql)

    log_manager.log(message='executing query to write to Athena', args={'write_sql': batch_write_sql, "job": JOB_KEY})
    spark.sql(batch_write_sql)
    job.commit()

def read_config(bucket, file_prefix):
    s3 = boto3.resource('s3')
    i = 0
    bucket = s3.Bucket(bucket)
    for obj in bucket.objects.filter(Prefix=file_prefix):
        buf = StringIO(obj.get()['Body'].read().decode('utf-8'))
        config = configparser.ConfigParser()
        config.readfp(buf)
        return config


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()