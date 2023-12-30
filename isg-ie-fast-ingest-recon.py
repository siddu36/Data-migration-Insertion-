import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.context import SparkConf
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
from pyspark.sql.functions import lit
from py4j.protocol import Py4JJavaError
from io import StringIO
import configparser
import boto3
import os
import logging_service
import json
import re
import datetime
import pytz

CONFIG_BUCKET_NAME_KEY = "config_bucket"
FEED_CONFIG_FILE_KEY = "feed_config_file"
SYS_CONFIG_KEY = "sys_config_file"
GUID_KEY = "guid"
REGION_KEY = "region"
BOTO3_AWS_REGION = ""
PROCESS_KEY_BUSINESS_RECON_DQV = "landing_to_raw_recon_dqv"
PROCESS_KEY_BUSINESS_RECON = "landing_to_raw_recon"
PROCESS_KEY_REGULAR_LOG = "landing_to_raw_recon_logs"
JOB_KEY = "landing_to_raw"


def main():
    global BOTO3_AWS_REGION
    args = getResolvedOptions(sys.argv,
                              ['JOB_NAME', GUID_KEY,
                               CONFIG_BUCKET_NAME_KEY,
                               SYS_CONFIG_KEY,
                               FEED_CONFIG_FILE_KEY,
                               REGION_KEY])

    # setting up variables from configs and job arguments
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    guid = args[GUID_KEY]
    sys_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[SYS_CONFIG_KEY])
    recon_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[FEED_CONFIG_FILE_KEY])

    athena_raw_database = sys_config.get(args[REGION_KEY], 'database')
    athena_access_database = sys_config.get(args[REGION_KEY], 'access_athena_db')
    cloudwatch_log_group = sys_config.get(args[REGION_KEY], 'cloudwatch_log_group')
    boto3_aws_region = sys_config.get(args[REGION_KEY], 'boto3_aws_region')
    client = boto3.client('logs', region_name=boto3_aws_region)
    config_file = args[FEED_CONFIG_FILE_KEY]

    config_file_name = config_file.split('.')[0].split('/')[-1].split('_dyf')[0]

    log_manager_business_recon = logging_service.LogManager(cw_loggroup=cloudwatch_log_group, cw_logstream=config_file_name+'_'+guid,
                                             process_key=PROCESS_KEY_BUSINESS_RECON, client=client,
                                             job=args[REGION_KEY] + '-isg-ie-fast-ingest-recon')

    log_manager_business_dqv = logging_service.LogManager(cw_loggroup=cloudwatch_log_group, cw_logstream=config_file_name+'_'+guid,
                                             process_key=PROCESS_KEY_BUSINESS_RECON_DQV, client=client,
                                             job=args[REGION_KEY] + '-isg-ie-fast-ingest-recon')

    log_manager = logging_service.LogManager(cw_loggroup=cloudwatch_log_group, cw_logstream=config_file_name+'_'+guid,
                                             process_key=PROCESS_KEY_REGULAR_LOG, client=client,
                                             job=args[REGION_KEY] + '-isg-ie-fast-ingest-recon')

    config_type = config_file.split('.')[0].split('_')[-1]
    if config_type == 'dqv':
        for section_name in recon_config.sections():
            log_manager.log(message="Inside section:", args={'Section name': section_name})
            print("Section name:", section_name)
            if (recon_config.has_option(section_name, 'target_table')):
                dqv_table = recon_config.get(section_name, 'target_table')
            if (recon_config.has_option(section_name, 'validation_query')):
                validation_query = recon_config.get(section_name, 'validation_query')
                validation_query = re.sub('{athena_raw_db}', athena_raw_database, validation_query)
                validation_query = re.sub('{athena_access_db}', athena_access_database, validation_query)
            if (recon_config.has_option(section_name, 'validation_fields')):
                validation_fields = recon_config.get(section_name, 'validation_fields')

            dqv_df = spark.sql(validation_query)
            print("After executing dqv_df")
            dqv_df.take(1)
            dqv_df.registerTempTable("dqv_temp_table")
            # dqv_df.cache()
            print("After registering dqv temp")
            df_cnt_df = spark.sql('select count(*) cnt from dqv_temp_table')
            df_ct = df_cnt_df.head().cnt

            if df_ct > 0:
                print("dqv validations failed:", section_name)
                log_manager.log(message="Data quality validation failed:",
                                args={'Section name': section_name, 'Count': str(df_ct)})
                validation_rule = section_name
                selectquery = " select '" + validation_rule + "' as section_name ," + validation_fields + ", CURRENT_TIMESTAMP as job_run_time,'" + guid + "' as guid  from  dqv_temp_table"
                target_select_df = spark.sql(selectquery)

                print("After getting dqv select query for splunk")
                outputjson = target_select_df.toJSON().collect()

                for item in outputjson:
                    print(item)
                    log_args = json.loads(item)
                    log_args['rule_name'] = section_name
                    log_args['content_type'] = 'reconciliation_data'
                    log_args['config_type'] = config_type
                    timestamp_format = "%Y-%m-%dT%H:%M:%S.%fZ"
                    element = datetime.datetime.strptime(log_args['job_run_time'], timestamp_format)
                    datetime_eastern = element.astimezone(tz=pytz.timezone('US/Eastern'))
                    print(datetime_eastern.strftime(timestamp_format))
                    log_args['job_run_time_est'] = datetime_eastern.strftime(timestamp_format)
                    log_manager_business_dqv.log(message=item, args=log_args)

                log_manager.log(message="outputjson flow completed:", args={'audit_write_sql': selectquery})
                dqv_insert_sql = "insert into " + athena_raw_database + "." + dqv_table + \
                                 " select '" + validation_rule + "'," + validation_fields + "," + \
                                 "cast(UNIX_TIMESTAMP((from_utc_timestamp(CURRENT_timestamp(), 'EST5EDT')), 'yyyy-MM-dd HH:mm:ss') as timestamp),'" + \
                                 guid + "' from  dqv_temp_table"
                spark.sql(dqv_insert_sql)
                print("After inserting to dqv table")
            # dqv_df.unpersist()
    elif config_type == 'dyf':
        log_manager.log(message="Starting the fast ingest recon job with dynamic frame",
                        args={"environment": args[REGION_KEY], 'raw_db': athena_raw_database, "job": JOB_KEY})
        print("Job starting in dynamic mode")
        run_reconciliation(recon_config, log_manager, athena_raw_database, athena_access_database, spark, glueContext, config_type, guid,log_manager_business_recon)
    else:
        log_manager.log(message="Starting the fast ingest recon job",
                        args={"environment": args[REGION_KEY], 'raw_db': athena_raw_database, "job": JOB_KEY})
        print("Job starting in normal mode")
        run_reconciliation(recon_config, log_manager, athena_raw_database, athena_access_database, spark,
                                   glueContext, config_type, guid,log_manager_business_recon)
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

def run_reconciliation(recon_config,log_manager,athena_raw_database,athena_access_database,spark,glueContext,config_type,guid,log_manager_business_recon):
    for section_name in recon_config.sections():
        log_manager.log(message="Inside section:", args={'Section name': section_name})
        print("Section name: ", section_name)
        audit_fields = ""
        ctrl_name = section_name
        if (recon_config.has_option(ctrl_name, 'target_table')):
            recon_table = recon_config.get(section_name, 'target_table')
        if (recon_config.has_option(ctrl_name, 'isgie_table')):
            isgie_table = recon_config.get(section_name, 'isgie_table')
        if (recon_config.has_option(ctrl_name, 'join_fields')):
            join_fields = recon_config.get(section_name, 'join_fields')
        if (recon_config.has_option(ctrl_name, 'audit_fields')):
            audit_fields = recon_config.get(section_name, 'audit_fields')
        if (recon_config.has_option(ctrl_name, 'isgie_query')):
            isgie_query = recon_config.get(section_name, 'isgie_query')
            isgie_query = re.sub('{athena_raw_db}', athena_raw_database, isgie_query)
            isgie_query = re.sub('{athena_access_db}', athena_access_database, isgie_query)
        if (recon_config.has_option(ctrl_name, 'fast_query')):
            fast_query = recon_config.get(section_name, 'fast_query')
            fast_query = re.sub('{athena_raw_db}', athena_raw_database, fast_query)

        log_manager.log(message="Reconciliation details:",
                        args={'isgie_query': isgie_query, 'fast_query': fast_query})
        if config_type == 'dyf':
            dyf = glueContext.create_dynamic_frame.from_catalog(database=athena_raw_database,table_name=isgie_table)
            dyf.toDF().createOrReplaceTempView("dyf_temp_table")
            config_type = 'recon'
            print(" Dynamic frame registered as temp table")

        isgie_df = spark.sql(isgie_query)
        isgie_df.take(1)
        fast_df = spark.sql(fast_query)
        fast_df.take(1)
        pattern = re.compile(r'\s+')
        join_fields = re.sub(pattern, '', join_fields)
        join_chunks = join_fields.split(',')
        cond_list = []
        translation = {39: None}
        if (len(join_chunks) > 1):
            for keyname in join_chunks:
                join_str = "isgie_df.isgie_" + keyname + "==" + "fast_df.fast_" + keyname
                cond_list.append(join_str)
            join_columns = str(cond_list).translate(translation)
        else:
            join_columns = '[join_fields]'

        isg_fast_df = isgie_df.join(fast_df, eval(join_columns), 'outer').withColumn("ctrl_name",
                                                                                             lit(ctrl_name)). \
                    withColumn("isgie_table", lit(isgie_table)). \
                    withColumn("differences",
                               isgie_df["isgie_count"].cast(LongType()) - fast_df["fast_count"].cast(LongType())). \
                    withColumn("run_status", F.when(
                    (isgie_df["isgie_count"].cast(LongType()) - fast_df["fast_count"].cast(LongType())) == 0,
                    "success").otherwise("Failure"))
        isg_fast_df.take(1)

        print("After join operation")
        isg_fast_df.registerTempTable("fast_isgie_temp_table")
        selectquery = " select '" + ctrl_name + "' as section_name,differences," + audit_fields + ", CURRENT_TIMESTAMP as job_run_time,'" + guid + "' as guid from  fast_isgie_temp_table"
        target_select_df = spark.sql(selectquery)
        #print(target_select_df.show())
        log_manager.log(message=target_select_df.show())
        target_select_df.take(1)
        target_select_df.registerTempTable("fast_rec_table")
        print("After registering temp table recon")
        df_cnt_df = spark.sql('select count(*) cnt from fast_rec_table')
        df_cts = df_cnt_df.head().cnt
        log_manager.log(message="Temp table created for splunk dashboard:", args={'Record count': df_cts})
        print("Dataframe count:", df_cts)

        if df_cts > 0:
            print("Adding recon data to splunk:")
            outputjsonrec = target_select_df.toJSON().collect()

            for item in outputjsonrec:
                print(item)
                log_args = json.loads(item)
                log_args['rule_name'] = section_name
                log_args['content_type'] = 'reconciliation_data'
                log_args['config_type'] = config_type
                timestamp_format = "%Y-%m-%dT%H:%M:%S.%fZ"
                element = datetime.datetime.strptime(log_args['job_run_time'], timestamp_format)
                datetime_eastern = element.astimezone(tz=pytz.timezone('US/Eastern'))
                print(datetime_eastern.strftime(timestamp_format))
                log_args['job_run_time_est'] = datetime_eastern.strftime(timestamp_format)
                #log_manager_business_recon.log(message=item, args=log_args)
                log_manager_business_recon.log(message='none', args=log_args)

            log_manager.log(message="outputjson flow completed:", args={'audit_write_sql': selectquery})

            print("Before inserting to recon table")

            audit_write_sql = "insert into " + athena_raw_database + "." + recon_table + \
                              " select " + audit_fields + ",  cast(UNIX_TIMESTAMP((from_utc_timestamp(CURRENT_timestamp(), 'EST5EDT')), 'yyyy-MM-dd HH:mm:ss') as timestamp),'" + guid + "' from  fast_isgie_temp_table"
            log_manager.log(message="Executing audit sql for fast recon:",
                            args={'audit_write_sql': audit_write_sql})
            print("Audit write sql:", audit_write_sql)
            spark.sql(audit_write_sql)
            print("After inserting to recon table")

        # Code to write the reconciliation data to splunk

        # isg_fast_df.unpersist()


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()