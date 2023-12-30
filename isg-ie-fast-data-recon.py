from os import device_encoding
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import sys
import configparser
from io import StringIO
from awsglue.job import Job
import boto3
import re
import logging_service
from botocore.exceptions import ClientError
from pyspark.sql.functions import explode
from pyspark.sql.functions import concat_ws, lit, col, when, row_number, regexp_extract,regexp_replace, date_format
import json
from pyspark.sql.window import Window
import ast
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from pytz import timezone
from pyspark.sql.types import *
import pandas as pd
from typing import Dict
from pyspark.sql import DataFrame as SDF
from pyspark.sql.functions import countDistinct
import pytz
from pyspark.sql.types import StringType


CONFIG_BUCKET_NAME_KEY = "config_bucket"
SYS_CONFIG_KEY = "sys_config_file"
APP_CONFIG_KEY = "app_config_file"
FEED_CONFIG_KEY = "feed_config_file"
REGION_KEY = "region"
FINAL_DATA= "recon_result_key_prefix"
EVENT_NAME = "event_name"
SOURCE_DATETIME_FORMAT="%Y-%m-%dT%H:%M:%S.%f"
TARGET_DATETIME_FORMAT="%Y-%m-%d %H:%M:%S.%f"

newYorkTz = pytz.timezone("America/New_York")
today = datetime.now(newYorkTz).date()
yesterday = today - timedelta(days=1)

#comparing the source and target tables for the possible mismatches and storing them in the following location "s3://{source_bucket}/{final_data}/{table_name}/{yesterday}/comparison_details/"
# def compare_tables(join_key, columns, primary_key,  source_table1, target_table1, table_name, source_bucket, final_data):
#     df1 = source_table1
#     df2 = target_table1
#     dfj = df1.alias("src").join(df2.alias("tgt"),join_key,"full_outer")
#     dfj.cache()

#     df = dfj.withColumn("difference",concat_ws(" ",*map(lambda x:(when(((lit(col("src."+x))!=(lit(
#         col("tgt."+x))))), (concat_ws(" ", lit(x),lit("src1"), lit(col("src."+x)), lit("tgt1"),
#                                     lit(col("tgt."+x)))))), columns)))
#     dfj.unpersist()
#     df.cache()
#     dffinal = df.filter(df.difference != "")
#     global time_of_run
#     global result

#     final_comparison_result = dffinal.select("difference", *join_key)
#     final_comparison_result = final_comparison_result.withColumn(
#         "table_name", lit(table_name))\
#         .withColumn("primary_key",lit(",".join(join_key)))\
#         .withColumn("primary_key_id", concat_ws(",", *map(lambda x:col(
#         x), join_key))) \
#         .withColumn("date", lit(str(today)))
#     result = final_comparison_result.count()
#     return result

#Checking for the file existance in the specified s3 location
def file_exist(bucket, prefix):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    for object_summary in bucket.objects.filter(Prefix=prefix):
        return True
    return False

# function to convert the timestamp datatype to the datetype
def convert(df):
    for each_column in df.columns:
        if each_column.upper() == 'TIMESTAMP' :
            df = df.withColumn(each_column, col(each_column).cast(StringType()))
    return df

def main():
    global BOTO3_AWS_REGION
    args = getResolvedOptions(sys.argv,
                              ['JOB_NAME', CONFIG_BUCKET_NAME_KEY,SYS_CONFIG_KEY,REGION_KEY,APP_CONFIG_KEY,FEED_CONFIG_KEY,EVENT_NAME,FINAL_DATA])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    event_name= args[EVENT_NAME]    
    final_data = args[FINAL_DATA]
    region=args[REGION_KEY]

    app_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[APP_CONFIG_KEY])
    sys_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[SYS_CONFIG_KEY])
    feed_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[FEED_CONFIG_KEY])
    boto3_aws_region = sys_config.get(region, 'boto3_aws_region')
    cloudwatch_log_group = sys_config.get(region, 'cloudwatch_log_group')
    #reading the variables from the config files
    pgdb_schema = app_config.get(region, 'access_pgres_db_schema')
    db_hostname = app_config.get(region, 'access_pgres_db_host')
    db_port = app_config.get(region, 'access_pgres_db_port')    
    pgres_database = app_config.get(region, 'access_pgres_db')
    pgdb_secret_name = app_config.get(region, 'access_pgres_secret_name')
    result_bucket = sys_config.get(region, 'pre_raw_bucket')
    source_bucket = sys_config.get(region, 'raw_bucket')
    secret = get_secret(pgdb_secret_name, boto3_aws_region)
    secret = json.loads(secret)
    jdbc_conn_string = "jdbc:postgresql://" + db_hostname + ":" + db_port + "/" + pgres_database + "?rewriteBatchedStatements=true&reWriteBatchedInserts=true"
    read_properties = {"user": secret.get('username'),
                        "password": secret.get('password'),
                        "driver": "org.postgresql.Driver",
                        "sslmode": "require"}
    client = boto3.client('logs', region_name=boto3_aws_region)
    log_manager = logging_service.LogManager(cw_loggroup=cloudwatch_log_group, cw_logstream="fast-data-recon-"+event_name ,
                                             process_key="isgie_automation", client=client,
                                             job=region + '-isg-ie-fast-raw-to-pgdb-recon')
    primary_key = ast.literal_eval(feed_config.get(event_name, 'Primary_key'))
    field_key = ast.literal_eval(feed_config.get(event_name, 'Source_Field_Key'))
    sql_field_key = ast.literal_eval(feed_config.get(event_name, 'Target_Field_Key'))
    pgdb_table_name = feed_config.get(event_name, 'Target_Table_Name')
    comparing_columns = ast.literal_eval(feed_config.get(event_name, 'Compare_Missing_Columns'))
    source_data_key_prefix = str(feed_config.get(event_name, 'FolderName'))
    if (region == 'qa'):
        source_data_key_prefix = source_data_key_prefix+"_QA"
    To_TimStamp_Conversion = ast.literal_eval(feed_config.get(event_name, 'To_TimeStamp_Conversion'))
    #checking for the ExprValues existance in the feed config file
    Selectvalues = feed_config.has_option(event_name, 'ExprValues') and ast.literal_eval(feed_config.get(event_name, 'ExprValues')) or None

    try:
        #checking for the file in the s3 location with today or yesterday date
        if (file_exist(source_bucket, f"{source_data_key_prefix}/event_creation_time={str(yesterday)}/") or file_exist(source_bucket, f"{source_data_key_prefix}/event_creation_time={str(today)}/")):
            fast_query = "(select * from " + pgdb_schema+"."+pgdb_table_name +") as tmp"
            
            current_time = datetime.now(newYorkTz)
            current_time_minus_24hrs = current_time - timedelta(days=1)
            current_time_str=current_time.strftime(SOURCE_DATETIME_FORMAT)
            current_time_minus_24hrs_str=current_time_minus_24hrs.strftime(SOURCE_DATETIME_FORMAT)

            target_df = spark.read.jdbc(url=jdbc_conn_string, table=fast_query, properties=read_properties)            
            target_df = target_df.filter((F.col(sql_field_key[0])).between (current_time_minus_24hrs.strftime(TARGET_DATETIME_FORMAT), current_time.strftime(TARGET_DATETIME_FORMAT)))
            target_df = target_df.select(*primary_key,*[F.col(sql_field_key[each]).alias(field_key[each]) for each in range(len(sql_field_key))])
            #Reading the files existing from the s3 location
            if (file_exist(source_bucket, f"{source_data_key_prefix}/event_creation_time={str(yesterday)}/") and file_exist(source_bucket, f"{source_data_key_prefix}/event_creation_time={str(today)}/")):
                #reading source data: data platform data
                file_list= [f"s3://{source_bucket}/{source_data_key_prefix}/event_creation_time={str(yesterday)}/",f"s3://{source_bucket}/{source_data_key_prefix}/event_creation_time={str(today)}/"]
            elif file_exist(source_bucket, f"{source_data_key_prefix}/event_creation_time={str(yesterday)}/") and not file_exist(source_bucket, f"{source_data_key_prefix}/event_creation_time={str(today)}/"):
                file_list= [f"s3://{source_bucket}/{source_data_key_prefix}/event_creation_time={str(yesterday)}/"]
            else:
                file_list= [f"s3://{source_bucket}/{source_data_key_prefix}/event_creation_time={str(today)}/"]
            source_df = spark.read.json(file_list)
            #if ExprValues exists then we need to extract the column(specified in the Selectvalues with the F.expr spark function) from the dataframe and name the column accordingly else read the data withoutany extractions
            if Selectvalues:
                source_df= source_df.select(*(set(primary_key) - set(Selectvalues)),*[F.expr(Selectvalues[each]).alias(each) for each in Selectvalues],*(set(field_key) - set(Selectvalues)))
                source_df = source_df.filter((F.col(field_key[0]) >= current_time_minus_24hrs_str)  & (F.col(field_key[0]) < current_time_str))
            else:
                source_df = source_df.filter((F.col(field_key[0]) >= current_time_minus_24hrs_str)  & (F.col(field_key[0]) < current_time_str))
                source_df = source_df.select(*primary_key ,  *field_key)

            source_count= source_df.count()
            target_count= target_df.count()
            wind1= Window.partitionBy(primary_key).orderBy(F.col(field_key[0]).desc())
            # get the latest record per primary key value
            source_df_w = source_df.withColumn("row_num", F.row_number().over(wind1)).filter(F.col("row_num") ==1 ).drop("row_num")
            so_count= source_df_w.count()
            #log_manager.log(message=f' S3 distinct object count between {current_time_minus_24hrs_str} and {current_time_str} ', args={"env": region, 'eventName': event_name, 'Count': str(so_count)})
            #log_manager.log(message=f' No. of records in pgdb table between {current_time_minus_24hrs.strftime(TARGET_DATETIME_FORMAT)} and {current_time.strftime(TARGET_DATETIME_FORMAT)}', args={"env": region, 'eventName': event_name, 'Count': str(target_count)})

            # get the missing records in target and write to s3
            for eachkey in primary_key:
                if not dict(source_df_w.dtypes)[eachkey] == dict(target_df.dtypes)[eachkey]:
                    source_df_w = source_df_w.withColumn(eachkey, col(eachkey).cast(dict(target_df.dtypes)[eachkey]))

           
            missingrecords= source_df_w.join(target_df, on=primary_key , how="leftanti")
            missingrecords = missingrecords.select([*primary_key,*field_key])            
            missing_records_count = missingrecords.count()

            if missing_records_count > 0:
                missingrecords = missingrecords.toPandas().to_csv("s3://"+result_bucket+"/"+final_data+"/missing_records_"+pgdb_table_name+"_"+str(yesterday)+".csv", index= False)

            #Convert the column to the the timestamp which is mentioned in the To_TimStamp_Conversion

            # for each_col in To_TimStamp_Conversion:
            #      source_df_w = source_df_w.withColumn(each_col, col(each_col).cast(DateType()))
            for each_column in target_df.dtypes:
                 if not each_column[1] == source_df_w.schema[each_column[0]].dataType:
                     source_df_w = source_df_w.withColumn(each_column[0], col(each_column[0]).cast(each_column[1]))


            for each_col in To_TimStamp_Conversion:
                #source_df_w=  source_df_w.withColumn(each_col, regexp_extract(source_df_w[each_col], '[+,-]?\d+\.?\d{0,5}', 0))
                source_df_w=  source_df_w.withColumn(each_col, regexp_replace(source_df_w[each_col], 'T',' '))
            for each_col in To_TimStamp_Conversion:                
                #target_df=  target_df.withColumn(each_col, regexp_extract(target_df[each_col].cast(StringType()), '[+,-]?\d+\.?\d{0,5}', 0))
                target_df=  target_df.withColumn(each_col, target_df[each_col].cast(StringType()))
       

            # for each_col in To_TimStamp_Conversion:
            #     target_df=  target_df.withColumn(each_col, F.to_timestamp(F.col(each_col)))

            #Conver the datatypes of the source and target primary/composite keys if the datatypes from source to target is not matching.
            #target_df =convert(target_df)
           

            #converting the timstampe datatype columns with the datetype columns
            #source_df_w =convert(source_df_w)

            # print("target_df schema after the string conversion")
            # print(target_df.printSchema())
            difference_total_row_count= so_count - target_count
            source_column_count= len(source_df_w.columns)
            target_column_count= len(target_df.columns)
            difference_total_column_count= source_column_count - target_column_count
            # mismatched_record_count= compare_tables(primary_key, comparing_columns,primary_key, source_df_w, target_df, pgdb_table_name, result_bucket, final_data)
            # create_df= [[pgdb_table_name, so_count, target_count, difference_total_row_count, source_column_count, target_column_count, difference_total_column_count, mismatched_record_count]]
            # create_df_header= ['TableName', 'source_row_count', 'target_row_count', 'difference_total_row_count', 'source_column_count', 'target_column_count', 'difference_total_column_count', 'mismatched_record_count']
            # pf= pd.DataFrame(create_df, columns= create_df_header)
            # create_result_df=  spark.createDataFrame(pf)
            # create_result_df.show(truncate=False)
            # create_result_df.toPandas().to_csv("s3://"+result_bucket+"/"+final_data+"/comparison_summary_"+pgdb_table_name+"_"+str(yesterday)+".csv", index=False)
            # log_manager.log(message=f' Reconciliation Summary - {str(yesterday)}: \n  source_row_count  - {str(so_count)}: \n  target_row_count - {str(target_count)}: \n  difference_total_row_count - {str(difference_total_row_count)}: \n  source_column_count - {str(source_column_count)}: \n target_column_count - {str(target_column_count)}: \n difference_total_column_count - {str(difference_total_column_count)}: \n mismatched_record_count - {str(mismatched_record_count)} ',
            #                  args={"env": region, 'eventName': event_name, 'Source Count': str(difference_total_row_count)})
            log_manager.log(message=f' Reconciliation Summary for the period {current_time_minus_24hrs_str} and {current_time_str}',
                              args={"env": region, 'eventName': event_name,'sourceCount': str(source_count),'sourceDistinctCount': str(so_count),
                              'targetCount': str(target_count)})

        spark.stop()

    except Exception as e:
        raise e

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

def get_secret(secret_name, region_name):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        print(e)
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.

        if 'SecretString' in get_secret_value_response:
            secret_value = get_secret_value_response['SecretString']
        else:
            secret_value = base64.b64decode(get_secret_value_response['SecretBinary'])
        return secret_value


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()

'''
This job runs to reconcile the data from source to target along side finding out the missing records and storing them for further analysis. It also stores the summary between the source and target tables where it stores the following information ['TableName', 'source_row_count', 'target_row_count', 'difference_total_row_count', 'source_column_count', 'target_column_count', 'difference_total_column_count', 'mismatched_record_count']. We can specify the table name in the job parameters section that we plan to run.

Note: For the columns for some sepcific tables where the column are not directly extracted can be specified the ExprValues section in the config file devfast_pm_cvq_datareconcile.conf.

'''
