import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import configparser
from io import StringIO
import logging_service
import json
import re
import boto3
from botocore.exceptions import ClientError
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F

GUID_KEY = "guid"
REGION_KEY = "region"
PROCESS_KEY = "raw_to_pgdb"
CONFIG_BUCKET_NAME_KEY = "config_bucket"
SYS_CONFIG_KEY = "sys_config_file"
APP_CONFIG_KEY = "app_config_file"
FEED_CONFIG_KEY = "feed_config_file"
BATCH_SIZE = "jdbc_batch_size"
NUM_PARTITIONS = "jdbc_num_partitions"


def main():
    global BOTO3_AWS_REGION

    args = getResolvedOptions(sys.argv, ['JOB_NAME',
                                         GUID_KEY,
                                         REGION_KEY,
                                         CONFIG_BUCKET_NAME_KEY,
                                         SYS_CONFIG_KEY,
                                         FEED_CONFIG_KEY,
                                         APP_CONFIG_KEY,
                                         BATCH_SIZE,
                                         NUM_PARTITIONS])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    sys_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[SYS_CONFIG_KEY])
    app_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[APP_CONFIG_KEY])
    feed_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[FEED_CONFIG_KEY])

    guid = args[GUID_KEY]
    cloudwatch_log_group = sys_config.get(args[REGION_KEY], 'cloudwatch_log_group')
    boto3_aws_region = sys_config.get(args[REGION_KEY], 'boto3_aws_region')
    raw_bucket_name = sys_config.get(args[REGION_KEY], 'raw_bucket')
    client = boto3.client('logs', region_name=boto3_aws_region)

    batch_size = args[BATCH_SIZE]
    num_partitions = args[NUM_PARTITIONS]

    db_hostname = app_config.get(args[REGION_KEY], 'access_pgres_db_host')
    db_port = app_config.get(args[REGION_KEY], 'access_pgres_db_port')
    pgdb_schema = app_config.get(args[REGION_KEY], 'access_pgres_db_schema')
    pgres_database = app_config.get(args[REGION_KEY], 'access_pgres_db')
    pgdb_secret_name = app_config.get(args[REGION_KEY], 'access_pgres_secret_name')

    pgdb_insert_table = feed_config.get('transformation-rules', 'pgdb-insert-table')
    pgdb_insert_table = re.sub('{pgdb_schema}', pgdb_schema, pgdb_insert_table)

    feed = args[FEED_CONFIG_KEY].split('/')[-1].split('.')[0]

    log_manager = logging_service.LogManager(cw_loggroup=cloudwatch_log_group, cw_logstream=feed + '_' + guid,
                                             process_key=PROCESS_KEY, client=client,
                                             job=args[REGION_KEY] + '-isg-ie-stream-ingest-pgdb')
    log_manager.log(message="Starting the fast stream ingest pgdb historical load job",
                    args={"environment": args[REGION_KEY],
                          'event': feed,
                          'batch_size': batch_size,
                          'num_partitions': num_partitions})

    secret = get_secret(pgdb_secret_name, boto3_aws_region)
    secret = json.loads(secret)

    source_schema_config = feed_config.has_option('transformation-rules', 'source-schema') and feed_config.get(
        'transformation-rules', 'source-schema') or None
    pgdb_sql_config = feed_config.has_option('transformation-rules', 'pgdb-insert-sql') and feed_config.get(
        'transformation-rules', 'pgdb-insert-sql') or None
    json_raw_path_config = feed_config.has_option('transformation-rules', 'json-raw-file-path') and feed_config.get(
        'transformation-rules', 'json-raw-file-path') or None
    extract_composite_key_path = feed_config.has_option('transformation-rules',
                                                        'extract-composite-key-path') and feed_config.get(
        'transformation-rules', 'extract-composite-key-path') or None
    composite_key_alias = feed_config.has_option('transformation-rules', 'composite-key-alias') and feed_config.get(
        'transformation-rules', 'composite-key-alias') or None

    coalesce_min_partitions = feed_config.has_option('transformation-rules',
                                                     'coalesce-min-partitions') and feed_config.get(
        'transformation-rules', 'coalesce-min-partitions') or None

    composite_key_path_and_alias = feed_config.has_option('transformation-rules', 'composite-keys-path-and-alias') and feed_config.get(
        'transformation-rules', 'composite-keys-path-and-alias') or None

    # check if json raw file path exists
    json_raw_file_path_exists = False
    json_raw_file_path = None

    if json_raw_path_config:
        json_raw_file_path = feed_config.get('transformation-rules', 'json-raw-file-path')
        json_raw_file_path_exists = prefix_exits(raw_bucket_name, json_raw_file_path)

    if pgdb_sql_config and source_schema_config and coalesce_min_partitions and json_raw_file_path_exists:
        try:
            src_schema = feed_config.get('transformation-rules', 'source-schema')
            pgdb_insert_sql = feed_config.get('transformation-rules', 'pgdb-insert-sql')

            schema = StructType.fromJson(json.loads(src_schema))
            s3_src_path = "s3://" + raw_bucket_name + "/" + json_raw_file_path

            df = spark.read.schema(schema).json(s3_src_path)
            # Along with PolicyNumber few events have an additional column as primary key and for those events
            # if extract composite key is enabled we will need to extract the value form payload
            # and add it as new column to DF

            # below config only accepts one extra composite key extract
            if extract_composite_key_path:
                if composite_key_alias:
                    log_manager.log(message="Extracting composite key",
                                    args={"environment": args[REGION_KEY],
                                          'event': feed,
                                          'composite key path': extract_composite_key_path,
                                          'composite key alias': composite_key_alias})
                    df = df.withColumn(composite_key_alias, F.get_json_object("PayLoad", extract_composite_key_path))
                else:
                    log_manager.log(message="composite-key-alias needs to be specified when composite primary key is "
                                            "enabled.",
                                    args={"environment": args[REGION_KEY],
                                          'event': feed})

            # below config accepts multiple composite key extract
            if composite_key_path_and_alias:
                log_manager.log(message="Extracting composite key and setting alias",
                                args={"environment": args[REGION_KEY],
                                      'event': feed,
                                      'composite key path and alias': composite_key_path_and_alias})
                composite_keys_dict = json.loads(composite_key_path_and_alias)
                for key in composite_keys_dict:
                    df = df.withColumn(composite_keys_dict[key], F.get_json_object("PayLoad", key))

            df.registerTempTable("inputjsontbl")

            source_records_count = df.count()

            log_manager.log(message="No. of records fetched from S3 path " + json_raw_file_path,
                            args={"environment": args[REGION_KEY],
                                  'event': feed,
                                  "count": source_records_count})

            if source_records_count > 0:
                log_manager.log(message="Executing the pgdb insert query",
                                args={"environment": args[REGION_KEY],
                                      'event': feed})

                # Run pgdb load query
                access_table_df = spark.sql(pgdb_insert_sql)
                mode = "append"

                # if the input DF partitions is greater than the configured number do coalesce
                access_table_partitions = access_table_df.rdd.getNumPartitions()
                min_partitions = int(coalesce_min_partitions)
                log_manager.log(message="Input dataframe partitions",
                                args={"environment": args[REGION_KEY],
                                      'event': feed,
                                      'Partitions': access_table_partitions})
                if access_table_partitions > min_partitions:
                    access_table_df = access_table_df.coalesce(min_partitions)
                    log_manager.log(message="Successfully performed Coalesce to reduce the partitions",
                                    args={"environment": args[REGION_KEY],
                                          'event': feed,
                                          'Partitions': access_table_df.rdd.getNumPartitions()})

                jdbc_conn_string = "jdbc:postgresql://" + db_hostname + ":" + db_port + "/" + pgres_database + "?rewriteBatchedStatements=true&reWriteBatchedInserts=true"
                properties = {"user": secret.get('username'),
                              "password": secret.get('password'),
                              "driver": "org.postgresql.Driver",
                              "batchsize": batch_size,
                              "IsolationLevel": "READ_COMMITTED",
                              "rewriteBatchedStatements": "true",
                              "numPartitions": num_partitions,
                              "sslmode": "require",
                              "stringtype": "unspecified"}

                access_table_df.write.jdbc(url=jdbc_conn_string, table=pgdb_insert_table, mode=mode,
                                           properties=properties)
                log_manager.log(message="Successfully inserted historical data to pgdb for " + feed,
                                args={"environment": args[REGION_KEY],
                                      'event': feed})

        except Exception as e:
            error_message = redact_error_message(str(e), db_hostname, db_port, pgres_database, pgdb_insert_table)
            log_manager.log(message='Error performing pgdb insert for ' + feed,
                            args={"environment": args[REGION_KEY],
                                  'event': feed,
                                  'Exception': error_message})
            raise ValueError(error_message)

        try:
            # Get the count of records successfully inserted to database
            pgdb_success_count_query = "(select count(*) as hist_count from " + pgdb_insert_table + ") AS tmp"
            read_properties = {"user": secret.get('username'),
                               "password": secret.get('password'),
                               "driver": "org.postgresql.Driver",
                               "sslmode": "require"}
            result = spark.read.jdbc(url=jdbc_conn_string, table=pgdb_success_count_query,
                                     properties=read_properties)
            success_count = result.head().hist_count

            log_manager.log(message="Count of records inserted to pgdb for " + feed,
                            args={"environment": args[REGION_KEY],
                                  'event': feed,
                                  'count': success_count})

        except Exception as e:
            error_message = redact_error_message(str(e), db_hostname, db_port, pgres_database, pgdb_insert_table)
            log_manager.log(message='Error getting count of successfully inserted records to pgdb for ' + feed,
                            args={"environment": args[REGION_KEY],
                                  'event': feed,
                                  'Exception': error_message})
            raise ValueError(error_message)
    else:
        log_manager.log(message="Condition not satisfied to process any records.",
                        args={"environment": args[REGION_KEY],
                              'event': feed})
    job.commit()


def redact_error_message(error_message, db_host, db_port, db_name, db_table):
    error_message = error_message.replace(db_host, "***")
    error_message = error_message.replace(db_port, "***")
    error_message = error_message.replace(db_name, "***")
    error_message = error_message.replace(db_table, "***")
    return error_message


def prefix_exits(bucket, prefix):
    exists = False
    if bucket and prefix:
        s3_client = boto3.client('s3')
        res = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        if 'Contents' in res:
            exists = True
    return exists


def read_config(bucket, file_prefix):
    s3 = boto3.resource('s3')
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
