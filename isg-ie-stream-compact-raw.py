import boto3
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.context import DynamicFrame
from awsglue.job import Job
from pyspark.sql import SparkSession
import configparser
from io import StringIO
from io import BytesIO
import io
from avro.datafile import DataFileReader
import avro.io
from deepdiff import DeepDiff
import json
from pyspark.sql.utils import AnalysisException
import schema_services
import time
from datetime import date
from datetime import timedelta

# SRC_ROOT_DIR = 'src_root_dir'
# DEST_ROOT_DIR = 'dest_root_dir'
# SCHEMA_FILE = 'schema_file'

CONFIG_BUCKET_NAME_KEY = "config_bucket"
FEED_CONFIG_FILE_KEY = "feed_config_file"
SYS_CONFIG_KEY = "sys_config_file"
REGION_KEY = "region"
GUID_KEY = "guid"
FAST_EVENT_KEY = "fast_event"
APP_NAME_KEY = "application"


def main():
    global BOTO3_AWS_REGION

    args = getResolvedOptions(sys.argv,
                              ['JOB_NAME',
                               CONFIG_BUCKET_NAME_KEY,
                               FEED_CONFIG_FILE_KEY,
                               SYS_CONFIG_KEY,
                               FAST_EVENT_KEY,
                               APP_NAME_KEY,
                               REGION_KEY])

    sc = SparkContext()

    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    job = Job(glueContext)

    job.init(args['JOB_NAME'], args)

    sys_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[SYS_CONFIG_KEY])

    fast_event = args[FAST_EVENT_KEY]
    tooling_bucket = args[CONFIG_BUCKET_NAME_KEY]
    region = args[REGION_KEY]
    app_name = args[APP_NAME_KEY]

    raw_bucket_name = sys_config.get(args[REGION_KEY], 'raw_bucket')
    # event_ful_name = 'FAST_' + fast_event.upper() + '_RAW_ES'
    # crm_event_name = 'CRM-Annuities-' + region + '-' + fast_event
    athena_raw_database = sys_config.get(args[REGION_KEY], 'database')
    work_group_name = sys_config.get(args[REGION_KEY], 'work_group')
    pre_raw_bucket_name = sys_config.get(args[REGION_KEY], 'pre_raw_bucket')
    boto3_aws_region = sys_config.get(args[REGION_KEY], 'boto3_aws_region')

    if app_name == 'crm_annuities':
        crm_event_name = 'CRM-Annuities-' + region + '-' + fast_event + '-Change'
        print(crm_event_name)
        event_prefix = 'crm_ann'
        crm_src_root_dir = "s3://" + raw_bucket_name + '/crm_streaming/' + crm_event_name + '/'
        crm_dest_root_dir = "s3://" + raw_bucket_name + '/crm_ann_raw_hist/' + fast_event.lower() + '/'
        final_repair_sql = "msck repair table " + athena_raw_database + ".crm_annuities_" + fast_event.lower() + "_hist"

    if app_name == 'crm_life':
        crm_event_name = 'CRM-Life-' + region + '-' + fast_event + '-Change'
        print(crm_event_name)
        event_prefix = 'crm_life'
        crm_src_root_dir = "s3://" + raw_bucket_name + '/crm_streaming/' + crm_event_name + '/'
        crm_dest_root_dir = "s3://" + raw_bucket_name + '/crm_life_raw_hist/' + fast_event.lower() + '/'
        final_repair_sql = "msck repair table " + athena_raw_database + ".crm_life_" + fast_event.lower() + "_hist"

    if app_name[0:3] == 'crm':
        src_bucket_and_prefix = get_bucket_and_prefix(crm_src_root_dir)
        src_bucket = src_bucket_and_prefix[0]
        src_prefix = src_bucket_and_prefix[1]

        dest_bucket_and_prefix = get_bucket_and_prefix(crm_dest_root_dir)
        dest_bucket = dest_bucket_and_prefix[0]
        dest_prefix = dest_bucket_and_prefix[1]
        print(dest_bucket)
        print(dest_prefix)

        all_dest_partitions = get_all_partitions(dest_bucket, dest_prefix)
        leng_part = len(all_dest_partitions)
        print(leng_part)
        if leng_part == 0:
            print("came inside loop")
            all_yr_partitions = get_all_partitions(src_bucket, src_prefix)
            print(all_yr_partitions)
            for yr in all_yr_partitions:
                mn_prefix = yr
                all_mn_partitions = get_all_partitions(src_bucket, mn_prefix)
                print(all_mn_partitions)
                for mn in all_mn_partitions:
                    dt_prefix = mn
                    all_dt_partitions = get_all_partitions(src_bucket, dt_prefix)
                    print(all_dt_partitions)
                    for dt in all_dt_partitions:
                        destination_write(dt, raw_bucket_name, event_prefix, crm_event_name, fast_event, spark)
                        print("Write completed")
                        time.sleep(5)
        else:
            date_list = []
            for de in all_dest_partitions:
                fin = de.split("/")[2]
                date_fun = fin.split("=")[1]
                date_list.append(date_fun)
            max_date = max(date_list)
            print('max_date is ' + max_date)
            max_yr = str(max_date).split("-")[0]
            max_mn = str(max_date).split("-")[1]
            max_dt = str(max_date).split("-")[2]
            print(max_yr)
            print(max_mn)
            print(max_dt)
            all_yr_partitions = get_all_partitions(src_bucket, src_prefix)
            for yr in all_yr_partitions:
                s3_yr = yr.split("/")[2]
                print(s3_yr)
                if s3_yr == max_yr:
                    mn_prefix = yr
                    all_mn_partitions = get_all_partitions(src_bucket, mn_prefix)
                    print(all_mn_partitions)
                    for mn in all_mn_partitions:
                        s3_mn = mn.split("/")[3]
                        print(s3_mn)
                        if s3_mn == max_mn:
                            dt_prefix = mn
                            all_dt_partitions = get_all_partitions(src_bucket, dt_prefix)
                            print(all_dt_partitions)
                            for dt in all_dt_partitions:
                                s3_dt = dt.split("/")[4]
                                print(s3_dt)
                                if s3_dt >= max_dt:
                                    destination_write(dt, raw_bucket_name, event_prefix, crm_event_name, fast_event, spark)
                                    print("Write completed")
                                    time.sleep(5)
                        elif s3_mn > max_mn:
                            dt_prefix = mn
                            all_dt_partitions = get_all_partitions(src_bucket, dt_prefix)
                            print(all_dt_partitions)
                            for dt in all_dt_partitions:
                                destination_write(dt, raw_bucket_name, event_prefix, crm_event_name, fast_event, spark)
                                print("Write completed")
                                time.sleep(5)
                        else:
                            print("Incorrect date")
                elif s3_yr > max_yr:
                    mn_prefix = yr
                    all_mn_partitions = get_all_partitions(src_bucket, mn_prefix)
                    print(all_mn_partitions)
                    for mn in all_mn_partitions:
                            dt_prefix = mn
                            all_dt_partitions = get_all_partitions(src_bucket, dt_prefix)
                            print(all_dt_partitions)
                            for dt in all_dt_partitions:
                                destination_write(dt, raw_bucket_name, event_prefix, crm_event_name, fast_event, glueContext)
                                print("Write completed")
                                time.sleep(5)
                else:
                    print("incorrect date")
        run_query(boto3_aws_region, final_repair_sql, athena_raw_database,
                  "s3://" + pre_raw_bucket_name + "/tmp/athena-query-results/drop_partition", work_group_name)
        print("MSCK repair for hist table completed")

    elif app_name == 'fast':
        event_ful_name = 'FAST_' + fast_event.upper() + '_RAW_ES'

        src_root_dir = "s3://" + raw_bucket_name + '/fast/' + event_ful_name + '/'
        dest_root_dir = "s3://" + raw_bucket_name + '/fast_raw_hist/' + event_ful_name + '/'
        schema_file_path = "s3://" + tooling_bucket + '/' + region + '/code/config-files/landing-to-raw/fast_schema/' + fast_event + '_v1.avsc'

        src_bucket_and_prefix = get_bucket_and_prefix(src_root_dir)
        src_bucket = src_bucket_and_prefix[0]
        src_prefix = src_bucket_and_prefix[1]

        dest_bucket_and_prefix = get_bucket_and_prefix(dest_root_dir)
        dest_bucket = dest_bucket_and_prefix[0]
        dest_prefix = dest_bucket_and_prefix[1]

        # schema = get_schema(schema_file_path)
        # print('Schema is : ' + schema)

        uncompacted_dir_prefixes = get_uncompacted_folders(src_root_dir, dest_root_dir)

        print('uncompacted pfx == ')
        print(uncompacted_dir_prefixes)

        for uncomp_prefix in uncompacted_dir_prefixes:
            writer_schema = get_writer_schema(src_bucket, uncomp_prefix, spark, fast_event, athena_raw_database)
            # print("Writer schema : "+str(writer_schema)+"\n")
            # print(uncomp_prefix)
            partition_part = get_partition_from_prefix(uncomp_prefix)
            src_partition_path = 's3://' + src_bucket + '/' + uncomp_prefix
            dest_partition_path = dest_root_dir + partition_part + '/'

            print('\n' + 'Src =' + src_partition_path + '\n dest = ' + dest_partition_path)
            df = spark.read.format("avro").option("avroSchema", writer_schema).load(src_partition_path)
            cnt_parti = df.count()
            fin_parti = map_parti(cnt_parti)
            cDF = df.coalesce(fin_parti)
            cDF.write.mode('overwrite').format("avro").option("avroSchema", writer_schema).save(dest_partition_path)
            time.sleep(5)

        final_repair_sql = "msck repair table " + athena_raw_database + ".fast_" + fast_event + "_hist"
        # final_repair = spark.sql(final_repair_sql)
        run_query(boto3_aws_region, final_repair_sql, athena_raw_database,
                  "s3://" + pre_raw_bucket_name + "/tmp/athena-query-results/drop_partition", work_group_name)
        print("MSCK repair for hist table completed")

    else:
        if fast_event.upper() == 'DISCONNECT':
             if region == 'qa':
                 event_ful_name = 'UW_' + fast_event.upper() + '_RESULTS_BO_ES_QA'
             else:
                 event_ful_name = 'UW_' + fast_event.upper() + '_RESULTS_BO_ES'

             dest_loc = 'UW_' + fast_event.upper() + '_RESULTS_BO_ES'
             src_root_dir = "s3://" + raw_bucket_name + '/serv/' + event_ful_name + '/'
             dest_root_dir = "s3://" + raw_bucket_name + '/serv_raw_hist/' + dest_loc + '/'
        else:
            event_ful_name = 'GPSLIFE_' + fast_event.upper() + '_EVENTS_ES'
            src_root_dir = "s3://" + raw_bucket_name + '/serv/' + event_ful_name + '/'
            dest_root_dir = "s3://" + raw_bucket_name + '/serv_raw_hist/' + event_ful_name + '/'

        src_bucket_and_prefix = get_bucket_and_prefix(src_root_dir)
        src_bucket = src_bucket_and_prefix[0]
        src_prefix = src_bucket_and_prefix[1]

        dest_bucket_and_prefix = get_bucket_and_prefix(dest_root_dir)
        dest_bucket = dest_bucket_and_prefix[0]
        dest_prefix = dest_bucket_and_prefix[1]

        # schema = get_schema(schema_file_path)
        # print('Schema is : ' + schema)

        uncompacted_dir_prefixes = get_uncompacted_folders(src_root_dir, dest_root_dir)

        print('uncompacted pfx == ')
        print(uncompacted_dir_prefixes)

        for uncomp_prefix in uncompacted_dir_prefixes:
            partition_part = get_partition_from_prefix(uncomp_prefix)
            src_partition_path = 's3://' + src_bucket + '/' + uncomp_prefix
            dest_partition_path = dest_root_dir + partition_part + '/'

            print('\n' + 'Src =' + src_partition_path + '\n dest = ' + dest_partition_path)
            df = spark.read.json(src_partition_path)
            cnt_parti = df.count()
            fin_parti = map_parti(cnt_parti)
            cDF = df.coalesce(fin_parti)
            cDF.write.mode('overwrite').format("json").save(dest_partition_path)
            time.sleep(5)

        final_repair_sql = "msck repair table " + athena_raw_database + ".serv_gpslife_" + fast_event + "_hist"
        # final_repair = spark.sql(final_repair_sql)
        run_query(boto3_aws_region, final_repair_sql, athena_raw_database,
                  "s3://" + pre_raw_bucket_name + "/tmp/athena-query-results/drop_partition", work_group_name)
        print("MSCK repair for hist table completed")


def get_writer_schema(src_bucket, uncomp_prefix, spark, eventname, athena_raw_database):
    file_schema = get_latest_file_schema(src_bucket, uncomp_prefix)
    src_schema = schema_services.Schemaupdate.get_latest_push_schema(eventname, spark, athena_raw_database)

    # print('file schema' + str(file_schema)+'\n')
    # print('src schema' + str(src_schema)+'\n')
    # print('Data types...')
    # print(type(file_schema))
    # print(type(src_schema))
    schema_diff = DeepDiff(json.loads(src_schema), json.loads(file_schema))
    if not schema_diff:
        print('schema is similar\n')
        return schema_services.Schemaupdate.get_latest_isgie_schema(eventname, spark, athena_raw_database)
        # no difference
    else:
        print('schema is different')
        print(schema_diff)
        print('\n')
        schema_services.Schemaupdate.save_push_schema(eventname, file_schema, spark, athena_raw_database)
        isgie_schema = schema_services.Schemaupdate.get_null_schema(str(file_schema))
        # print('ISGIE Schema : ' + isgie_schema)
        schema_services.Schemaupdate.save_isgie_schema(eventname, isgie_schema, spark, athena_raw_database)
        return isgie_schema


def get_schema(schemapath):
    result = get_bucket_and_prefix(schemapath)
    bucket = result[0]
    key = result[1]

    s3 = boto3.resource('s3')

    obj = s3.Object(bucket, key)
    return obj.get()['Body'].read().decode('utf-8').replace('\n', '')

def destination_write(row_ent, raw_bucket_name, event_prefix, event_name, fast_event, spark):
    date_fnd = row_ent.split("/")[2] + '-' + row_ent.split("/")[3] + '-' + row_ent.split("/")[4]
    print(date_fnd)
    crm_fnd_src_root_dir = "s3://" + raw_bucket_name + '/crm_streaming/' + event_name + '/' + row_ent.split("/")[2] + '/' + row_ent.split("/")[3] + '/' + row_ent.split("/")[4] + '/'
    crm_fnd_dest_root_dir = "s3://" + raw_bucket_name + '/' + event_prefix +'_raw_hist/' + fast_event.lower() + '/event_creation_time=' + date_fnd + '/'
    df = spark.read.json(crm_fnd_src_root_dir)
    df.show()
    fdf = df.coalesce(10)
    fdf.write.mode('overwrite').format("json").save(crm_fnd_dest_root_dir)
    return

def split_fol(n_list, l):
    return n_list[:l], n_list[l:]


# assume input as  - fast/FAST_POLICYMASTER_RAW_ES/event_creation_time=2021-02-23/
def get_partition_from_prefix(prefix):
    partition = prefix.split("/")[-2]
    return partition


def get_second_max_partition(s3_bucket, prefix):
    client = boto3.client('s3')
    result = client.list_objects(Bucket=s3_bucket, Prefix=prefix, Delimiter='/')
    folderlist = []
    if 'CommonPrefixes' not in result:
        print(' -----------------------------------------------------------------')
        print('Warning - no partitions in destination folder!!')
        print(' -----------------------------------------------------------------')
        return
        # print(result)
    max_partition = ''
    second_max_partition = ''
    for i in range(len(result['CommonPrefixes'])):
        prefix = result['CommonPrefixes'][i]['Prefix']
        if max_partition == '':
            max_partition = prefix
        if prefix > max_partition:
            second_max_partition = max_partition
            max_partition = prefix
        else:
            if second_max_partition != '' and prefix > second_max_partition:
                second_max_partition = prefix
        folderlist.append(result['CommonPrefixes'][i]['Prefix'])

    print(max_partition)
    print(second_max_partition)

    if second_max_partition == '':
        return max_partition

    return second_max_partition


def get_all_partitions(s3_bucket, prefix):
    client = boto3.client('s3')
    result = client.list_objects(Bucket=s3_bucket, Prefix=prefix,
                                 Delimiter='/')
    print(result)
    folderlist = []
    if 'CommonPrefixes' not in result:
        print(' -----------------------------------------------------------------')
        print('Warning - no partitions in destination folder!!')
        print(' -----------------------------------------------------------------')
        return folderlist
    else:
        for i in range(len(result['CommonPrefixes'])):
            folderlist.append(result['CommonPrefixes'][i]['Prefix'])
    return folderlist


def get_bucket_and_prefix(full_s3_path: str):
    bucket = full_s3_path.split('/')[2]
    prefix = full_s3_path[full_s3_path.index(full_s3_path.split('/')[3]):]
    return (bucket, prefix)


# s3://pruvpcaws003-isg-raw-dev/fast/FAST_POLICYMASTER_RAW_ES/event_creation_time=2021-02-23/
def replace_src_date_with_dest(src_root_dir, dest_prefix: str):
    date_token = dest_prefix.split("=")[1]
    new_src_prefix = src_root_dir + "event_creation_time=" + date_token
    return new_src_prefix


def get_uncompacted_folders(src_root_dir, dest_root_dir):
    src_bucket_and_prefix = get_bucket_and_prefix(src_root_dir)
    src_bucket = src_bucket_and_prefix[0]
    src_prefix = src_bucket_and_prefix[1]
    all_src_partitions = get_all_partitions(src_bucket, src_prefix)

    # print('All src partitions : '+str(all_src_partitions))

    pending_partitions = []

    dest_bucket_and_prefix = get_bucket_and_prefix(dest_root_dir)
    dest_bucket = dest_bucket_and_prefix[0]
    dest_prefix = dest_bucket_and_prefix[1]
    destination_max_partition = get_second_max_partition(dest_bucket, dest_prefix)

    partition_exists_in_dest_folder = False

    if destination_max_partition is not None:
        partition_exists_in_dest_folder = True

        dest_max_partition_prefix = get_bucket_and_prefix(destination_max_partition)[1]
        # print('Destination max prefix ' + dest_max_partition_prefix)
        src_prefix_with_max_dt = replace_src_date_with_dest(src_prefix, dest_max_partition_prefix)
        # print(src_prefix_with_max_dt)

        # src_full_path_max_dt = "s3://" + src_bucket + "/" + src_prefix_with_max_dt
        for p in all_src_partitions:
            if p >= src_prefix_with_max_dt:
                pending_partitions.append(p)

    else:
        # Destination folder doesn't contain any partitions
        # So return all source partitions as list
        pending_partitions = all_src_partitions

    # print(pending_partitions)
    return pending_partitions


def map_parti(cnt):
    if cnt <= 1000:
        return 10
    if (cnt > 1000 and cnt <= 3000):
        return 30
    if (cnt > 3000 and cnt <= 5000):
        return 50
    if (cnt > 5000):
        return 200


def read_config(bucket, file_prefix):
    s3 = boto3.resource('s3')
    i = 0
    bucket = s3.Bucket(bucket)
    for obj in bucket.objects.filter(Prefix=file_prefix):
        buf = StringIO(obj.get()['Body'].read().decode('utf-8'))
        config = configparser.ConfigParser()
        config.readfp(buf)
        return config


# Returns the avro schema of latest object in a given partition
def get_latest_file_schema(bucket_name, folder_prefix):
    latest_obj = get_max_lastmodified_object(bucket_name, folder_prefix)
    return get_avro_schema(latest_obj)


def get_max_lastmodified_object(bucket_name, folder_prefix):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=folder_prefix)
    latest = None
    for page in page_iterator:
        if "Contents" in page:
            latest2 = max(page['Contents'], key=lambda x: x['LastModified'])
            if latest is None or latest2['LastModified'] > latest['LastModified']:
                latest = latest2
    return (bucket_name, latest['Key'])


def get_avro_schema(s3_bucket_and_prefix):
    bucket = s3_bucket_and_prefix[0]
    prefix = s3_bucket_and_prefix[1]
    client = boto3.client('s3')
    s3Object = client.get_object(Bucket=bucket, Key=prefix)
    avro_file_bytes = io.BytesIO(s3Object['Body'].read())
    reader = DataFileReader(avro_file_bytes, avro.io.DatumReader())
    # schema = reader.datum_reader.writer_schema
    avro_schema = reader.meta
    schema = avro_schema['avro.schema']
    return schema.decode()


def run_query(region, query, database, s3_output, work_group_name):
    client = boto3.client('athena', region_name=region)
    # response = client.start_query_execution(
    execution_id = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': s3_output,
        },
        WorkGroup=work_group_name
    )
    # print('Execution ID: ' + execution_id['QueryExecutionId'])
    print(execution_id['QueryExecutionId'])
    # sys.exit(0)
    if not execution_id:
        return

    while True:
        stats = client.get_query_execution(QueryExecutionId=execution_id['QueryExecutionId'])
        status = stats['QueryExecution']['Status']['State']
        print (status)
        if status == 'RUNNING':
            time.sleep(2)
        elif status == 'QUEUED':
            time.sleep(2)
        else:
            print ("job completed with status of " + status)
            break


if __name__ == '__main__':
    main()
    # latest = get_max_lastmodified_object('pruvpcaws003-isg-raw-dev', 'fast/FAST_POLICYMASTER_RAW_ES/event_creation_time=2021-03-02/')
    # print(latest)
    # schema= get_avro_schema(latest)
    # print(schema)

# get_bucket_and_prefix("s3://bucketname/abcd/efg/")
# output  = replace_src_date_with_dest("fast/FAST_POLICYMASTER_RAW_ES/event_creation_time=2021-02-23/", "fast_raw_hist/FAST_POLICYMASTER_RAW_ES/event_creation_time=2021-02-19/")
# print (output)

# get_uncompacted_folders("s3://pruvpcaws003-isg-raw-dev/fast/FAST_DAILYBATCH_RAW_ES/",
#                        "s3://pruvpcaws003-isg-raw-dev/test/dest/")
