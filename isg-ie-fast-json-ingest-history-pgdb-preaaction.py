import boto3
import sys
import configparser
from io import StringIO
import logging_service
from awsglue.utils import getResolvedOptions
import re
import json
from pg import DB
from botocore.exceptions import ClientError

GUID_KEY = "guid"
REGION_KEY = "region"
PROCESS_KEY = "raw_to_pgdb-preaction"
CONFIG_BUCKET_NAME_KEY = "config_bucket"
FEED_CONFIG_FILE_KEY = "feed_config_file"
SYS_CONFIG_KEY = "sys_config_file"
APP_CONFIG_KEY = "app_config_file"

args = getResolvedOptions(sys.argv, [CONFIG_BUCKET_NAME_KEY,
                                     FEED_CONFIG_FILE_KEY,
                                     SYS_CONFIG_KEY,
                                     APP_CONFIG_KEY,
                                     REGION_KEY,
                                     GUID_KEY])


def main():
    guid = args[GUID_KEY]
    feed_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[FEED_CONFIG_FILE_KEY])
    sys_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[SYS_CONFIG_KEY])
    app_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[APP_CONFIG_KEY])
    cloudwatch_log_group = sys_config.get(args[REGION_KEY], 'cloudwatch_log_group')
    boto3_aws_region = sys_config.get(args[REGION_KEY], 'boto3_aws_region')
    raw_bucket_name = sys_config.get(args[REGION_KEY], 'raw_bucket')
    client = boto3.client('logs', region_name=boto3_aws_region)
    feed = args[FEED_CONFIG_FILE_KEY].split('/')[-1].split('.')[0]

    # log that the preaction job is starting
    log_manager = logging_service.LogManager(cw_loggroup=cloudwatch_log_group, cw_logstream=feed + '_' + guid,
                                             process_key=PROCESS_KEY, client=client,
                                             job=args[REGION_KEY] + '-isg-ie-app-access-pgdb-preaction')

    log_manager.log(message="Starting the fast stream ingest pgdb historical load pre-action job for " + feed,
                    args={"environment": args[REGION_KEY],
                          'event': feed})

    db_hostname = app_config.get(args[REGION_KEY], 'access_pgres_db_host')
    db_port = app_config.get(args[REGION_KEY], 'access_pgres_db_port')
    pgdb_schema = app_config.get(args[REGION_KEY], 'access_pgres_db_schema')
    pgres_database = app_config.get(args[REGION_KEY], 'access_pgres_db')
    pgdb_secret_name = app_config.get(args[REGION_KEY], 'access_pgres_secret_name')

    secret = get_secret(pgdb_secret_name, boto3_aws_region)
    secret = json.loads(secret)

    pgdb_delete_sql = feed_config.has_option('transformation-rules', 'pgdb-delete-sql') and feed_config.get(
        'transformation-rules', 'pgdb-delete-sql') or None
    json_raw_path_config = feed_config.has_option('transformation-rules', 'json-raw-file-path') and feed_config.get(
        'transformation-rules', 'json-raw-file-path') or None

    if pgdb_delete_sql and json_raw_path_config:
        try:
            json_raw_file_path = feed_config.get('transformation-rules', 'json-raw-file-path')
            s3_objects_count = get_count_of_historical_objects(raw_bucket_name, json_raw_file_path)

            if s3_objects_count > 0:
                db = DB(dbname=pgres_database, host=db_hostname, port=int(db_port), user=secret.get('username'),
                        passwd=secret.get('password'))
                pgdb_delete_sql = feed_config.get('transformation-rules', 'pgdb-delete-sql')
                pgdb_table_name = pgdb_delete_sql.split(".")[1]
                proc_call = "CALL pdctrpt.kill_blocking_users_of_object(pv_schema_name := '" + pgdb_schema + "', pv_object_name := '" + pgdb_table_name + "');"
                pgdb_delete_sql = re.sub('{pgdb_schema}', pgdb_schema, pgdb_delete_sql)

                log_manager.log(message="Starting pgdb remove blocking users",
                                args={"environment": args[REGION_KEY],
                                      'event': feed})
                db.query(proc_call)

                log_manager.log(message="Starting pgdb truncate query",
                                args={"environment": args[REGION_KEY],
                                      'event': feed})
                db.query(pgdb_delete_sql)

                log_manager.log(message="pgdb truncate query completed successfully",
                                args={"environment": args[REGION_KEY],
                                      'event': feed})
            else:
                log_manager.log(
                    message="Truncate did not happen as there are no objects in s3 to insert to pgdb for " + feed,
                    args={"environment": args[REGION_KEY],
                          'event': feed})
        except Exception as e:
            log_manager.log(message='Error wile truncating ' + feed + ' table',
                            args={"environment": args[REGION_KEY],
                                  'event': feed,
                                  'Exception': str(e)})
            raise e
    else:
        log_manager.log(message="Condition not satisfied to truncate ." + feed,
                        args={"environment": args[REGION_KEY],
                              'event': feed})


def get_count_of_historical_objects(bucket, raw_path):
    s3 = boto3.client('s3')
    count = 0
    response = s3.list_objects(
        Bucket=bucket,
        Prefix=raw_path,
    )
    if 'Contents' in response:
        count = len(response['Contents'])
    return count


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
