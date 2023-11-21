#######################################TASK-0#################################################
# IMPORTS

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql import types as T

import boto3

import json
import requests
from datetime import datetime as dt

import time
from botocore.exceptions import ClientError
glue_client = boto3.client('glue')

# SPARK CONFIG

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

s3_resource = boto3.resource('s3')

print('##############TASK-0-IMPORTS+SPARK_CONFIG-COMPLETED################')

#######################################TASK-1#################################################
# PARAMETERS

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATA_SOURCE_PATH', 'DATA_TARGET_PATH', 'DATA_STAGE_PATH',  'DATA_TARGET_PATH_PREFIX', 'DATASET_DATE'])

JOB_NAME = args['JOB_NAME']
DATA_SOURCE_PATH = args['DATA_SOURCE_PATH']
DATA_TARGET_PATH = args['DATA_TARGET_PATH']
DATA_STAGE_PATH = args['DATA_STAGE_PATH']
#SQL_SCRIPT_PATH_PREFIX = args['SQL_SCRIPT_PATH_PREFIX']
DATA_TARGET_PATH_PREFIX = args['DATA_TARGET_PATH_PREFIX']
DATASET_DATE = args['DATASET_DATE']

Client = JOB_NAME.split('_')[1]
Domain = JOB_NAME.split('_')[2]
EntityName = JOB_NAME.split('_')[3]
Source = JOB_NAME.split('_')[4]
Target = JOB_NAME.split('_')[6]
Action = JOB_NAME.split('_')[7]
Env = JOB_NAME.split('_')[8]

database = f"{Client}_{Domain}_{Action}_{Env}"
target_table = f"{EntityName}_{Target}"
crawler = f"crawler-{database}-{target_table}"

# SET PATH
data_source_path = DATA_SOURCE_PATH
target_write_path = DATA_TARGET_PATH + Env + '/' + DATA_TARGET_PATH_PREFIX + '/' + target_table + '/' + f'dataset_date={DATASET_DATE}/'
data_stage_path = DATA_STAGE_PATH + Env + '/' + DATA_TARGET_PATH_PREFIX + '/' + target_table + '/' + JOB_NAME.lower()

#sql_script_bucket = SQL_SCRIPT_PATH_PREFIX.split('//')[1].split('/', 1)[0]
#sql_script_key = SQL_SCRIPT_PATH_PREFIX.split('//')[1].split('/', 1)[1] + f'dataset_date={DATASET_DATE}/{JOB_NAME}.sql'

print (f"Client : {Client}")
print (f"Domain : {Domain}")
print (f"EntityName : {EntityName}")
print (f"Source : {Source}")
print (f"Target : {Target}")
print (f"Action : {Action}")
print (f"Env : {Env}")

print (f"data_source_path : {data_source_path}")
print (f"target_write_path : {target_write_path}")
print (f"data_stage_path : {data_stage_path}")
#print (f"SQL_SCRIPT_PATH_PREFIX : {SQL_SCRIPT_PATH_PREFIX}")
print (f"database : {database}")
print (f"target_table : {target_table}")
print (f"crawler : {crawler}")

print (f"DATASET_DATE : {DATASET_DATE}")

# raise Exception('Forced Exception')

print('##############TASK-1-PARAMETERS+SET_PATH-COMPLETED################')

#######################################TASK-2#################################################
# UDF
def read_s3_content(bucket_name, key):
    response = s3_resource.Object(bucket_name, key).get() 
    return response['Body'].read()

def run_crawler(crawler, database, target_table, dataset_date):
    def get_crawler_state(glue_client, crawler):
        response_get = glue_client.get_crawler(Name=crawler)
        state = response_get["Crawler"]["State"]
        return state
    def set_crawler_to_ready_state(glue_client, crawler):
        state = get_crawler_state(glue_client, crawler)
        state_previous = state
        print (f"Crawler {crawler} is {state.lower()}.")
        while (state != "READY") :
            time.sleep(30)
            state = get_crawler_state(glue_client, crawler)
            if state != state_previous:
                print (f"Crawler {crawler} is {state.lower()}.")
                state_previous = state
    try:
        tables_in_inbound_db = [tbl['tableName'] for tbl in spark.sql(f'''show tables in {database}''').select('tableName').collect()]
        if target_table in tables_in_inbound_db:
            
            if f'dataset_date={dataset_date}' in [partition['partition']  for partition in spark.sql(f'''show partitions {database}.{target_table}''').select('partition').collect()]:
                print (f"table -> {database}.{target_table} already present, partion {dataset_date} already exists")
            else:
                print (f"table -> {database}.{target_table} already present, adding partition {dataset_date}")
                spark.sql(f"alter table {database}.{target_table} add partition (dataset_date = '{dataset_date}')")
                print (f"alter table {database}.{target_table} add partition (dataset_date = '{dataset_date}')")
                print ('added partition successfully')
        else:
            print (f"table -> {database}.{target_table} not present")
            print ('crawler setting ready state')
            set_crawler_to_ready_state(glue_client, crawler)
            print ('started crawler')
            glue_client.start_crawler(Name=crawler)
            print ('crawler setting ready state')
            set_crawler_to_ready_state(glue_client, crawler)
    except Exception as e:
        raise Exception(e)

print('##############TASK-2-UDF-DEFINED################')

###################################TASK-3-DATA-EXTRACTION#######################################

url_city_val_api = 'https://www.cf.marutisuzukisubscribe.com/api/common/cities-brief'
url_dealer_api = 'https://cf.msilcrm.co.in/crm-common/api/common/msil/dms/dealer-master'

res_city_val = requests.get(url_city_val_api)

if res_city_val.status_code == 200:
    res_city_val_json = res_city_val.json()
    city_val_json = res_city_val_json['data']
    
df_city_val = spark.createDataFrame(data=city_val_json)
stateCds = [rw.stateCd for rw in df_city_val.select('stateCd').distinct().filter("stateCd is not Null").collect()]

dealer_headers = {
    'x-api-key': 'rzEw0mlWmf3zZKkhA01VnxPrXss6rSO5YedEPpye', 
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'
}
dealer_params = {
    'stateCd' : None
}

dealer_api_data = dict()
dealer_api_data['data'] = list()

for stateCd in stateCds:
    dealer_params['stateCd'] = stateCd
    res_dealer_master_val = requests.get(url=url_dealer_api, headers=dealer_headers, params=dealer_params)
    try:
        scd = res_dealer_master_val.json()['data'][0]['stateCd']
    except Exception as e:
        scd = 'default'
    finally:
        if scd.strip('\t') == stateCd.strip('\t'):
            dealer_api_data['data'] = dealer_api_data['data'] + res_dealer_master_val.json()['data']
        else:
            print (stateCd, scd, ' not matched')

df_dealer_api_data = spark.createDataFrame(data=dealer_api_data['data'])
#df_dealer_api_data.write.format('parquet').mode('overwrite').save('s3://msil-inbound-crm-raw-non-prod/lookup/dealer_master/')

print('##############TASK-3-DATA-EXTRACTION-COMPLETED################')

###################################TASK-4-DATA-TRANSFORMATION#######################################

###################################
YOUR CODE SHOULD BE HERE

raise Exception ("Forced Exception")
###################################
df = spark.sql(sqlQuery=query).withColumn('snapshot_dt', F.current_timestamp())
data_count = df.count()
print (f"data_count : {data_count}")
df.write.format('parquet').mode('overwrite').save(target_write_path)

print('##############TASK-4-DATA-TRANSFORMATION-COMPLETED################')

###################################TASK-7-REFRESH-ATHENA#######################################

run_crawler(crawler=crawler, database=database, target_table=target_table, dataset_date=DATASET_DATE)

print('##############TASK-7-REFRESH-ATHENA-COMPLETED################')

print('##############JOB-COMPLETED-SUCCESSFULLY################')
job.commit()