"""Part A. Time Analysis (25%)
Create a bar plot showing the number of transactions occurring every month between the start and end of the dataset.

Create a bar plot showing the average value of transaction in each month between the start and end of the dataset.

Note: As the dataset spans multiple years and you are aggregating together all transactions in the same month, make sure to include the year in your analysis.

Note: Once the raw results have been processed within Spark you may create your bar plot in any software of your choice (excel, python, R, etc.)
"""
import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from operator import add
from time import gmtime, strftime

if __name__ == "__main__":
    
    ########### SET UP JOB AND CLUSTER ACCESS ###########

    spark = SparkSession\
        .builder\
        .appName("Part-A")\
        .getOrCreate()
        
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
    ########### FILTER FUNCS ###########
    
    def good_transaction(line):
        try:
            fields = line.split(',')
            
            # checking if expected number of fields are present
            if len(fields)!=15:
                return False
            
            # checking for required formats
            float(fields[7]) # value
            int(fields[11]) # block_timestamp
            
            return True
        except:
            return False
    
    ########### STEP 1 ###########
    
    # Read and clean all transactions
    lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    lines_count = lines.count()
    print("ethereum_partA_log: lines_count", lines_count)
    # lines_count 369817359
    
    print("ethereum_partA_log: lines.take(2)", lines.take(2))
    # ['hash 0, nonce 1, block_hash 2, block_number 3, transaction_index 4, from_address 5, to_address 6, value 7, gas 8, gas_price 9, input 10, block_timestamp 11, max_fee_per_gas 12, max_priority_fee_per_gas 13, transaction_type 14', '0x01f7583ce239ad19682ccbadb21d69a03cf7be333f4be9c02233874e3f79bf9d,0,0x9b430b5151969097fd7e5c29726a3afe680ab56f0f14a49a8f045b66392add15,46400,0,0xf55cdbdf978f539ea4eee0621f7f1a0f9f00f7d9,0x6baf48e1c966d16559ce2dddb616ffa72004851e,5000000000000000,21000,500000000000,0x,1438922527,,,0']
    
    # Cleaning for good transaction lines
    clean_lines = lines.filter(good_transaction)
    clean_lines_count = clean_lines.count()
    print("ethereum_partA_log: clean_lines_records", clean_lines_count)
    # clean_lines_records 369817358
    
    print("ethereum_partA_log:  number of malformed records", lines_count - clean_lines_count)
    # number of malformed records 1
    
    ########### STEP 2 ###########
    
    # Mapping block_timestamp (index: 11) as YYYY/MM key with (value (index: 7), 1) as value
    map_transactions_mY = clean_lines.map(lambda l: (strftime("%Y/%m", gmtime(int(l.split(',')[11]))), (float(l.split(',')[7]), 1)))
    print("ethereum_partA_log: map_transactions_mY.take(1)", map_transactions_mY.take(1))
    # [('2015/08', (5000000000000000.0, 1))]
    
    ########### STEP 3 ########### 
    
    # Reduce by summing all values and counts
    total_transactions_mY = map_transactions_mY.reduceByKey(lambda x1, x2: (x1[0]+x2[0], x1[1]+x2[1]))
    print("ethereum_partA_log: total_transactions_mY.take(1)", total_transactions_mY.take(1))
    # [('2015/11', (1.396303238107546e+25, 234733))]
    
    ########### STEP 4 ###########
    
    # Keep the transaction count and average the transaction value by dividing total value by total count
    avg_transactions_mY = total_transactions_mY.map(lambda x: (x[0], int(x[1][1]), float(x[1][0])/int(x[1][1]))).sortBy(lambda mY_val: mY_val[0], ascending=True)
    print("ethereum_partA_log: avg_transactions_mY.take(1)", avg_transactions_mY.take(1))
    # [('2015/08', 85609, 4.8052118459597475e+20)]
    
    ########### SAVING RESULTS ###########

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_PART_A/time_analysis.txt')
    my_result_object.put(Body=json.dumps(avg_transactions_mY.collect()))
    
    spark.stop()
