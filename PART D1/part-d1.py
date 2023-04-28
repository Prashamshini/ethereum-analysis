"""Part D. Data exploration (40%)

The final part of the coursework requires you to explore the data and perform some analysis of your choosing. Below are some suggested ideas for analysis which could be undertaken, along with an expected grade for completing it to a good standard. You may attempt several of these tasks or undertake your own.

Data Overhead: The blocks table contains a lot of information that may not strictly be necessary for a functioning cryptocurrency e.g. logs_bloom, sha3_uncles, transactions_root, state_root, receipts_root. Analyse how much space would be saved if these columns were removed. Note that all of these values are hex_strings so you can assume that each character after the first two requires four bits (this is not the case in reality but it would be possible to implement it like this) as this will affect your calculations. (20%/40%)
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
        .appName("Part-D-1")\
        .getOrCreate()
        
    # shared read-only object bucket containing datasets
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
    
    def good_block(line):
        try:
            fields = line.split(',')
            
            # checking if expected number of fields are present and not header
            if len(fields) != 19 or fields[0] == "number":
                return False
            
            return True
        except:
            return False
    
    ########### STEP 1 ###########
    
    # Read and clean all blocks
    blocks_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv")
    blocks_lines_count = blocks_lines.count()
    print("ethereum_partD_i_log: blocks_lines_count", blocks_lines_count)
    # blocks_lines_count 7000002
    
    print("ethereum_partD_i_log: blocks_lines.take(2)", blocks_lines.take(2))
    # ['number 0, hash 1, parent_hash 2, nonce 3, sha3_uncles 4, logs_bloom 5, transactions_root 6, state_root 7, receipts_root 8, miner 9, difficulty 10, total_difficulty 11, size 12, extra_data 13, gas_limit 14, gas_used 15, timestamp 16, transaction_count 17, base_fee_per_gas 18', '400,<hash>,<parent_hash>,0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421,0xcefeb930c0faeb5477ab34ae5333f143b91c75a0b8f5973c813546e0112435a3,0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421,0x28921e4e2c9d84f4c0f0c0ceb991f45751a0fe93,20742135970,7556615279346,539,0x476574682f76312e302e302f6c696e75782f676f312e342e32,5000,0,1438271032,0,']
    
    # Cleaning for good block lines
    blocks_clean_lines = blocks_lines.filter(good_block)
    blocks_clean_lines_count = blocks_clean_lines.count()
    print("ethereum_partD_i_log: blocks_clean_lines_records", blocks_clean_lines_count)
    # blocks_clean_lines_records 7000001
    
    print("ethereum_partD_i_log: number of lines removed", blocks_lines_count - blocks_clean_lines_count)
    # number of lines removed 1
    
    ########### STEP 2 ###########
    
    # Estimate the length of each each field by taking the length of each field, subtract 2 (for 0x) and multiply that by 4 (number of bits)
    map_block = blocks_clean_lines.map(lambda l: (1, ((len(l.split(',')[4])-2)*4, (len(l.split(',')[5])-2)*4, (len(l.split(',')[6])-2)*4, (len(l.split(',')[7])-2)*4, (len(l.split(',')[8])-2)*4)))
    print("ethereum_partD_i_log: map_block.take(1)", map_block.take(1))
    # [(1, (256, 2048, 256, 256, 256))]
    
    ########### STEP 3 ###########
    
    # Sum the number of bits for each column
    sum_block = map_block.reduceByKey(lambda x1, x2: (x1[0]+x2[0], x1[1]+x2[1], x1[2]+x2[2], x1[3]+x2[3], x1[4]+x2[4]))
    print("ethereum_partD_i_log: sum_block.take(1)", sum_block.take(1))
    # [(1, (1792000256, 14336002048, 1792000256, 1792000256, 1792000256))]
    
    ########### STEP 4 ###########
    
    # Total all the columns to find the data overhead
    total_block = sum_block.map(lambda l: ('total', l[1][0]+l[1][1]+l[1][2]+l[1][3]+l[1][4]))
    print("ethereum_partD_i_log: total_block.take(1)", total_block.take(1))
    # [('total', 21504003072)]
    
    ########### SAVING RESULTS ###########

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object1 = my_bucket_resource.Object(s3_bucket,'ethereum_PART_D1/data_overhead_total.txt')
    my_result_object1.put(Body=json.dumps(total_block.collect()))
    
    my_result_object2 = my_bucket_resource.Object(s3_bucket,'ethereum_PART_D1/data_overhead_col.txt')
    my_result_object2.put(Body=json.dumps(sum_block.collect()))
    
    spark.stop()
