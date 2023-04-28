"""Part C. Top Ten Most Active Miners (10%)

Evaluate the top 10 miners by the size of the blocks mined. This is simpler as it does not require a join. You will first have to aggregate blocks to see how much each miner has been involved in. You will want to aggregate size for addresses in the miner field. This will be similar to the wordcount that we saw in Lab 1 and Lab 2. You can add each value from the reducer to a list and then sort the list to obtain the most active miners.
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
        .appName("Part-C")\
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
            
            # checking if expected number of fields are present and fields[9] miner has value
            if len(fields) != 19 or len(fields[9]) <= 0:
                return False
            
            # checking for required formats
            int(fields[12]) # size
            
            return True
        except:
            return False
        
    
    ########### STEP 1 ###########
    
    # Read and clean all blocks
    blocks_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv")
    blocks_lines_count = blocks_lines.count()
    print("ethereum_partC_log: blocks_lines_count", blocks_lines_count)
    # blocks_lines_count 7000002
    
    print("ethereum_partC_log: blocks_lines.take(2)", blocks_lines.take(2))
    # ['number 0, hash 1, parent_hash 2, nonce 3, sha3_uncles 4, logs_bloom 5, transactions_root 6, state_root 7, receipts_root 8, miner 9, difficulty 10, total_difficulty 11, size 12, extra_data 13, gas_limit 14, gas_used 15, timestamp 16, transaction_count 17, base_fee_per_gas 18', '400,<hash>,<parent_hash>,0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421,0xcefeb930c0faeb5477ab34ae5333f143b91c75a0b8f5973c813546e0112435a3,0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421,0x28921e4e2c9d84f4c0f0c0ceb991f45751a0fe93,20742135970,7556615279346,539,0x476574682f76312e302e302f6c696e75782f676f312e342e32,5000,0,1438271032,0,']
    
    # Cleaning for good block lines
    blocks_clean_lines = blocks_lines.filter(good_block)
    blocks_clean_lines_count = blocks_clean_lines.count()
    print("ethereum_partC_log: blocks_clean_lines_records", blocks_clean_lines_count)
    # blocks_clean_lines_records 7000001
    
    print("ethereum_partC_log: number of lines removed", blocks_lines_count - blocks_clean_lines_count)
    # number of lines removed 1
    
    ########### STEP 2 ###########
    
    # Map each miner with size of block mined
    map_miner_size = blocks_clean_lines.map(lambda l: (l.split(',')[9], int(l.split(',')[12])))
    print("ethereum_partC_log: map_miner_size.take(1)", map_miner_size.take(1))
    # [('0x28921e4e2c9d84f4c0f0c0ceb991f45751a0fe93', 539)]
    
    ########### STEP 3 ###########
    
    # Sum all sizes mined by each miner and ordering them by size in descending order
    total_miner_size = map_miner_size.reduceByKey(add).sortBy(lambda miner_size: miner_size[1], ascending=False)
    print("ethereum_partC_log: total_miner_size.take(1)", total_miner_size.take(1))
    # [('0xea674fdde714fd979de3edf0f56aa9716b898ec8', 17453393724)]
    
    ########### SAVING RESULTS ###########

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_PART_C/top10_miners.txt')
    my_result_object.put(Body=json.dumps(total_miner_size.take(10)))
    
    spark.stop()
