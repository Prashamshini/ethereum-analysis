"""Part D. Data exploration (40%)

The final part of the coursework requires you to explore the data and perform some analysis of your choosing. Below are some suggested ideas for analysis which could be undertaken, along with an expected grade for completing it to a good standard. You may attempt several of these tasks or undertake your own.

Gas Guzzlers: For any transaction on Ethereum a user must supply gas. How has gas price changed over time? Have contracts become more complicated, requiring more gas, or less so? How does this correlate with your results seen within Part B. To obtain these marks you should provide a graph showing how gas price has changed over time, a graph showing how gas used for contract transactions has changed over time and identify if the most popular contracts use more or less than the average gas used. (20%/40%)
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
        .appName("Part-D-2")\
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
    
    def good_transaction_top10(line):
        try:
            fields = line.split(',')
            
            # checking if expected number of fields are present and the address is in top 10 contract adresses
            if len(fields) != 15 or fields[6] not in top10_addresses:
                return False
            
            # checking for required formats
            float(fields[8]) # gas
            
            return True
        except:
            return False
        
    def good_transaction(line):
        try:
            fields = line.split(',')
            
            # checking if expected number of fields are present
            if len(fields) != 15 and len(fields[6]) <= 0:
                return False
            
            # checking for required formats
            float(fields[8]) # gas
            
            return True
        except:
            return False
        
    
    ########### STEP 1 ###########
    
    # Read all transactions
    lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    lines_count = lines.count()
    print("ethereum_partD_ii_log: lines_count", lines_count)
    # lines_count 369817359
    
    print("ethereum_partD_ii_log: lines.take(2)", lines.take(2))
    # ['hash 0, nonce 1, block_hash 2, block_number 3, transaction_index 4, from_address 5, to_address 6, value 7, gas 8, gas_price 9, input 10, block_timestamp 11, max_fee_per_gas 12, max_priority_fee_per_gas 13, transaction_type 14', '0x01f7583ce239ad19682ccbadb21d69a03cf7be333f4be9c02233874e3f79bf9d,0,0x9b430b5151969097fd7e5c29726a3afe680ab56f0f14a49a8f045b66392add15,46400,0,0xf55cdbdf978f539ea4eee0621f7f1a0f9f00f7d9,0x6baf48e1c966d16559ce2dddb616ffa72004851e,5000000000000000,21000,500000000000,0x,1438922527,,,0']
    
    ########### STEP 2 ###########
    
    # Filter good contract transactions
    trans_lines = lines.filter(good_transaction)
    trans_lines_count = trans_lines.count()
    print("ethereum_partD_ii_log: trans_lines_count", trans_lines_count)
    #  trans_lines_count 369817358
        
    # Extracting gas used (index: 8) for each transaction
    map_gas_used = trans_lines.map(lambda l: (1, (float(l.split(',')[8]), 1)))
    print("ethereum_partD_ii_log: map_gas_used.take(1)", map_gas_used.take(1))
    # [(1, (21000.0, 1))]
    
    ########### STEP 3 ###########
    
    # Summing gas used and number of transactions
    total_gas_used = map_gas_used.reduceByKey(lambda x1, x2: (x1[0]+x2[0], x1[1]+x2[1]))
    print("ethereum_partD_ii_log: total_gas_used.take(1)", total_gas_used.take(1))
    # [(1, (59808970955531.0, 369817358))]
        
    # Averaging the the gas used
    avg_gas_used = total_gas_used.map(lambda l: (l[0], l[1][0]/l[1][1]))
    print("ethereum_partD_ii_log: avg_gas_used.take(1)", avg_gas_used.take(1))
    # [(1, 161725.69962368018)]
    
    ########### STEP 4 ###########
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    f = my_bucket_resource.Object(s3_bucket, "ethereum_PART_B/top10_contracts.txt")
    f_output = f.get()['Body'].read().decode('utf-8').split(",")
    top10_addresses = []
    for i in range(0, len(f_output), 2):
        top10_addresses.append(f_output[i].strip(" []\""))    
    print("ethereum_partD_ii_log: top10_addresses", top10_addresses)
    
    ########### STEP 5 ###########
    
    clean_lines = lines.filter(good_transaction_top10)
    clean_lines_count = clean_lines.count()
    print("ethereum_partD_ii_log: clean_lines_records", clean_lines_count)
    # clean_lines_records 5209803
        
    # Mapping to_address (index: 6) with value (index: 7)
    map_contracts_value = clean_lines.map(lambda l: (l.split(',')[6], (float(l.split(',')[8]), 1)))
    print("ethereum_partD_ii_log: map_contracts_value.take(1)", map_contracts_value.take(1))
    # [('0xe28e72fcf78647adce1f1252f240bbfaebd63bcc', (150000.0, 1))]
    
    # Summing value for each address 
    total_contracts_value = map_contracts_value.reduceByKey(lambda x1, x2: (x1[0]+x2[0], x1[1]+x2[1]))
    print("ethereum_partD_ii_log: total_contracts_value.take(1)", total_contracts_value.take(1))
    # [('0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444', (26790518214.0, 322183))]
        
    # Rearranging the map such that it is (address, value) and ordering by value in descending order
    avg_address_values = total_contracts_value.map(lambda l: (l[0], l[1][0]/l[1][1])).sortBy(lambda contract_value: contract_value[1], ascending=False)
    print("ethereum_partD_ii_log: avg_address_values.take(1)", avg_address_values.take(1))
    # [('0x341e790174e3a4d35b65fdc067b6b5634a61caea', 198832.66666666666)]
        
    ########### SAVING RESULTS ###########

    my_result_object1 = my_bucket_resource.Object(s3_bucket,'ethereum_PART_D2/top10_avg_used.txt')
    my_result_object1.put(Body=json.dumps(avg_address_values.collect()))
    
    my_result_object2 = my_bucket_resource.Object(s3_bucket,'ethereum_PART_D2/avg_gas_used.txt')
    my_result_object2.put(Body=json.dumps(avg_gas_used.collect()))
    
    spark.stop()
