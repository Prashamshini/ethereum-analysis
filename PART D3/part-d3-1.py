"""Part D. Data exploration (40%)

The final part of the coursework requires you to explore the data and perform some analysis of your choosing. Below are some suggested ideas for analysis which could be undertaken, along with an expected grade for completing it to a good standard. You may attempt several of these tasks or undertake your own.

Popular Scams: Utilising the provided scam dataset, what is the most lucrative form of scam? How does this change throughout time, and does this correlate with certain known scams going offline/inactive? To obtain the marks for this catergory you should provide the id of the most lucrative scam and a graph showing how the ether received has changed over time for the dataset. (20%/40%)
"""
import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession, SQLContext
from datetime import datetime
from operator import add
from time import gmtime, strftime

if __name__ == "__main__":
    
    ########### SET UP JOB AND CLUSTER ACCESS ###########

    spark = SparkSession\
        .builder\
        .appName("Part-D-3")\
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
    
    def good_transaction(line):
        try:
            fields = line.split(',')
            # fields[6] to_address
            if len(fields) != 15 or len(fields[6]) <= 0:
                return False
            float(fields[7]) # value
            return True
        except:
            return False

    
    ########### STEP 1 ###########
    
    # Load 
    scams_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/scams.json").map(lambda x: json.loads(x))
    scams_lines_count = scams_lines.count()
    print("ethereum_partD_iii_log: scams_lines_count", scams_lines_count)
    # scams_lines_count 1
    
    # Separate each scam
    map_addresses = scams_lines.flatMap(lambda x: [(str(x['result'][addr]['id'])+"_"+x['result'][addr]['category'], x['result'][addr]['addresses']) for addr in x['result']])
    print("ethereum_partD_iii_log: map_addresses.take(2)", map_addresses.take(2))
    # [('130_Phishing', ['0x00e01a648ff41346cdeb873182383333d2184dd1']), ('1200_Phishing', ['0x858457daa7e087ad74cdeeceab8419079bc2ca03'])]
    
    # Map addresses and id
    map_address_id = map_addresses.flatMap(lambda x: [(i, x[0]) for i in x[1]]).map(lambda x: (x[0], x[1]))
    print("ethereum_partD_iii_log: map_address_id.take(2)", map_address_id.take(2))
    # [('0x00e01a648ff41346cdeb873182383333d2184dd1', '130_Phishing'), ('0x858457daa7e087ad74cdeeceab8419079bc2ca03', '1200_Phishing')]
    
    ########### STEP 2 ###########
    
    transactions_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    transactions_lines_count = transactions_lines.count()
    print("ethereum_partD_iii_log: transactions_lines_count", transactions_lines_count)
    # transactions_lines_count 369817359
    
    print("ethereum_partD_iii_log: transactions_lines.take(2)", transactions_lines.take(2))
    # ['hash 0, nonce 1, block_hash 2, block_number 3, transaction_index 4, from_address 5, to_address 6, value 7, gas 8, gas_price 9, input 10, block_timestamp 11, max_fee_per_gas 12, max_priority_fee_per_gas 13, transaction_type 14', '0x01f7583ce239ad19682ccbadb21d69a03cf7be333f4be9c02233874e3f79bf9d,0,0x9b430b5151969097fd7e5c29726a3afe680ab56f0f14a49a8f045b66392add15,46400,0,0xf55cdbdf978f539ea4eee0621f7f1a0f9f00f7d9,0x6baf48e1c966d16559ce2dddb616ffa72004851e,5000000000000000,21000,500000000000,0x,1438922527,,,0']
    
    transactions_clean_lines = transactions_lines.filter(good_transaction)
    transactions_clean_lines_count = transactions_clean_lines.count()
    print("ethereum_partD_iii_log: transactions_clean_lines_records", transactions_clean_lines_count)
    # transactions_clean_lines_records 367602949
    
    map_address_value = transactions_clean_lines.map(lambda l: (l.split(',')[6], int(l.split(',')[7])))
    print("ethereum_partD_iii_log: map_address_value.take(1)", map_address_value.take(1))
    # [('0x6baf48e1c966d16559ce2dddb616ffa72004851e', 5000000000000000)]
    
    total_address_value = map_address_value.reduceByKey(add) # .sortBy(lambda contract_value: contract_value[1], ascending=False)
    print("ethereum_partD_iii_log: total_address_value.take(1)", total_address_value.take(1))
    # [('0xb958258c9bda3564ecea82b2ef925e09719ad996', 20812399700000000000)]
    
    ########### STEP 3 ###########
    
    # Join scams and transactions
    map_address_value_id = total_address_value.join(map_address_id)
    print("ethereum_partD_iii_log: map_address_value_id.take(1)", map_address_value_id.take(1))
    # [('0x3f2cf669c657316985b01b1b797cb259c97d6551', (10806043065000000000, '3656_Scamming'))]
    
    ########### STEP 4 ###########
    
    # Remap in required format of id and value
    map_value_id = map_address_value_id.map(lambda l: (l[1][1], l[1][0]))
    print("ethereum_partD_iii_log: map_value_id.take(1)", map_value_id.take(1))
    # [('3656_Scamming', 10806043065000000000)]
    
    total_value_id = map_value_id.reduceByKey(add).sortBy(lambda id_value: id_value[1], ascending=False)
    print("ethereum_partD_iii_log: total_value_id.take(1)", total_value_id.take(1))
    # [('5622_Scamming', 16709083588073530571339)]
    
    ########### SAVING RESULTS ###########
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_PART_D3/top10_scam_ids.txt')
    my_result_object.put(Body=json.dumps(total_value_id.take(10)))
    
    spark.stop()
