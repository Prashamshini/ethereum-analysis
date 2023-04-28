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
            int(fields[11]) # block_timestamp
            return True
        except:
            return False
        
    def category_phishing(fields):
        try:
            if fields[0] == 'Phishing':
                return True
            else:
                return False
        except:
            return False
        
    def category_scamming(fields):
        try:
            if fields[0] == 'Scamming':
                return True
            else:
                return False
        except:
            return False
        
    def category_fakeico(fields):
        try:
            if fields[0] == 'Fake ICO':
                return True
            else:
                return False
        except:
            return False

    ########### STEP 1 ###########
    
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
    
    map_address_value_date = transactions_clean_lines.map(lambda l: (l.split(',')[6], (int(l.split(',')[7]), strftime("%Y/%m", gmtime(int(l.split(',')[11]))))))
    print("ethereum_partD_iii_log: map_address_value_date.take(1)", map_address_value_date.take(1))
    # [('0x6baf48e1c966d16559ce2dddb616ffa72004851e', (5000000000000000, '2015/08'))]
    
    ########### STEP 2 ###########
    
    scams_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/scams.json").map(lambda x: json.loads(x))
    
    map_addresses = scams_lines.flatMap(lambda x: [(x['result'][addr]['category'], x['result'][addr]['addresses']) for addr in x['result']])
    print("ethereum_partD_iii_log: map_addresses.take(2)", map_addresses.take(2))
    # [('Phishing', ['0x00e01a648ff41346cdeb873182383333d2184dd1']), ('Phishing', ['0x858457daa7e087ad74cdeeceab8419079bc2ca03'])]
    
    ########### STEP 3 ###########
    
    ######## SCAMMING ########
    scamming_addresses = map_addresses.filter(category_scamming)
    print("ethereum_partD_iii_log: scamming_addresses.take(2)", scamming_addresses.take(2))
    # [('Scamming', ['0xf0a924661b0263e5ce12756d07f45b8668c53380', '0x4423b4d5174dd6fca701c053bf48faeaaaee59f0']), ('Scamming', ['0xf0a924661b0263e5ce12756d07f45b8668c53380', '0x4423b4d5174dd6fca701c053bf48faeaaaee59f0'])]
    
    map_scamming_addresses = scamming_addresses.flatMap(lambda x: [(i, x[0]) for i in x[1]]).map(lambda x: (x[0], x[1]))
    print("ethereum_partD_iii_log: map_scamming_addresses.take(2)", map_scamming_addresses.take(2))
    # [('0xf0a924661b0263e5ce12756d07f45b8668c53380', 'Scamming'), ('0x4423b4d5174dd6fca701c053bf48faeaaaee59f0', 'Scamming')]
    print("ethereum_partD_iii_log: count of map_scamming_addresses", map_scamming_addresses.count())
    #  count of map_scamming_addresses 2733
    
    map_address_value_scamming = map_address_value_date.join(map_scamming_addresses)
    print("ethereum_partD_iii_log: map_address_value_scamming.take(1)", map_address_value_scamming.take(1))
    # [('0x3f2cf669c657316985b01b1b797cb259c97d6551', ((1427867800000000000, '2018/05'), 'Scamming'))]
    
    map_value_scamming = map_address_value_scamming.map(lambda l: ((l[1][0][1], l[1][1]), l[1][0][0]))
    print("ethereum_partD_iii_log: map_value_scamming.take(1)", map_value_scamming.take(1))
    # [(('2018/05', 'Scamming'), 990000000000000000)]
    
    total_value_scamming = map_value_scamming.reduceByKey(add).sortBy(lambda mY_val: mY_val[0][0], ascending=True)
    print("ethereum_partD_iii_log: total_value_scamming.take(1)", total_value_scamming.take(1))
    # [(('2017/06', 'Scamming'), 9878410120000000000)]
    
    ######## PHISHING ########
    phishing_addresses = map_addresses.filter(category_phishing)
    print("ethereum_partD_iii_log: phishing_addresses.take(2)", phishing_addresses.take(2))
    
    map_phishing_addresses = phishing_addresses.flatMap(lambda x: [(i, x[0]) for i in x[1]]).map(lambda x: (x[0], x[1]))
    print("ethereum_partD_iii_log: map_phishing_addresses.take(2)", map_phishing_addresses.take(2)) 
    print("ethereum_partD_iii_log: count of map_phishing_addresses", map_phishing_addresses.count())
    
    map_address_value_phishing = map_address_value_date.join(map_phishing_addresses)
    print("ethereum_partD_iii_log: map_address_value_phishing.take(1)", map_address_value_phishing.take(1))
    
    map_value_phishing = map_address_value_phishing.map(lambda l: ((l[1][0][1], l[1][1]), l[1][0][0]))
    print("ethereum_partD_iii_log: map_value_phishing.take(1)", map_value_phishing.take(1))
    
    total_value_phishing = map_value_phishing.reduceByKey(add).sortBy(lambda mY_val: mY_val[0][0], ascending=True)
    print("ethereum_partD_iii_log: total_value_phishing.take(1)", total_value_phishing.take(1))
    
    ######## FAKE ICO ########
    fakeico_addresses = map_addresses.filter(category_fakeico)
    print("ethereum_partD_iii_log: fakeico_addresses.take(2)", fakeico_addresses.take(2))
    
    map_fakeico_addresses = fakeico_addresses.flatMap(lambda x: [(i, x[0]) for i in x[1]]).map(lambda x: (x[0], x[1]))
    print("ethereum_partD_iii_log: map_fakeico_addresses.take(2)", map_fakeico_addresses.take(2))
    print("ethereum_partD_iii_log: count of map_fakeico_addresses", map_fakeico_addresses.count())
    
    map_address_value_fakeico = map_address_value_date.join(map_fakeico_addresses)
    print("ethereum_partD_iii_log: map_address_value_fakeico.take(1)", map_address_value_fakeico.take(1)) 
    
    map_value_fakeico = map_address_value_fakeico.map(lambda l: ((l[1][0][1], l[1][1]), l[1][0][0]))
    print("ethereum_partD_iii_log: map_value_fakeico.take(1)", map_value_fakeico.take(1))
    
    total_value_fakeico = map_value_fakeico.reduceByKey(add).sortBy(lambda mY_val: mY_val[0][0], ascending=True)
    print("ethereum_partD_iii_log: total_value_fakeico.take(1)", total_value_fakeico.take(1))
    
    ########### SAVING RESULTS ###########
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object1 = my_bucket_resource.Object(s3_bucket,'ethereum_PART_D3/scamming.txt')
    my_result_object1.put(Body=json.dumps(total_value_scamming.collect()))
    
    my_result_object2 = my_bucket_resource.Object(s3_bucket,'ethereum_PART_D3/phishing.txt')
    my_result_object2.put(Body=json.dumps(total_value_phishing.collect()))
    
    my_result_object3 = my_bucket_resource.Object(s3_bucket,'ethereum_PART_D3/fakeico.txt')
    my_result_object3.put(Body=json.dumps(total_value_fakeico.collect()))
    
    spark.stop()
