"""Part B. Top Ten Most Popular Services (25%)

Evaluate the top 10 smart contracts by total Ether received. You will need to join address field in the contracts dataset to the to_address in the transactions dataset to determine how much ether a contract has received.
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
        .appName("Part-B")\
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
            
            # checking if expected number of fields are present and fields[6] to_address has value
            if len(fields) != 15 or len(fields[6]) <= 0:
                return False
            
            # checking for required formats
            float(fields[7]) # value
            
            return True
        except:
            return False

    def good_contract(line):
        try:
            fields = line.split(',')
            # # checking if expected number of fields are present and fields[0] address has value and isn't the header
            if len(fields) != 6 or len(fields[0]) <= 0 or fields[0] == 'address':
                return False
            
            return True
        except:
            return False
    
    ########### STEP 1 ###########
    
    # Read and clean all transactions
    transactions_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    transactions_lines_count = transactions_lines.count()
    print("ethereum_partB_log: transactions_lines_count", transactions_lines_count)
    # transactions_lines_count 369817359
    
    print("ethereum_partB_log: transactions_lines.take(2)", transactions_lines.take(2))
    # ['hash 0, nonce 1, block_hash 2, block_number 3, transaction_index 4, from_address 5, to_address 6, value 7, gas 8, gas_price 9, input 10, block_timestamp 11, max_fee_per_gas 12, max_priority_fee_per_gas 13, transaction_type 14', '0x01f7583ce239ad19682ccbadb21d69a03cf7be333f4be9c02233874e3f79bf9d,0,0x9b430b5151969097fd7e5c29726a3afe680ab56f0f14a49a8f045b66392add15,46400,0,0xf55cdbdf978f539ea4eee0621f7f1a0f9f00f7d9,0x6baf48e1c966d16559ce2dddb616ffa72004851e,5000000000000000,21000,500000000000,0x,1438922527,,,0']
    
    # Cleaning for good transaction lines
    transactions_clean_lines = transactions_lines.filter(good_transaction)
    transactions_clean_lines_count = transactions_clean_lines.count()
    print("ethereum_partB_log: transactions_clean_lines_records", transactions_clean_lines_count)
    # transactions_clean_lines_records 367602949
    
    print("ethereum_partB_log: number of transactions removed", transactions_lines_count - transactions_clean_lines_count)
    # number of transactions removed 2214410
    
    ########### STEP 2 ###########
    
    # Mapping to_address (index: 6) with value (index: 7)
    map_contracts_value = transactions_clean_lines.map(lambda l: (l.split(',')[6], float(l.split(',')[7])))
    print("ethereum_partB_log: map_contracts_value.take(1)", map_contracts_value.take(1))
    # [('0x6baf48e1c966d16559ce2dddb616ffa72004851e', 5000000000000000.0)]
    
    # Summing value for each address 
    total_contracts_value = map_contracts_value.reduceByKey(add)
    print("ethereum_partB_log: total_contracts_value.take(1)", total_contracts_value.take(1))
    # [('0x3c085b91f6811f333f45a74424a9054dc3d0981e', 3.638589925256387e+20)]
    
    ########### STEP 3 ###########
    
    # Read and clean all contracts
    contracts_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")
    contracts_lines_count = contracts_lines.count()
    print("ethereum_partB_log: contracts_lines_count", contracts_lines_count)
    # contracts_lines_count 2214410
    
    print("ethereum_partB_log: contracts_lines.take(2)", contracts_lines.take(2))
    # ['address [0], bytecode [1], function_sighashes [2], is_erc20 [3], is_erc721 [4], block_number [5]', '0x94c81a1dbc5c41a5e9962a2d6da5aa5ff684259f,<bytecode as hex>,"0x2ef9db13,0xe3767876,0xee97f7f3",False,False,']
    
    # Cleaning for good contract lines
    contracts_clean_lines = contracts_lines.filter(good_contract)
    contracts_clean_lines_count = contracts_clean_lines.count()
    print("ethereum_partB_log: contracts_clean_lines_records", contracts_clean_lines_count)
    # contracts_clean_lines_records 382049
    
    print("ethereum_partB_log: number of contracts removed", contracts_lines_count - contracts_clean_lines_count)
    # number of contracts removed 1832361
    
    ########### STEP 4 ###########
    
    # Extracting the addresses of smart contracts
    map_contracts = contracts_clean_lines.map(lambda l: (l.split(',')[0], 1))
    print("ethereum_partB_log: map_contracts.take(1)", map_contracts.take(1))
    # [('0xe28e72fcf78647adce1f1252f240bbfaebd63bcc', 1)]
    
    ########### STEP 5 ###########
    
    # Joining the smart contracts with the value
    combined_contracts_value = map_contracts.join(total_contracts_value) # Joining the dataset
    print("ethereum_partB_log: combined_contracts_value.take(1)", combined_contracts_value.take(1))
    # [('0x097ea01d2303b3512e902caaf365a83607fdf0ef', (1, 1.197495e+16))]
    
    ########### STEP 6 ###########
    
    # Rearranging the map such that it is (address, value) and ordering by value in descending order
    map_address_values = combined_contracts_value.map(lambda l: (l[0], l[1][1])).sortBy(lambda contract_value: contract_value[1], ascending=False)
    print("ethereum_partB_log: map_address_values.take(1)", map_address_values.take(1))
    # [('0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444', 8.415536369994145e+25)]
        
    ########### SAVING RESULTS ###########

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_PART_B/top10_contracts.txt')
    my_result_object.put(Body=json.dumps(map_address_values.take(10)))
    
    spark.stop()
