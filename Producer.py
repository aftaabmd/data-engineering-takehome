#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan  7 23:58:15 2023

@author: aftaabmohammed
"""

# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""



from kafka  import KafkaProducer
from time import sleep
from json import dumps


def generate_customer_stream(input_customer_transaction):

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'), linger_ms=10, api_version=(2,4,0))

    with open(input_customer_transaction,mode='r') as f:
        
        for line in f:
            
            line = line.strip()
            pipesplit = line.split('|')            
            json_comb = dumps(line,ensure_ascii=False) 
            print(json_comb)                                        # pick observation and encode to JSON
            producer.send(input_customer_transaction.split("/")[-1][0:-4], value=json_comb)           # send encoded observation to Kafka topic
            sleep(1)
            

    producer.flush()
    producer.close()
    
    


def main():
    
    for i in ["sample_data/Customer.txt","sample_data/Customer_Extended.txt","sample_data/Product.txt","sample_data/Refund.txt","sample_data/Sales.txt"]:
    
        generate_customer_stream(i)


main()