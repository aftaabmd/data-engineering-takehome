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


def generate_customer_stream(**kwargs):

    input_file_loc = kwargs['path_stream']
    topic = kwargs['Topic']

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'), linger_ms=10, api_version=(2,4,0))

    with open(os.getcwd() + input_file_loc,mode='r') as f:
        
        for line in f:
            
            line = line.strip()
            pipesplit = line.split('|')            
            json_comb = dumps(line,ensure_ascii=False) 
            print(json_comb)                                        # pick observation and encode to JSON
            producer.send(topic, value=json_comb)           # send encoded observation to Kafka topic
            sleep(1)
            

    producer.flush()
    producer.close()
    
    

