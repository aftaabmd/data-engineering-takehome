from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import os
from time import sleep






#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0,' \
 #                                              'org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 pyspark-shell'
#

def consume_cus(input_df,checkpoint_path):
    
    query = json_df\
        .writeStream \
        .format("memory") \
        .queryName("CustomerTable") \
        .outputMode("append") \
        .option("checkpoint",checkpoint_path)\
        .start()

    query.awaitTermination(5)

    # Let it Fill up the table
    sleep(10)


def consume_cus_ext(input_df,checkpoint_path):
   
    # Stream the data, from a Kafka topic to a Spark in-memory table
    query = json_df\
        .writeStream \
        .format("memory") \
        .queryName("Customer_ExtendedTable") \
        .outputMode("append") \
        .option("checkpoint",checkpoint_path)\
        .start()

    query.awaitTermination(5)

    # Let it Fill up the table
    sleep(10)

    
def consume_prod(input_df,checkpoint_path):
    
    query = json_df\
        .writeStream \
        .format("memory") \
        .queryName("ProductTable") \
        .outputMode("append") \
        .option("checkpoint",checkpoint_path)\
        .start()

    query.awaitTermination(5)

    # Let it Fill up the table
    sleep(10)


def consume_refund(input_df,checkpoint_path):
   
    # Stream the data, from a Kafka topic to a Spark in-memory table
    query = json_df\
        .writeStream \
        .format("memory") \
        .queryName("RefundTable") \
        .outputMode("append") \
        .option("checkpoint",checkpoint_path)\
        .start()

    query.awaitTermination(5)

    # Let it Fill up the table
    sleep(10)

def consume_sales(input_df,checkpoint_path):
   
    # Stream the data, from a Kafka topic to a Spark in-memory table
    query = json_df\
        .writeStream \
        .format("memory") \
        .queryName("SalesTable") \
        .outputMode("append") \
        .option("checkpoint",checkpoint_path)\
        .start()

    query.awaitTermination(5)

    # Let it Fill up the table
    sleep(10)


    
def pyspark_consumer(spark,checkpoint_trans_path,checkpoint_loc_path):

    trans_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "Customer,Customer_Extended,Product, Sales, Refund") \
        .option("startingOffsets", "earliest") \
        .load()


def main():
    

    pyspark_consumer(spark,checkpoint_path,checkpoint_loc_path)


if __name__ == "__main__":
    main()
