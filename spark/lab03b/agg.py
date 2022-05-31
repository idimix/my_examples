#!/usr/bin/python
# -*- coding: utf-8 -*-

##############################################################################
# create SparkSession
##############################################################################

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ivashnikov").getOrCreate()
sc = spark.sparkContext


from pyspark.sql.functions import countDistinct, col, explode, from_json, to_json, lit, struct, when, from_unixtime, to_date, date_format, window, sum, mean, count, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# params
topic_in = 'dmitry_ivashnikov'
topic_out = 'dmitry_ivashnikov_lab03b_out'

kafka_params_in = {
    "kafka.bootstrap.servers": "spark-master-1:6667",
    "startingOffsets": "earliest",
    "subscribe": topic_in,
}
kafka_params_out = {
    "kafka.bootstrap.servers": "10.0.0.5:6667", 
    "checkpointLocation": "checkpoint",
}

# open_data
input_data = spark \
    .readStream \
    .format("kafka") \
    .options(**kafka_params_in) \
    .load()

# processing
clmns_in = [
    'category', 
    'event_type', 
    'item_id', 
    'item_price', 
    'timestamp', 
    'uid',
]
clmns_out = [
    'start_ts', 
    'end_ts', 
    'revenue', 
    'visitors', 
    'purchases', 
    'aov',
]

jsonSchema = StructType([StructField(clmn, StringType()) for clmn in clmns_in])
value_in = from_json(col('value').cast("string"), jsonSchema).alias('value')
clmns = list(map(lambda x: 'value.%s' % x, clmns_in))
timestamp = from_unixtime(col('timestamp') / 1000)

revenue = sum(when(col('event_type') == 'buy', col('item_price')).otherwise(0)).cast(LongType()).alias('revenue')
visitors = count(col('uid') != 'null').alias('visitors')
purchases = sum(when(col('event_type') == 'buy', 1).otherwise(0)).cast(LongType()).alias('purchases')
aov = (revenue / purchases).alias('aov')

start_ts = unix_timestamp(col('window.start'))
end_ts = unix_timestamp(col('window.end'))

value_out = to_json(struct(clmns_out)).alias('value')
topic = lit(topic_out).alias('topic')

agg_data = input_data \
    .select(value_in) \
    .select(*clmns) \
    .groupby(window(timestamp, '1 hour')) \
    .agg(revenue, visitors, purchases, aov) \
    .withColumn('start_ts', start_ts) \
    .withColumn('end_ts', end_ts) \
    .select(value_out, topic)
    
# write
sq = agg_data \
    .writeStream \
    .format("kafka") \
    .outputMode("Update") \
    .trigger(processingTime="5 seconds") \
    .options(**kafka_params_out).start()
sq.awaitTermination()
