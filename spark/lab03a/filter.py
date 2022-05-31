#!/usr/bin/python
# -*- coding: utf-8 -*-

##############################################################################
# create SparkSession
##############################################################################

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ivashnikov").getOrCreate()
sc = spark.sparkContext

spark.conf.set("spark.sql.session.timeZone", "GMT")


from pyspark.sql.functions import countDistinct, col, explode, from_json, from_unixtime, to_date, date_format
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
import os



def main(topic_name, offset, path):


    kafka_params = {
        "kafka.bootstrap.servers": "spark-master-1:6667",
        "startingOffsets": """ { "%s": { "0": %s } } """ % (topic_name, offset) if offset.isdigit() else offset,
        "endingOffsets": "latest",
        "subscribe": topic_name,
    }

    json_clmns = [
        'category', 
        'event_type', 
        'item_id', 
        'item_price', 
        'timestamp', 
        'uid',
    ]

    jsonSchema = StructType(
        [StructField(clmn, StringType()) for clmn in json_clmns]
    )

    value = from_json(col('value').cast("string"), jsonSchema)
    clmns = list(map(lambda x: 'value.%s' % x, json_clmns))
    date = date_format(
        to_date(from_unixtime(col('timestamp') / 1000)), 
        'yyyyMMdd'
    )

    # read
    input_data = spark \
        .read \
        .format("kafka") \
        .options(**kafka_params) \
        .load() \
        .select(value.alias('value')) \
        .select(*clmns) \
        .withColumn('date', date)

    #write
    for event_type in ['view', 'buy']:
        input_data \
            .filter('event_type == "%s"' % event_type) \
            .write \
            .format("json") \
            .partitionBy('date') \
            .mode("overwrite") \
            .save(os.path.join(path, event_type))

        
if __name__ == "__main__":

    topic_name = spark.conf.get("spark.filter.topic_name")
    offset = spark.conf.get("spark.filter.offset")
    path = spark.conf.get("spark.filter.output_dir_prefix")

    main(topic_name, offset, path)