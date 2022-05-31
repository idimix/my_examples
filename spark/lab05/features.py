#!/usr/bin/python
# -*- coding: utf-8 -*-

##############################################################################
# create SparkSession
##############################################################################

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ivashnikov").getOrCreate()
sc = spark.sparkContext

spark.conf.set("spark.sql.session.timeZone", "GMT")

##############################################################################
# вспомогательные функции
##############################################################################

from pyspark.sql.functions import explode, when, col, expr, lower, concat, lit, create_map, dayofweek, hour, collect_list, udf
from pyspark.ml.feature import CountVectorizer
from pyspark.sql.types import IntegerType, ArrayType
import os
import re
import numpy as np



def main():
     
    # open and transform weblogs
    timestamp = expr("from_unixtime(visits.timestamp/1000)")

    hour_col = hour('timestamp').alias('hour')
    dayofweek_col = dayofweek('timestamp').alias('dayofweek')

    work_hours = when((9 <= col('hour')) & (col('hour') < 18), 1).otherwise(0).alias('web_fraction_work_hours')
    evening_hours = when((18 <= col('hour')) & (col('hour') < 24), 1).otherwise(0).alias('web_fraction_evening_hours')
    hours = [when(col('hour') == i, 1).otherwise(0).alias('web_hour_%s' % i) for i in range(24)]

    weekdays_dict = {1: 'sun', 2: 'mon', 3: 'tue', 4: 'wed', 5: 'thu', 6: 'fri', 7: 'sat'}
    days = [when(col('dayofweek') == k, 1).otherwise(0).alias('web_day_%s' % v) for k, v in weekdays_dict.items()]
    all_visits = lit(1).alias('all_visits')

    domain = expr("regexp_replace(parse_url(visits.url, 'HOST'), 'www.', '')").alias('domain')

    weblogs = spark \
        .read.format('json') \
        .load('/mf-labs/laba02/weblogs.json') \
        .withColumn('visits', explode('visits')) \
        .withColumn('timestamp', timestamp) \
        .select('*', hour_col, dayofweek_col) \
        .select('uid', domain, all_visits, work_hours, evening_hours, *days, *hours)


    # create data_features
    data_features = weblogs \
        .drop('domain') \
        .groupby('uid') \
        .sum()

    for column in data_features.columns:
        data_features = data_features.withColumnRenamed(column, re.sub('\(|\)|sum', '', column))

    data_features = data_features \
        .withColumn('web_fraction_work_hours', col('web_fraction_work_hours') / col('all_visits')) \
        .withColumn('web_fraction_evening_hours', col('web_fraction_evening_hours') / col('all_visits')) \
        .drop('all_visits')


    # create domain_features
    top1000 = weblogs \
        .where('domain != "null"') \
        .groupby('domain') \
        .count() \
        .sort(col('count').desc()) \
        .limit(1000) \
        .select('domain', lit(1).alias('index')) \
        .groupby('index') \
        .agg(collect_list(col('domain')).alias('domains'))

    cv = CountVectorizer(inputCol='domains', outputCol='domain_features').fit(top1000)
    index_sorted = np.argsort(cv.vocabulary)
    vector_udf = udf(lambda vector: list(map(int, np.array(vector.toArray())[index_sorted])), ArrayType(IntegerType()))

    domain_features = weblogs \
        .groupby('uid') \
        .agg(collect_list(col('domain')).alias('domains'))
    domain_features = cv \
        .transform(domain_features) \
        .withColumn('domain_features', vector_udf('domain_features')) \
        .drop('domains')


    # open users-items and join all data
    users_items = spark \
        .read.format('parquet') \
        .load('/user/dmitry.ivashnikov/users-items/20200429')

    all_features = users_items.join(domain_features, on='uid').join(data_features, on='uid')

    # save
    all_features \
        .write \
        .format("parquet") \
        .mode("overwrite") \
        .save('/user/dmitry.ivashnikov/features')
    
    
if __name__ == "__main__":
    main()