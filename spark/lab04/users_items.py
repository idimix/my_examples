#!/usr/bin/python
# -*- coding: utf-8 -*-

##############################################################################
# create SparkSession
##############################################################################

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ivashnikov").getOrCreate()
sc = spark.sparkContext



##############################################################################
# вспомогательные функции
##############################################################################


from pyspark.sql.functions import col, lower, concat, lit, max as spark_max, regexp_replace
import re
import os

def read_df(action_type, input_dir):
    """чтение датафрейма с заданным типом"""

    item_id = concat(
        lit(action_type), 
        lit('_'), 
        lower(regexp_replace(col('item_id'), '\s|-', '_'))
    ).alias('item_id')
    
    return spark \
        .read \
        .format('json') \
        .load(os.path.join(input_dir, action_type)) \
        .select('date', 'uid', item_id)


def union_df(df1, df2):
    """объединение двух датафреймов с разным набором колонок"""

    columns1 = set(df1.columns)
    columns2 = set(df2.columns)

    columns1_new = columns2 - columns1
    columns2_new = columns1 - columns2

    expr1 = list(columns1) + list(map(lambda x: '0 as %s' % x, columns1_new))
    expr2 = list(columns2) + list(map(lambda x: '0 as %s' % x, columns2_new))

    df_union = df1.selectExpr(*expr1).unionByName(df2.selectExpr(*expr2))
    
    return df_union.select(sorted(df_union.columns))


def group_sum_df(df, key):
    """группировка по ключу и сумма остальных колонок с последующим переименованием"""

    df_new = df \
        .groupby(key) \
        .sum()

    for col in df_new.columns:
        df_new = df_new.withColumnRenamed(col, re.sub('\(|\)|sum', '', col))

    return df_new


def main(update, input_dir, output_dir):
    # read data
    visits = read_df('buy', input_dir).union(read_df('view', input_dir))
    visits.cache()
    
    # get max date
    max_date_str = str(visits.agg(spark_max('date')).collect()[0][0])
    
    # creat users_items
    users_items_new = visits \
        .groupby('uid') \
        .pivot('item_id') \
        .count() \
        .na.fill(0)

    users_items_final = users_items_new

    if update:
        cmd = 'hdfs dfs -ls %s' % os.path.join(output_dir)
        dir_list = os.popen(cmd).read().strip().split('\n')
        pattern = '(/.+)'
        dir_list = list(filter(lambda x: re.search(pattern, x), dir_list))
        dir_list = list(map(lambda x: re.search(pattern, x).group(1).rsplit('/', 1)[1], dir_list))
        if len(dir_list):

            last_dir = max(dir_list)
            print('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
            print(last_dir)
            print('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
            # read old data
            users_items_old = spark \
                .read \
                .format("parquet") \
                .load(os.path.join(output_dir, last_dir))

            users_items_old.cache()
            users_items_old.count()

            # union old and new data
            users_items_final = group_sum_df(
                union_df(users_items_new, users_items_old), 'uid'
            )

    # save
    users_items_final \
        .write \
        .format("parquet") \
        .mode("overwrite") \
        .save(os.path.join(output_dir, max_date_str))
    
    
if __name__ == "__main__":
    
    # get params from conf    
    update = spark.conf.get("spark.users_items.update") == "1"
    input_dir = spark.conf.get("spark.users_items.input_dir")
    output_dir = spark.conf.get("spark.users_items.output_dir")
    
    main(update, input_dir, output_dir)
