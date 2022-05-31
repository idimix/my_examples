#!/usr/bin/python
# -*- coding: utf-8 -*-

##############################################################################
# create SparkSession
##############################################################################

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ivashnikov").getOrCreate()
sc = spark.sparkContext


##############################################################################
# импорт необходимых библиотек и создание вспомогательных функций
##############################################################################

from pyspark.sql.functions import explode, col, expr, lower, concat, lit, create_map, regexp_replace
from itertools import chain
from auth_postgresql import login, password


def open_elastic(table_name, option=("es.nodes", "10.0.0.5:9200")):
    df = spark.read.format("org.elasticsearch.spark.sql").option(*option).load(table_name)
    return df


def open_cassandra(table_opts, host='10.0.0.5', port='9042'):
    spark.conf.set("spark.cassandra.connection.host", host)
    spark.conf.set("spark.cassandra.connection.port", port)
    spark.conf.set("spark.cassandra.output.consistency.level", "ANY")
    spark.conf.set("spark.cassandra.input.consistency.level", "ONE")

    df = spark.read.format("org.apache.spark.sql.cassandra").options(**table_opts).load()
    return df


def open_json(path):
    df = spark.read.format('json').load(path)
    return df


def open_postgresql(table_name, jdbc_url):
    df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return df

def main():

    ##############################################################################
    # загрузка данных
    ##############################################################################

    # Информация о клиентах
    table_opts = {"table": "clients", "keyspace": "labdata"}
    clients = open_cassandra(table_opts)

    # Логи посещения интернет-магазина
    visits = open_elastic("visits")

    # Логи посещения веб-сайтов
    weblogs = open_json('/mf-labs/laba02/weblogs.json')

    # Информация о категориях веб-сайтов
    jdbc_url = 'jdbc:postgresql://10.0.0.5:5432/labdata?user={}&password={}'.format(login, password)
    domain_cats = open_postgresql('domain_cats', jdbc_url)


    ##############################################################################
    # обработка данных
    ##############################################################################

    # обработка данных о клиентах
    age_cat_dict = {i:
        '18-24' if 18 <= i < 25 else
        '25-34' if 25 <= i < 35 else 
        '35-44' if 35 <= i < 45 else 
        '45-54' if 45 <= i < 55 else 
        '>=55'
        for i in range(18, 100)
    }
    mapping_expr = create_map([lit(x) for x in chain(*age_cat_dict.items())])
    clients_processed = clients.select('uid', 'gender', mapping_expr[col('age')].alias('age_cat'))

    # обработка данных о посещениях интернет магазина
    cat = concat(lit('shop_'), lower(regexp_replace(col('category'), '\s|-', '_')))
    visits_count = visits \
        .na.drop(subset='uid') \
        .withColumn('cat', cat) \
        .groupby('uid') \
        .pivot('cat') \
        .count() \
        .na.fill(0)

    # обработка данных о посещениях веб-сайтов
    domain_cats_processed = domain_cats.withColumn('category', concat(lit('web_'), 'category'))
    domain = expr("regexp_replace(parse_url(visits.url, 'HOST'), '^www\.', '')").alias('domain')
    weblogs_count = weblogs \
        .withColumn('visits', explode('visits')) \
        .select('uid', domain) \
        .join(domain_cats_processed, on='domain', how='inner') \
        .groupby('uid') \
        .pivot('category') \
        .count() \
        .na.fill(0) \

    # создание финальной таблицы и запись в БД
    clients_stat = clients_processed \
        .join(visits_count, on='uid', how='left') \
        .join(weblogs_count, on='uid', how='left')

    clients_stat.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://10.0.0.5:5432/dmitry_ivashnikov") \
        .option("dbtable", "clients") \
        .option("user", login) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    
if __name__ == "__main__":
    main()
