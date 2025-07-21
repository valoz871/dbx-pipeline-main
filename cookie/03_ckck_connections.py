# Databricks notebook source
base_path = dbutils.widgets.get("base_path")
ipCountryCode = dbutils.widgets.get("ipCountryCode")
max_dim_ip = dbutils.widgets.get("max_dim_ip")
daysago = int(dbutils.widgets.get("daysago"))


import pyspark.sql.functions as f
from datetime import date, timedelta,datetime
from pyspark.sql import Window, Row
from pyspark.sql.types import StringType, IntegerType, BooleanType, MapType, ArrayType
from functools import reduce
from pyspark.sql import DataFrame
import collections
import itertools
import math

date = dbutils.widgets.get("date")

today =  datetime.strptime(date,'%Y-%m-%d').date()

past_date = today - timedelta(days=daysago)

# COMMAND ----------


flat_ip = spark.read.format("parquet").option("basePath", base_path).load(base_path + \
                    "processing/cookie/flat_ip_withcount/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))\
                    .select("ip", "num_id2_per_day","ipCountryCode")\
                    .groupBy("ip","ipCountryCode").agg(f.max("num_id2_per_day").alias("max_num_id2_per_day"))\
                    .where("max_num_id2_per_day <= " +max_dim_ip)\
                    .select("ip","ipCountryCode")\
                    .distinct()

flat_ip.persist()
flat_ip.count()

# COMMAND ----------

w = Window.partitionBy(f.col("id5UID"),f.col("ipCountryCode"))



df = spark.read.format("parquet").option("basePath", base_path).load( base_path + "input/cookie/aggregated_data")\
                    .where("date>='"+past_date.strftime('%Y-%m-%d')+"' and date<='"+today.strftime('%Y-%m-%d')+"'")\
                    .where("ipCountryCode='"+ipCountryCode+"'")\
                    .withColumn("day",f.dayofmonth(f.col('date')))\
                    .groupBy("ip", "id5UID","ipCountryCode").agg(f.sum("num_events_per_day").alias("num_events"))\
                    .join(flat_ip, ["ip","ipCountryCode"])\
                    .where("ip is not null")\
                    .where("id5UID is not null")\
                    .withColumn("num_ip", f.size(f.collect_set(f.col("ip")).over(w)))\
                    .withColumn("tot_events", f.sum(f.col("num_events")).over(w))
              
flat_ip.unpersist()


# COMMAND ----------


ip_group = df.withColumn("info_array", f.array(df.id5UID, df.num_ip, df.num_events, df.tot_events ))\
                .select("ip","info_array","ipCountryCode")\
                .groupBy("ip","ipCountryCode")\
                .agg(f.sort_array(f.collect_set(f.col("info_array"))).alias("dev4ip"), f.count(f.col("info_array")).alias("ndev")).where("ndev<51")


# COMMAND ----------


def createConnections(a_friend):
    ip = a_friend[0]
    cookie_info_list = a_friend[2]
    country=a_friend[1]
    #ndev = len(cookie_info_list)
    ndev = a_friend[3]
    # create possible future connections
    all_connections = []
    
    for cookie_info_pair in itertools.combinations(cookie_info_list, 2):
        # creo le coppie ordinate
        if cookie_info_pair[0][0] > cookie_info_pair[1][0]:
            a_tupla = tuple(itertools.chain.from_iterable((cookie_info_pair[1], cookie_info_pair[0])))
            all_connections.append((ip , 1./ndev,country) + a_tupla)
        else:
            a_tupla = tuple(itertools.chain.from_iterable(cookie_info_pair))
            all_connections.append((ip , 1./ndev,country) + a_tupla)
    return all_connections


# COMMAND ----------


friend_connections = ip_group.rdd.flatMap(lambda rec: createConnections(rec)).toDF()\
                            .withColumnRenamed("_1", "ip_comune")\
                            .withColumnRenamed("_2", "conn_weight")\
                            .withColumnRenamed("_3", "ipCountryCode")\
                            .withColumnRenamed("_4", "dev1")\
                            .withColumnRenamed("_5", "num_ip_dev1")\
                            .withColumnRenamed("_6", "num_events_dev1")\
                            .withColumnRenamed("_7", "tot_events_dev1")\
                            .withColumnRenamed("_8", "dev2")\
                            .withColumnRenamed("_9", "num_ip_dev2")\
                            .withColumnRenamed("_10", "num_events_dev2")\
                            .withColumnRenamed("_11", "tot_events_dev2")\
                            .withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d')))

itisNew=False
try:
  df = spark.read.parquet(base_path + "processing/cookie/friend_connections/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
except:
  itisNew=True

if itisNew: 
  friend_connections.write.format("parquet").partitionBy("ipCountryCode","proc_date").mode("append") \
    .option("replaceWhere", "ipCountryCode = '"+ipCountryCode+"'").save(base_path + "processing/cookie/friend_connections/")

# COMMAND ----------

df_pair = spark.read.format("parquet").option("basePath", base_path).load(base_path +\
                    "processing/cookie/friend_connections/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))\
                    .groupBy("dev1","num_ip_dev1","tot_events_dev1","dev2","num_ip_dev2","tot_events_dev2","ipCountryCode","ip_comune")\
                    .agg(f.sum("conn_weight").alias("conn_weight"),\
                         f.sum("num_events_dev1").alias("num_events_dev1"),f.sum("num_events_dev2").alias("num_events_dev2"))\
                    .select( "ipCountryCode","ip_comune", "conn_weight", "dev1", "num_ip_dev1", "num_events_dev1", "tot_events_dev1",\
                            "dev2", "num_ip_dev2", "num_events_dev2", "tot_events_dev2")\
                    .groupBy("dev1", "dev2", "ipCountryCode","num_ip_dev1", "tot_events_dev1", "num_ip_dev2", "tot_events_dev2")\
                    .agg(\
                            f.countDistinct(f.col("ip_comune")).alias("common_ip"),\
                            f.sum(f.col("conn_weight").cast("double")).cast("string").alias("common_ip_weighted"),\
                            f.sum(f.col("num_events_dev1").cast("double")).cast("string").alias("num_events_dev1_on_intersection"),\
                            f.sum(f.col("num_events_dev2").cast("double")).cast("string").alias("num_events_dev2_on_intersection"))\
                    .withColumn("dice", 2*f.col("common_ip")/(f.col("num_ip_dev1") + f.col("num_ip_dev2")))\
                    .withColumn("jaccard", f.col("common_ip")/(f.col("num_ip_dev1") + f.col("num_ip_dev2") - f.col("common_ip")))\
                    .withColumn("overlap", f.col("common_ip")/(f.least(f.col("num_ip_dev1"), f.col("num_ip_dev2"))))\
                    .withColumn("common_ip_ratio1", f.col("common_ip")/f.col("num_ip_dev1"))\
                    .withColumn("common_ip_ratio2", f.col("common_ip")/f.col("num_ip_dev2"))\
                    .withColumn("common_ip_events1", (f.col("num_events_dev1_on_intersection") + f.col("num_events_dev2_on_intersection"))/(f.col("tot_events_dev1") + f.col("tot_events_dev2")))\
                    .withColumn("common_ip_events2", f.col("num_events_dev1_on_intersection") / (f.col("tot_events_dev1") + f.col("num_events_dev2_on_intersection") / f.col("tot_events_dev2")))\
                    .withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d')))
                    
try:
  df = spark.read.parquet(base_path + "processing/cookie/df_pair/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
except:
  itisNew=True

if itisNew: 
  df_pair.write.format("parquet").partitionBy("ipCountryCode","proc_date").mode("append") \
    .option("replaceWhere", "ipCountryCode = '"+ipCountryCode+"'").save(base_path + "processing/cookie/df_pair/")


