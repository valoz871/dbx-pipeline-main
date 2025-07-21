# Databricks notebook source
dbutils.widgets.text("base_path","s3://wr-data-sand/cross-dev/id5/","base path")
dbutils.widgets.text("ipCountryCode","IT","ip country code")
dbutils.widgets.text("max_dim_ip","12","max id per ip")

# COMMAND ----------

import pyspark.sql.functions as f
from datetime import date, timedelta,datetime
from pyspark.sql import Window, Row
from pyspark.sql.types import StringType, IntegerType, BooleanType, MapType, ArrayType
from functools import reduce
from pyspark.sql import DataFrame

base_path = dbutils.widgets.get("base_path")
ipCountryCode = dbutils.widgets.get("ipCountryCode")
max_dim_ip = dbutils.widgets.get("max_dim_ip_mobile")
date = dbutils.widgets.get("date")
daysago = int(dbutils.widgets.get("daysago"))

# COMMAND ----------

today = datetime.strptime(date,'%Y-%m-%d').date()
past_date = today - timedelta(days=daysago)

# COMMAND ----------

MobileExist = True
try:
  spark.read.format("parquet").option("basePath", base_path).load(base_path + "input/mobile/aggregated_data/ipCountryCode="+ipCountryCode)
except:
  MobileExist =False

# COMMAND ----------

if MobileExist:
  
  ips_for_ck = spark.read.format("parquet").option("basePath", base_path).load(base_path+\
                          "processing/cookie/flat_ip_withcount/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))\
                          .join(spark.read.format("parquet").option("basePath", base_path).load(base_path + \
                          "processing/mobile/flat_ip_withcount/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))\
                          .select("ip"),"ip")

  flat_ip = spark.read.format("parquet").option("basePath", base_path).load(base_path +\
                    "processing/mobile/flat_ip_withcount/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))\
                    .union(ips_for_ck)\
                    .groupBy("ip","ipCountryCode").agg(f.max("num_id2_per_day").alias("max_num_id2_per_day"))\
                    .where("max_num_id2_per_day <=" +max_dim_ip)\
                    .select("ip","ipCountryCode")\
                    .distinct()



# COMMAND ----------

if MobileExist:

  df_1 = spark.read.format("parquet").option("basePath", base_path).load( base_path + "input/mobile/aggregated_data")\
                    .where("date>='"+past_date.strftime('%Y-%m-%d')+"' and date<='"+today.strftime('%Y-%m-%d')+"'")\
                    .where("ipCountryCode='"+ipCountryCode+"'")\
                    .withColumn("day",f.dayofmonth(f.col('date')))\
                    .withColumn("week",f.weekofyear(f.col('date')))\
                    .groupBy("ip", "ifa","ipCountryCode","week").agg(f.sum("num_events_per_day").alias("num_events"))\
                    .join(flat_ip, ["ip","ipCountryCode"])\
                    .withColumn("flag_ck", f.lit(0))
  
  df_2 = spark.read.format("parquet").option("basePath", base_path).load( base_path + "input/cookie/aggregated_data/")\
                    .where("date>='"+past_date.strftime('%Y-%m-%d')+"' and date<='"+today.strftime('%Y-%m-%d')+"'")\
                    .where("ipCountryCode='"+ipCountryCode+"'")\
                    .withColumn("day",f.dayofmonth(f.col('date')))\
                    .withColumn("week",f.weekofyear(f.col('date')))\
                    .groupBy("ip", "id5UID","ipCountryCode","week").agg(f.sum("num_events_per_day").alias("num_events"))\
                    .join(flat_ip, ["ip","ipCountryCode"])\
                    .withColumn("flag_ck", f.lit(1))


  df = df_1.union(df_2).withColumnRenamed("ifa","id").withColumnRenamed("ifa","id").withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d')))

  itisNew=False
  try:
    df = spark.read.parquet(base_path + "processing/mobile/weekwindow/grouped_data_total/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
  except:
    itisNew=True
  if itisNew:
    df.write.partitionBy("ipCountryCode","proc_date").format("parquet").mode("append")\
              .option("replaceWhere", "ipCountryCode = '"+ipCountryCode+"'")\
              .save( base_path + "processing/mobile/weekwindow/grouped_data_total/")
   

# COMMAND ----------

if MobileExist:
  w = Window.partitionBy(f.col("id"))

  df = spark.read.format("parquet").option("basePath", base_path).load(base_path +\
             "processing/mobile/weekwindow/grouped_data_total/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))\
                    .where("ip is not null")\
                    .where("id is not null")\
                    .withColumn("num_ip", f.size(f.collect_set(f.col("ip")).over(w)))\
                    .withColumn("tot_events", f.sum(f.col("num_events")).over(w))

# COMMAND ----------

if MobileExist:
  ip_group = df.withColumn("info_array", f.array(df.id, df.num_ip, df.num_events, df.tot_events, df.flag_ck ))\
                .select("ip","info_array","ipCountryCode","week")\
                .groupBy("ip","ipCountryCode","week")\
                .agg(f.sort_array(f.collect_set(f.col("info_array"))).alias("dev4ip"), f.count(f.col("info_array")).alias("ndev")).where("ndev<51")


# COMMAND ----------

a_friend_test = ["012dbf761ef1755de2c79f13280c9992","IT", [["6535113545612047286",2, 2, 3, 0], \
                                                        ["6535113545612047286", 2, 2, 3, 1]],1]


print(a_friend_test)

# COMMAND ----------

import collections
import itertools
import math

def createConnections(a_friend):
    ip = a_friend[0]
    cookie_info_list = a_friend[2]
    country=a_friend[1]
    ndev = a_friend[3]
    all_connections = []
    
    for cookie_info_pair in itertools.combinations(cookie_info_list, 2):
        if int(cookie_info_pair[0][4]) * int(cookie_info_pair[1][4]) == 0:
            if cookie_info_pair[0][0] > cookie_info_pair[1][0]:
                a_tupla = tuple(itertools.chain.from_iterable((cookie_info_pair[1], cookie_info_pair[0])))
                all_connections.append((ip , 1./ndev,country) + a_tupla)
            else:
                a_tupla = tuple(itertools.chain.from_iterable(cookie_info_pair))
                all_connections.append((ip , 1./ndev,country) + a_tupla)
    return all_connections


createConnections(a_friend_test)

# COMMAND ----------

if MobileExist:
  friend_connections = ip_group.drop("week").rdd.flatMap(lambda rec: createConnections(rec)).toDF()\
                            .withColumnRenamed("_1", "ip_comune")\
                            .withColumnRenamed("_2", "conn_weight")\
                            .withColumnRenamed("_3", "ipCountryCode")\
                            .withColumnRenamed("_4", "dev1")\
                            .withColumnRenamed("_5", "num_ip_dev1")\
                            .withColumnRenamed("_6", "num_events_dev1")\
                            .withColumnRenamed("_7", "tot_events_dev1")\
                            .withColumnRenamed("_8", "flag_ck_dev1")\
                            .withColumnRenamed("_9", "dev2")\
                            .withColumnRenamed("_10", "num_ip_dev2")\
                            .withColumnRenamed("_11", "num_events_dev2")\
                            .withColumnRenamed("_12", "tot_events_dev2")\
                            .withColumnRenamed("_13", "flag_ck_dev2")\
                            .withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d')))

  itisNew=False
  try:
    df = spark.read.parquet(base_path + "processing/mobile/weekwindow/friend_connections/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
  except:
    itisNew=True
  if itisNew:
    friend_connections.write.partitionBy("ipCountryCode","proc_date").format("parquet").mode("append") \
    .option("replaceWhere", "ipCountryCode = '"+ipCountryCode+"'").save(base_path + "processing/mobile/weekwindow/friend_connections/")

# COMMAND ----------

if MobileExist:
  itisNew=False
  try:
    df = spark.read.parquet(base_path + "processing/mobile/weekwindow/df_pair/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
  except:
    itisNew=True
  if itisNew:
    df_pair = spark.read.format("parquet").option("basePath", base_path).load(base_path + \
                    "processing/mobile/weekwindow/friend_connections/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))\
      .groupBy("dev1","num_ip_dev1","tot_events_dev1","flag_ck_dev1","dev2","num_ip_dev2","tot_events_dev2","ipCountryCode","ip_comune","flag_ck_dev2")\
                    .agg(f.sum("conn_weight").alias("conn_weight"),\
                         f.sum("num_events_dev1").alias("num_events_dev1"),f.sum("num_events_dev2").alias("num_events_dev2"))\
                    .select( "ipCountryCode","ip_comune", "conn_weight", "dev1", "num_ip_dev1", "num_events_dev1", "tot_events_dev1","flag_ck_dev1",\
                            "dev2", "num_ip_dev2", "num_events_dev2", "tot_events_dev2","flag_ck_dev2")\
                    .groupBy("dev1", "dev2", "flag_ck_dev1","flag_ck_dev2","ipCountryCode","num_ip_dev1", "tot_events_dev1", "num_ip_dev2", "tot_events_dev2")\
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
 
# COMMAND ----------

if MobileExist:
  itisNew=False
  try:
    df = spark.read.parquet(base_path + "processing/mobile/weekwindow/df_pair/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
  except:
    itisNew=True
  if itisNew:
    df_pair.write.partitionBy("ipCountryCode","proc_date").format("parquet").mode("append") \
    .option("replaceWhere", "ipCountryCode = '"+ipCountryCode+"'").save(base_path + "processing/mobile/weekwindow/df_pair/")

