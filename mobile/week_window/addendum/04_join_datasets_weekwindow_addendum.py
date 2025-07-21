# Databricks notebook source
def str2bool(v):
  return str(v).lower() in ("yes", "true", "t", "1")

# COMMAND ----------

base_path = dbutils.widgets.get("base_path")
ipCountryCode = dbutils.widgets.get("ipCountryCode")
noiseLevel = dbutils.widgets.get("noiseLevel_mobile") # 0.01


import pyspark.sql.functions as f
from datetime import date, timedelta,datetime
from pyspark.sql import Window, Row
from pyspark.sql.types import StringType, IntegerType, BooleanType, MapType, ArrayType
from functools import reduce

from pyspark.sql import DataFrame
date = dbutils.widgets.get("date")
daysago = int(dbutils.widgets.get("daysago"))
addendum = str2bool(dbutils.widgets.get("add_previous_month_maid"))

# COMMAND ----------

if addendum:
  MobileExist = True
  try:
    spark.read.format("parquet").option("basePath", base_path).load(base_path + "input/mobile/aggregated_data/ipCountryCode="+ipCountryCode)
  except:
    MobileExist =False
  if not MobileExist:
    addendum=False

# COMMAND ----------

if addendum:
  today = datetime.strptime(date,'%Y-%m-%d').date()#date.today()
  past_date = today - timedelta(days=daysago)

# COMMAND ----------

if addendum:
  if MobileExist:
    ls = dbutils.fs.ls(base_path + \
                          "processing/mobile/flat_ip_withcount/ipCountryCode="+ipCountryCode+"/")
    past_date_list = []
    diff = []
    for out in ls:
      past_date_list.append(datetime.strptime(ls[0].name.split("=")[1].split("/")[0],'%Y-%m-%d').date())
      diff.append(abs((past_date -datetime.strptime(ls[0].name.split("=")[1].split("/")[0],'%Y-%m-%d').date()).days))

    past_date_good = past_date_list[diff.index(min(diff))]
    past_date_lookback = past_date_good - timedelta(days=daysago)
    addendumdiff = abs(today-past_date_good)
    if addendumdiff.days<(daysago-7):
      addendum = False

# COMMAND ----------

if addendum:
  if MobileExist:
    df = spark.read.format("parquet").option("basePath", base_path).load( base_path +\
             "processing/mobile/weekwindow/df_pair_addendum/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))\
                .where("common_ip_weighted >=" +noiseLevel)\
                .withColumnRenamed("mf_os", "os")\
                .withColumnRenamed("mf_browser", "browser")\
                .withColumnRenamed("mf_device_type", "device_type")

    lookup_mobile = spark.read.format("parquet").option("basePath", base_path)\
                .load( base_path + "processing/mobile/id2_ua_info_clean/ipCountryCode="+ipCountryCode+"/proc_date="+past_date_good.strftime('%Y-%m-%d'))\
                .where("ipCountryCode='"+ipCountryCode+"'")\
                .withColumn("mf_os",f.lower(f.col("mf_os")))


    lookup_ck = spark.read.format("parquet").option("basePath", base_path).load( base_path + \
               "processing/cookie/id2_ua_info_clean/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))\
                    .select("id5UID","mf_os","ipCountryCode","proc_date")\
                    .where("ipCountryCode='"+ipCountryCode+"'").withColumn("mf_os",f.lower(f.col("mf_os")))

    lookup = lookup_mobile.union(lookup_ck)

# COMMAND ----------

if addendum:
  if MobileExist:
    df1 = df.join(lookup.select([f.col(c).alias(c + '_dev1') for c in lookup.columns if c != 'ifa' or c != 'ipCountryCode'] + [f.col("ifa"),f.col("ipCountryCode")])\
                    .withColumnRenamed("ifa", "dev1"),\
                    ["dev1","ipCountryCode"], how="left")
                    
    df2 = df1.join(lookup.select([f.col(c).alias(c + '_dev2') for c in lookup.columns if c != 'ifa' or c != 'ipCountryCode'] + [f.col("ifa"),f.col("ipCountryCode")])\
                    .withColumnRenamed("ifa", "dev2"),\
                    ["dev2","ipCountryCode"], how="left")


    df_final = df2.withColumn("sameOS", f.when(f.col("mf_os_dev1") == f.col("mf_os_dev2"), f.lit(1)).otherwise(f.lit(0)))\
                    .drop("os_dev1", "os_dev2")\
                    .withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d')))

    df_final.printSchema()

# COMMAND ----------

if addendum:
  if MobileExist:
    itisNew=False
    try:
      df = spark.read.parquet(base_path + "processing/mobile/weekwindow/df_final_addendum/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
    except:
      itisNew=True
    if itisNew:
      df_final.write.format("parquet").partitionBy("ipCountryCode","proc_date") \
      .mode("append") \
      .option("replaceWhere", "ipCountryCode = '"+ipCountryCode+"'") \
      .save(base_path + "processing/mobile/weekwindow/df_final_addendum")

# COMMAND ----------


