# Databricks notebook source
base_path = dbutils.widgets.get("base_path")
ipCountryCode = dbutils.widgets.get("ipCountryCode")
noiseLevel = dbutils.widgets.get("noiseLevel") 

import pyspark.sql.functions as f
from datetime import date, timedelta, datetime
from pyspark.sql import Window, Row
from pyspark.sql.types import StringType, IntegerType, BooleanType, MapType, ArrayType
from functools import reduce

from pyspark.sql import DataFrame

date = dbutils.widgets.get("date")
today = datetime.strptime(date,'%Y-%m-%d').date()#date.today()

# COMMAND ----------

df = spark.read.format("parquet").option("basePath", base_path).load( base_path + \
                 "processing/cookie/df_pair/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))\
                .where("common_ip_weighted >= " +noiseLevel)\
                .withColumnRenamed("mf_os", "os")\
                .withColumnRenamed("mf_browser", "browser")\
                .withColumnRenamed("mf_device_type", "device_type")

lookup = spark.read.format("parquet").option("basePath", base_path).load( base_path + \
         "processing/cookie/id2_ua_info_clean/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))

# COMMAND ----------

df1 = df.join(lookup.select([f.col(c).alias(c + '_dev1') for c in lookup.columns if c != 'id5UID' or c != 'ipCountryCode'] + [f.col("id5UID"),f.col("ipCountryCode")])\
                    .withColumnRenamed("id5UID", "dev1"),\
              ["dev1","ipCountryCode"], how="left")
                    
df2 = df1.join(lookup.select([f.col(c).alias(c + '_dev2') for c in lookup.columns if c != 'id5UID' or c != 'ipCountryCode'] + [f.col("id5UID"),f.col("ipCountryCode")] )\
                    .withColumnRenamed("id5UID", "dev2"),\
               ["dev2","ipCountryCode"], how="left")
                    
df_final = df2.withColumn("sameDeviceType", f.when(f.col("mf_device_type_dev1") == f.col("mf_device_type_dev2"), f.lit(1)).otherwise(f.lit(0)))\
                    .drop("mf_device_type_dev1", "mf_device_type_dev2")\
                    .withColumn("sameBrowser", f.when(f.col("mf_browser_dev1") == f.col("mf_browser_dev2"), f.lit(1)).otherwise(f.lit(0)))\
                    .drop("browser_dev1", "browser_dev2")\
                    .withColumn("sameOS", f.when(f.col("mf_os_dev1") == f.col("mf_os_dev2"), f.lit(1)).otherwise(f.lit(0)))\
                    .drop("os_dev1", "os_dev2")



# COMMAND ----------

itisNew=False
try:
  df = spark.read.parquet(base_path + "processing/cookie/df_final/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
except:
  itisNew=True

if itisNew: 
  df_final.withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d'))).write.format("parquet")\
.partitionBy("ipCountryCode","proc_date").mode("append").save(base_path + "processing/cookie/df_final/")

# COMMAND ----------

itisNew

# COMMAND ----------


