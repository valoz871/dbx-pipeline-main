# Databricks notebook source
base_path = dbutils.widgets.get("base_path")
ipCountryCode = dbutils.widgets.get("ipCountryCode")
daysago = int(dbutils.widgets.get("daysago"))

from datetime import date, timedelta,datetime
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType,StringType,DoubleType,IntegerType
date = dbutils.widgets.get("date")

# COMMAND ----------

MobileExist = True
try:
  spark.read.format("parquet").option("basePath", base_path).load(base_path + "input/mobile/aggregated_data/ipCountryCode="+ipCountryCode)
except:
  MobileExist =False

# COMMAND ----------

if MobileExist:
  today = datetime.strptime(date,'%Y-%m-%d').date()#date.today()
  past_date = today - timedelta(days=daysago)

# leggere id_2 e userAgent 
  temp = spark.read.format("parquet").option("basePath", base_path).load( base_path + "input/mobile/aggregated_data")\
                .where("date>='"+past_date.strftime('%Y-%m-%d')+"' and date<='"+today.strftime('%Y-%m-%d')+"'")\
                .where("ipCountryCode='"+ipCountryCode+"'")\
                .groupBy("ifa","platform","ipCountryCode")\
                .agg(f.sum("num_events_per_day").alias("count"))

#temp.persist()
#temp.count()

# COMMAND ----------

if MobileExist:
  w = Window.partitionBy(f.col("ifa"),f.col("ipCountryCode")).orderBy(f.desc("count"))

  df_os = temp.where("platform is not null")\
              .withColumn("rn", f.row_number().over(w)).where("rn == 1")\
              .drop("rn", "count")\
              .withColumnRenamed("platform", "mf_os")\
              .distinct()\
              .withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d')))

# COMMAND ----------

if MobileExist:
  itisNew=False
  try:
    df = spark.read.parquet(base_path + "processing/mobile/id2_ua_info_clean/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
  except:
    itisNew=True
  if itisNew:
    df_os.write.format("parquet").mode("append").partitionBy("ipCountryCode","proc_date")\
            .option("replaceWhere", "ipCountryCode = '"+ipCountryCode+"'")\
            .save(base_path + "processing/mobile/id2_ua_info_clean/")

# COMMAND ----------


