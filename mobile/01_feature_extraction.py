# Databricks notebook source
import pyspark.sql.functions as f
from datetime import date, timedelta,datetime
from pyspark.sql import Window, Row
from pyspark.sql.types import StringType, IntegerType, BooleanType, MapType, ArrayType
from functools import reduce

from pyspark.sql import DataFrame

# COMMAND ----------

id5Bucket = dbutils.widgets.get("id5Bucket_mobile")
base_path = dbutils.widgets.get("base_path")
ipCountryCode = dbutils.widgets.get("ipCountryCode")
date = dbutils.widgets.get("date")
daysago = int(dbutils.widgets.get("daysago"))

# COMMAND ----------

newCountry = False 
try:
  dbutils.fs.ls(base_path+"input/mobile/aggregated_data/ipCountryCode="+ipCountryCode)
except:
  newCountry = True

# COMMAND ----------

if newCountry:
  today = datetime.strptime(date,'%Y-%m-%d').date()
  datetime_object_2 = today 
  past_date = today - timedelta(days=daysago)
  datetime_object = past_date 
else:
  readLastDate = dbutils.fs.ls(base_path+"input/mobile/aggregated_data/ipCountryCode="+ipCountryCode)[-2][1].split("=")[1].replace("/", "")
  today = datetime.strptime(date,'%Y-%m-%d').date()
  datetime_object_2 = today 
  past_date = datetime.strptime(readLastDate, "%Y-%m-%d").date() + timedelta(days=1)#today - timedelta(days=16)
  datetime_object = past_date
  

# COMMAND ----------



d1 = datetime_object  # start date
d2 = datetime_object_2  # end date

delta = d2 - d1         # timedelta
date_range = []

for i in range(delta.days + 1):
    a = (d1 + timedelta(days=i))
    a = a.strftime('%Y/%m/%d')
    date_range.append(a)

d = str(date_range).replace('[','{').replace(']','}').replace('\'',"").replace(' ',"")

# COMMAND ----------

NewData = True
try:
  df = spark.read.format("csv").load(id5Bucket+d+"/country="+ipCountryCode)\
 .select(f.col("_c0").alias("timestamp"),f.col("_c1").alias("ifa"),f.col("_c2").alias("platform"),f.col("_c3").alias("ip"),f.col("_c4").alias("user_agent"))\
  .withColumn("ipCountryCode",f.lit(ipCountryCode))
except:
  NewData = False

# COMMAND ----------

NewData

# COMMAND ----------

if NewData:             
  temp = df.withColumn("date",f.from_unixtime(f.col('timestamp')/1000).cast("date"))\
                    .select("ifa", "platform","ipCountryCode","date","timestamp","ip")\
                    .groupBy( "ifa", "platform","date","ipCountryCode","ip")\
                    .agg(f.countDistinct("timestamp").alias("num_events_per_day"))

  temp.write.format("parquet").partitionBy("ipCountryCode", "date") \
    .mode("append") \
    .option("replaceWhere", "ipCountryCode = '"+ipCountryCode+"' and date>="+past_date.strftime('%Y-%m-%d')+" and date<="+today.strftime('%Y-%m-%d')) \
    .save( base_path + "input/mobile/aggregated_data/")


# COMMAND ----------

MobileExist = True
try:
  spark.read.format("parquet").option("basePath", base_path).load(base_path + "input/mobile/aggregated_data/ipCountryCode="+ipCountryCode)
except:
  MobileExist =False

# COMMAND ----------

if MobileExist:


  today = datetime.strptime(date,'%Y-%m-%d').date()#date.today()
  past_date = today - timedelta(days=31)
 

  temp = spark.read.format("parquet").option("basePath", base_path).load(base_path + "input/mobile/aggregated_data")\
                    .where("date>='"+past_date.strftime('%Y-%m-%d')+"' and date<='"+today.strftime('%Y-%m-%d')+"'")\
                    .where("ipCountryCode='"+ipCountryCode+"'").select("ifa", "ip","date","ipCountryCode")\
                    .groupBy("ip","date","ipCountryCode")\
                    .agg(f.countDistinct("ifa").alias("num_id2_per_day"))\
                    .withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d')))
  itisNew=False
  try:
    df = spark.read.parquet(base_path + "processing/mobile/flat_ip_withcount/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
  except:
    itisNew=True
  if itisNew:
    temp.write.format("parquet").partitionBy("ipCountryCode","proc_date") \
      .mode("append") \
      .option("replaceWhere", "ipCountryCode = '"+ipCountryCode+"'") \
      .save( base_path + "processing/mobile/flat_ip_withcount")


# COMMAND ----------


