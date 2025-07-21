# Databricks notebook source
from datetime import date, timedelta,datetime
import pyspark.sql.functions as f
from pyspark.sql import Window, Row
from pyspark.sql.types import StringType, IntegerType, BooleanType, MapType, ArrayType
from functools import reduce

from pyspark.sql import DataFrame

# COMMAND ----------

id5Bucket = dbutils.widgets.get("id5Bucket")
base_path = dbutils.widgets.get("base_path")
ipCountryCode = dbutils.widgets.get("ipCountryCode")
date = dbutils.widgets.get("date")
daysago = int(dbutils.widgets.get("daysago"))

# COMMAND ----------

newCountry = False 
try:
  dbutils.fs.ls(base_path+"input/cookie/aggregated_data/ipCountryCode="+ipCountryCode)
except:
  newCountry = True

# COMMAND ----------

if newCountry:
  today = datetime.strptime(date,'%Y-%m-%d').date()#date.today()
  datetime_object_2 = today 
  past_date = today - timedelta(days=daysago)
  datetime_object = past_date 
else:
  readLastDate = dbutils.fs.ls(base_path+"input/cookie/aggregated_data/ipCountryCode="+ipCountryCode)[-1][1].split("=")[1].replace("/", "")
  today = datetime.strptime(date,'%Y-%m-%d').date()
  datetime_object_2 = today 
  past_date = datetime.strptime(readLastDate, "%Y-%m-%d").date() + timedelta(days=1)
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

sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId",id5KeyId)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", id5SecretKey)
NewData = True
try:
  df = spark.read.parquet(id5Bucket+d+"/ipCountryCode="+ipCountryCode).withColumn("ipCountryCode",f.lit(ipCountryCode))
except:
  NewData = False

# COMMAND ----------

NewData

# COMMAND ----------

if NewData:
  df = spark.read.parquet(id5Bucket+d+"/ipCountryCode="+ipCountryCode).withColumn("ipCountryCode",f.lit(ipCountryCode))
  temp = df.withColumn("date",f.from_unixtime('timestamp').cast("date"))\
         .groupBy( "id5UID", "userAgent","date","ip","firstPartyUID","email","phoneNumber"\
                         ,"crossPartnerUserId","crossPartnerUserIdSource","partnerUserId","partnerId","idfa","gaid"\
                         ,"ipCountryCode","connect")\
         .agg(f.countDistinct("timestamp").alias("num_events_per_day"))

  temp.write.partitionBy("ipCountryCode", "date")\
          .mode("append")\
          .format("parquet")\
          .option("replaceWhere", "ipCountryCode = '"+ipCountryCode+"' and date>="+past_date.strftime('%Y-%m-%d')+" and date<="+today.strftime('%Y-%m-%d'))\
          .save( base_path + "input/cookie/aggregated_data/")

# COMMAND ----------

today = datetime.strptime(date,'%Y-%m-%d').date()#date.today()
past_date = today - timedelta(days=31)


temp = spark.read.format("parquet").load(base_path + "input/cookie/aggregated_data")\
                  .where("date>='"+past_date.strftime('%Y-%m-%d')+"' and date<='"+today.strftime('%Y-%m-%d')+"'")\
                  .where("ipCountryCode='"+ipCountryCode+"'")\
                  .select("id5UID", "ip","date","ipCountryCode")\
                  .groupBy("ip","date","ipCountryCode")\
                  .agg(f.countDistinct("id5UID").alias("num_id2_per_day"))\
                  .withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d')))

itisNew=False
try:
  df = spark.read.parquet(base_path + "processing/cookie/flat_ip_withcount/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
except:
  itisNew=True

if itisNew:
  temp.write.partitionBy("ipCountryCode","proc_date")\
      .mode("append")\
      .option("replaceWhere", "ipCountryCode = '"+ipCountryCode+"'")\
      .format("parquet").save(base_path + "processing/cookie/flat_ip_withcount")

# COMMAND ----------


temp = spark.read.format("parquet").load( base_path + "input/cookie/aggregated_data")\
                .where("date>='"+past_date.strftime('%Y-%m-%d')+"' and date<='"+today.strftime('%Y-%m-%d')+"'")\
                .where("ipCountryCode='"+ipCountryCode+"'")\
                .select("id5UID", "ip")\
                .agg(f.countDistinct("ip"),f.countDistinct("id5uid"))\
                .display()

# COMMAND ----------

temp = spark.read.format("parquet").load(base_path + "input/cookie/aggregated_data")\
          .where("date>='"+past_date.strftime('%Y-%m-%d')+"' and date<='"+today.strftime('%Y-%m-%d')+"'")\
          .where("ipCountryCode='"+ipCountryCode+"'")\
          .select("idfa", "ip")\
          .agg(f.countDistinct("ip"),f.countDistinct("idfa"))\
          .display()

# COMMAND ----------


