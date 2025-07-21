# Databricks notebook source
ipCountryCode = dbutils.widgets.get("ipCountryCode")
minProbThreshold = dbutils.widgets.get("minProbThreshold")
base_path = dbutils.widgets.get("base_path")
model_path = dbutils.widgets.get("model_path")
daysago = int(dbutils.widgets.get("daysago"))

import pandas as pd
import joblib
import numpy as np
import boto3 
from io import BytesIO
from pyspark.sql.functions import pandas_udf
from typing import Iterator, Tuple
import pyspark.sql.functions as f
import pyspark.sql.types as T 
from pyspark.sql.window import Window
from datetime import date, timedelta,datetime
date = dbutils.widgets.get("date")
today = datetime.strptime(date,'%Y-%m-%d').date()#date.today()
past_date = today - timedelta(days=daysago)

# COMMAND ----------


def read_joblib(path):
    ''' 
       Function to load a joblib file from an s3 bucket or local directory.
       Arguments:
       * path: an s3 bucket or local directory path where the file is stored
       Outputs:
       * file: Joblib file loaded
    '''

    # Path is an s3 bucket
    if path[:5] == 's3://':
        s3_bucket, s3_key = path.split('/')[2], path.split('/')[3:]
        s3_key = '/'.join(s3_key)
        with BytesIO() as f:
            boto3.client("s3").download_fileobj(Bucket=s3_bucket, Key=s3_key, Fileobj=f)
            f.seek(0)
            file = joblib.load(f)
    
    # Path is a local directory 
    else:
        with open(path, 'rb') as f:
            file = joblib.load(f)
    
    return file

def write_joblib(file, path):
    ''' 
       Function to write a joblib file to an s3 bucket or local directory.
       Arguments:
       * file: The file that you want to save 
       * path: an s3 bucket or local directory path. 
    '''

    # Path is an s3 bucket
    if path[:5] == 's3://':
        s3_bucket, s3_key = path.split('/')[2], path.split('/')[3:]
        s3_key = '/'.join(s3_key)
        with BytesIO() as f:
            joblib.dump(file, f)
            f.seek(0)
            boto3.client("s3").upload_fileobj(Bucket=s3_bucket, Key=s3_key, Fileobj=f)
    
    # Path is a local directory 
    else:
        with open(path, 'wb') as f:
            joblib.dump(file, f)
  

# COMMAND ----------

from pyspark.sql.functions import pandas_udf


@pandas_udf("float")


def predict_category_udf(batch_iter: Iterator[Tuple[pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series]]) -> Iterator[pd.Series]:
  #Load the model in each executor, only once
  model = read_joblib(model_path)
  #For each batch, apply transformation
  for num_ip_dev1,\
      tot_events_dev1,\
      num_ip_dev2,\
      tot_events_dev2,\
      common_ip, common_ip_weighted,\
      num_events_dev1_on_intersection,\
      num_events_dev2_on_intersection,\
      dice, jaccard, overlap,\
      common_ip_ratio1, common_ip_ratio2, common_ip_events1,\
      common_ip_events2, sameDeviceType, sameBrowser, sameOS in batch_iter:
      df = pd.DataFrame({ 'num_ip_dev1': num_ip_dev1, 'tot_events_dev1': tot_events_dev1, 'num_ip_dev2': num_ip_dev2, 'tot_events_dev2': tot_events_dev2, 'common_ip': common_ip, 'common_ip_weighted': common_ip_weighted, 'num_events_dev1_on_intersection': num_events_dev1_on_intersection, 'num_events_dev2_on_intersection': num_events_dev2_on_intersection, 'dice': dice, 'jaccard': jaccard, 'overlap': overlap, 'common_ip_ratio1': common_ip_ratio1, 'common_ip_ratio2': common_ip_ratio2, 'common_ip_events1': common_ip_events1, 'common_ip_events2': common_ip_events2, 'sameDeviceType': sameDeviceType, 'sameBrowser': sameBrowser, 'sameOS': sameOS } )
      proba = pd.Series(model.predict_proba(df.astype(float))[:,1])
      yield proba

# COMMAND ----------


df = spark.read.format("parquet").option("basePath", base_path).load(base_path +\
  "processing/cookie/df_final/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))

predicted = df.where("num_events_dev1 is not null")\
            .withColumn("proba", predict_category_udf(f.col("num_ip_dev1").cast("double"),f.col("tot_events_dev1").cast("double"),\
                                                       f.col("num_ip_dev2").cast("double"),\
                                                       f.col("tot_events_dev2").cast("double"),f.col("common_ip").cast("double"),\
                                                       f.col("common_ip_weighted").cast("double"),\
                                                       f.col("num_events_dev1_on_intersection").cast("double"),\
                                                       f.col("num_events_dev2_on_intersection").cast("double"),f.col("dice").cast("double"),\
                                                       f.col("jaccard").cast("double"),f.col("overlap").cast("double"),\
                                                       f.col("common_ip_ratio1").cast("double"),\
                                                       f.col("common_ip_ratio2").cast("double"),f.col("common_ip_events1").cast("double"),\
                                                       f.col("common_ip_events2").cast("double"),\
                                                       f.col("sameDeviceType").cast("double"),\
                                                       f.col("sameBrowser").cast("double"),f.col("sameOS").cast("double")))\
            .select("dev1","dev2","ipCountryCode","proba")\
            .withColumn("level",f.when(f.col("proba")>=0.95, f.lit("high"))\
                        .otherwise(f.when((f.col("proba")<0.95) & (f.col("proba")>=0.85), f.lit("medium") )\
                        .otherwise(f.when((f.col("proba")>=minProbThreshold) & (f.col("proba")<0.85), f.lit("low"))\
                        .otherwise(f.lit("discard")))))

# COMMAND ----------


itisNew=False
try:
  df = spark.read.parquet(base_path + "processing/cookie/all_id_couples/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
except:
  itisNew=True

if itisNew:

  firstParty = spark.read.format("parquet").option("basePath", base_path)\
                .load(base_path + "input/cookie/aggregated_data/").where("ipCountryCode='"+ipCountryCode+"'")\
                .where("date>='"+past_date.strftime('%Y-%m-%d')+"' and date<='"+today.strftime('%Y-%m-%d')+"'")\
                .select("firstPartyUID","id5UID","ipCountryCode")\
                .where("firstPartyUID is not null")\
                .groupBy("firstPartyUID","id5UID","ipCountryCode").count().drop("count")\
                .withColumn("id5id_for_first",f.count(f.lit(1)).over(Window.partitionBy("firstPartyUID")))\
                .where("id5id_for_first > 1")\
                .withColumn("type",f.lit("firstPartyUID"))\
                .withColumn("level",f.lit("high"))\
                .distinct()\
                .select("firstPartyUID","id5UID","ipCountryCode","level")\
                .withColumn("type_dev1",f.lit("firstPartyUID"))\
                .withColumn("type_dev2",f.lit("id5UID"))

  email = spark.read.format("parquet").option("basePath", base_path)\
                .load(base_path + "input/cookie/aggregated_data/").where("ipCountryCode='"+ipCountryCode+"'")\
                .where("date>='"+past_date.strftime('%Y-%m-%d')+"' and date<='"+today.strftime('%Y-%m-%d')+"'")\
                .select("id5UID","email","ipCountryCode")\
                .where("email is not null")\
                .withColumn("type",f.lit("email"))\
                .withColumn("level",f.lit("high"))\
                .distinct()\
                .select("email","id5UID","ipCountryCode","level")\
                .withColumn("type_dev1",f.lit("email"))\
                .withColumn("type_dev2",f.lit("id5UID"))

  df_tot_couples = predicted.select("dev1","dev2","ipCountryCode","level")\
                    .where("level != 'discard'")\
                    .withColumn("type_dev1",f.lit("id5UID"))\
                    .withColumn("type_dev2",f.lit("id5UID"))\
                    .union(email)\
                    .union(firstParty)

  df_tot_couples.withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d'))).write.partitionBy("ipCountryCode","proc_date").format("parquet").mode("append") \
    .option("replaceWhere", "ipCountryCode = '"+ipCountryCode+"'").save(base_path + "processing/cookie/all_id_couples/")


# COMMAND ----------

# hashing

unique_id_udf = f.udf(lambda x: hash(x), T.LongType())


dfWithHash = spark.read.format("parquet").option("basePath", base_path).load(base_path +\
                 "processing/cookie/all_id_couples/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))\
                .withColumn("dev1hash", unique_id_udf(f.col("dev1")))\
                .withColumn("dev2hash", unique_id_udf(f.col("dev2")))
                    


# COMMAND ----------

itisNew=False
try:
  df = spark.read.parquet(base_path + "processing/cookie/lookup_id_hash/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
except:
  itisNew=True

if itisNew:
  dfWithHash.select("dev1", "dev1hash","type_dev1","ipCountryCode")\
          .union(dfWithHash.select("dev2", "dev2hash","type_dev1","ipCountryCode"))\
          .where("dev1 is not null")\
          .withColumnRenamed("dev1", "cookie_id")\
          .withColumnRenamed("dev1hash", "cookie_id_hash")\
          .withColumnRenamed("type_dev1", "type")\
          .distinct()\
          .withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d')))\
          .write.partitionBy("ipCountryCode","proc_date").format("parquet").mode("append")\
          .option("replaceWhere", "ipCountryCode = '"+ipCountryCode+"'")\
          .save(base_path + "processing/cookie/lookup_id_hash/")

itisNew=False
try:
  df = spark.read.parquet(base_path + "processing/cookie/lookup_couples_hash/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
except:
  itisNew=True

if itisNew:
  dfWithHash.withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d'))).write.partitionBy("ipCountryCode","proc_date").format("parquet").mode("append") \
    .option("replaceWhere", "ipCountryCode = '"+ipCountryCode+"'").save(base_path + "processing/cookie/lookup_couples_hash/")

# COMMAND ----------


