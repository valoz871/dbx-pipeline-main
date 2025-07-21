# Databricks notebook source
ipCountryCode = dbutils.widgets.get("ipCountryCode")
minProbThreshold = dbutils.widgets.get("minProbThreshold_mobile")
base_path = dbutils.widgets.get("base_path")
maid_model_path = dbutils.widgets.get("maid_model_path")


import pandas as pd
from datetime import date, timedelta,datetime
import joblib
import numpy as np
import boto3 
from io import BytesIO
from pyspark.sql.functions import pandas_udf
from typing import Iterator, Tuple
import pyspark.sql.functions as f
date = dbutils.widgets.get("date")

# COMMAND ----------

today = datetime.strptime(date,'%Y-%m-%d').date()#date.today()

# COMMAND ----------



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


def predict_category_udf(batch_iter: Iterator[Tuple[pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series]]) -> Iterator[pd.Series]:
  #Load the model in each executor, only once
  model = read_joblib(maid_model_path)
  #For each batch, apply transformation
  for sameOS,\
      common_ip, common_ip_weighted,\
      num_events_dev1_on_intersection,\
      num_events_dev2_on_intersection,\
      dice, jaccard, overlap,\
      common_ip_ratio1, common_ip_ratio2, common_ip_events1,\
      common_ip_events2 in batch_iter:
      df = pd.DataFrame({ 'sameOS': sameOS, 'common_ip': common_ip, 'common_ip_weighted': common_ip_weighted, 'num_events_dev1_on_intersection': num_events_dev1_on_intersection, 'num_events_dev2_on_intersection': num_events_dev2_on_intersection, 'dice': dice, 'jaccard': jaccard, 'overlap': overlap, 'common_ip_ratio1': common_ip_ratio1, 'common_ip_ratio2': common_ip_ratio2, 'common_ip_events1': common_ip_events1, 'common_ip_events2': common_ip_events2 } )
      proba = pd.Series(model.predict_proba(df.astype(float))[:,1])
      yield proba

# COMMAND ----------

MobileExist = True
try:
  spark.read.format("parquet").option("basePath", base_path).load(base_path + "input/mobile/aggregated_data/ipCountryCode="+ipCountryCode)
except:
  MobileExist =False

# COMMAND ----------

if MobileExist:
  df = spark.read.format("parquet").option("basePath", base_path).load(base_path + \
                    "processing/mobile/weekwindow/df_final/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))

# COMMAND ----------

if MobileExist:
  predicted = df\
            .withColumn("proba", predict_category_udf(f.col("sameOS").cast("double"),f.col("common_ip").cast("double"),\
                                                       f.col("common_ip_weighted").cast("double"),\
                                                       f.col("num_events_dev1_on_intersection").cast("double"),\
                                                       f.col("num_events_dev2_on_intersection").cast("double"),f.col("dice").cast("double"),\
                                                       f.col("jaccard").cast("double"),f.col("overlap").cast("double"),\
                                                       f.col("common_ip_ratio1").cast("double"),\
                                                       f.col("common_ip_ratio2").cast("double"),f.col("common_ip_events1").cast("double"),\
                                                       f.col("common_ip_events2").cast("double")))

# COMMAND ----------

if MobileExist:
  allck = spark.read.format("parquet").option("basePath", base_path).load(base_path + \
                               "processing/cookie/lookup_couples_hash/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))

# COMMAND ----------

from pyspark.sql.functions import udf, col, countDistinct, max
import pyspark.sql.functions as f
import pyspark.sql.types as T 

unique_id_udf = udf(lambda x: hash(x), T.LongType())

# COMMAND ----------

if MobileExist:
  itisNew=False
  try:
    df = spark.read.parquet(base_path + "processing/individual/weekwindow/lookup_couples_hash/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
  except:
    itisNew=True
  if itisNew:
    predicted.withColumn("level",f.when(f.col("proba")>=0.95, f.lit("high"))\
                        .otherwise(f.when((f.col("proba")<0.95) & (f.col("proba")>=0.85), f.lit("medium") )\
                        .otherwise(f.when((f.col("proba")>= minProbThreshold) & (f.col("proba")<0.85), f.lit("low"))\
                        .otherwise(f.lit("discard")))))\
                        .select("dev1","dev2","ipCountryCode","level","flag_ck_dev1","flag_ck_dev2","mf_os_dev1","mf_os_dev2")\
                    .withColumn("type_dev1",f.when((f.col("flag_ck_dev1")==0) & (f.col("mf_os_dev1")=='ios'), f.lit("ios"))\
                                .otherwise(f.when((f.col("flag_ck_dev1")==0) & (f.col("mf_os_dev1")=='android'), f.lit("android"))\
                                .otherwise(f.lit("id5UID"))))\
                    .withColumn("type_dev2",f.when((f.col("flag_ck_dev2")==0) & (f.col("mf_os_dev2")=='ios'), f.lit("ios"))\
                                .otherwise(f.when((f.col("flag_ck_dev2")==0) & (f.col("mf_os_dev2")=='android'), f.lit("android"))\
                                .otherwise(f.lit("id5UID"))))\
                    .drop("flag_ck_dev1","flag_ck_dev2","mf_os_dev1","mf_os_dev2")\
                    .where("level != 'discard'").withColumn("dev1hash", unique_id_udf(col("dev1")))\
                    .withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d')))\
                    .withColumn("dev2hash", unique_id_udf(col("dev2"))).select([f.col(c) for c in allck.columns]).union(allck)\
                    .write.format("parquet").mode("append").partitionBy("ipCountryCode","proc_date")\
                    .option("replaceWhere", "ipCountryCode = '"+ipCountryCode+"'").save(base_path + "processing/individual/weekwindow/lookup_couples_hash")

  

# COMMAND ----------

if MobileExist:
  dfWithHash= spark.read.format("parquet").option("basePath", base_path).load(base_path + \
              "processing/individual/weekwindow/lookup_couples_hash/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
  
  itisNew=False
  try:
    df = spark.read.parquet(base_path + "processing/individual/weekwindow/lookup_id_hash/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
  except:
    itisNew=True
  if itisNew:
    dfWithHash.select("dev1", "dev1hash","type_dev1","ipCountryCode").union(dfWithHash.select("dev2", "dev2hash","type_dev1","ipCountryCode"))\
      .where("dev1 is not null")\
      .withColumnRenamed("dev1", "cookie_id")\
      .withColumnRenamed("dev1hash", "cookie_id_hash")\
      .withColumnRenamed("type_dev1", "type")\
      .distinct()\
      .withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d')))\
      .write.partitionBy("ipCountryCode","proc_date").format("parquet").mode("append")\
    .option("replaceWhere", "ipCountryCode = '"+ipCountryCode+"'").save(base_path + "processing/individual/weekwindow/lookup_id_hash/")


