# Databricks notebook source
import pyspark.sql.functions as f
from datetime import date, timedelta,datetime
from pyspark.sql import Window, Row
import collections
import itertools
from pyspark.sql.functions import udf
import math
from pyspark.sql.types import StringType, IntegerType, BooleanType, MapType, ArrayType, LongType
import pandas as pd
import joblib
import numpy as np
import boto3 
from io import BytesIO
from pyspark.sql.functions import pandas_udf
from typing import Iterator, Tuple


def createConnections(a_friend):
  ip = a_friend[0]
  cookie_info_list = a_friend[1]
  ndev = a_friend[2]
  all_connections = []
    
  for cookie_info_pair in itertools.combinations(cookie_info_list, 2):
        # creo le coppie ordinate
      if cookie_info_pair[0][0] > cookie_info_pair[1][0]:
            a_tupla = tuple(itertools.chain.from_iterable((cookie_info_pair[1], cookie_info_pair[0])))
            all_connections.append((ip , 1./ndev) + a_tupla)
      else:
            a_tupla = tuple(itertools.chain.from_iterable(cookie_info_pair))
            all_connections.append((ip , 1./ndev) + a_tupla)
  return all_connections
 


def str2bool(v):
  return str(v).lower() in ("yes", "true", "t", "1")

date = dbutils.widgets.get("date")
base_path = dbutils.widgets.get("base_path")
ipCountryCode = dbutils.widgets.get("ipCountryCode")
max_dim_ip = dbutils.widgets.get("max_dim_ip")

daysago = int(dbutils.widgets.get("daysago"))

today = datetime.strptime(date,'%Y-%m-%d').date()
past_date = today - timedelta(days=daysago)
household_model = dbutils.widgets.get("household_model")


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

itisNew=False
try:
  df = spark.read.parquet(base_path + "processing/household/df_pair/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
except:
  itisNew=True
if itisNew:
  
  temp = spark.read.format("parquet").option("basePath", base_path).load(base_path + \
             "output/weekwindow/persisted_graph/individual/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d')).distinct()
  idsup = temp.select("dev1","type_dev1","supervertex").union(temp.select("dev2","type_dev2","supervertex"))

  MobileExist = True
  try:
    spark.read.format("parquet").option("basePath", base_path).load(base_path + "input/mobile/aggregated_data/ipCountryCode="+ipCountryCode)
  except:
    MobileExist =False

  if MobileExist:
    flat_ip_ck = spark.read.format("parquet").option("basePath", base_path).load(base_path + "processing/cookie/flat_ip_withcount/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))\
                    .select("ip", "num_id2_per_day","ipCountryCode")\
                    .groupBy("ip","ipCountryCode").agg(f.max("num_id2_per_day").alias("max_num_id2_per_day"))\
                    .where("max_num_id2_per_day <=" +max_dim_ip)\
                    .select("ip")\
                    .distinct()
  
    flat_ip_maid = spark.read.format("parquet").option("basePath", base_path).load(base_path + "processing/mobile/flat_ip_withcount/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))\
                    .select("ip", "num_id2_per_day","ipCountryCode")\
                    .groupBy("ip","ipCountryCode").agg(f.max("num_id2_per_day").alias("max_num_id2_per_day"))\
                    .where("max_num_id2_per_day <=" +max_dim_ip)\
                    .select("ip")\
                    .distinct()
    

    flat_ip = flat_ip_ck.union(flat_ip_maid).distinct()
    temp = spark.read.format("parquet").option("basePath", base_path).load(base_path+"input/mobile/aggregated_data")\
                      .where("date>='"+past_date.strftime('%Y-%m-%d')+"' and date<='"+today.strftime('%Y-%m-%d')+"'")\
                      .where("ipCountryCode='"+ipCountryCode+"'")\
                      .withColumnRenamed("country","ipCountryCode").select("ifa", "ip","ipCountryCode","num_events_per_day")\
                      .withColumnRenamed("ifa","dev1")
  else:
    flat_ip = spark.read.format("parquet").option("basePath", base_path).load(base_path + \
                    "processing/cookie/flat_ip_withcount/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))\
                    .select("ip", "num_id2_per_day","ipCountryCode")\
                    .groupBy("ip","ipCountryCode").agg(f.max("num_id2_per_day").alias("max_num_id2_per_day"))\
                    .where("max_num_id2_per_day <=" +max_dim_ip)\
                    .select("ip")\
                    .distinct()

  temp2 = spark.read.format("parquet").option("basePath", base_path).load(base_path+"input/cookie/aggregated_data")\
                      .where("date>='"+past_date.strftime('%Y-%m-%d')+"' and date<='"+today.strftime('%Y-%m-%d')+"'")\
                      .where("ipCountryCode='"+ipCountryCode+"'")\
                      .select("id5UID", "ip","ipCountryCode","num_events_per_day")\
                      .withColumnRenamed("id5UID","dev1").distinct()
  if MobileExist:  
    idip = temp.union(temp2)
  else:
    idip = temp2
  
  supip = idsup.join(idip,"dev1").distinct()

  ip_lookup = supip.groupBy("ip").agg(f.countDistinct("supervertex").alias("count_sup")).orderBy(f.desc("count_sup"))#.where("count_sup<="+n_dev)

  w = Window.partitionBy(f.col("supervertex"))
  df = supip.join(ip_lookup.select("ip"),"ip").groupBy("ip", "supervertex").agg(f.sum("num_events_per_day").alias("num_events"))\
          .join(flat_ip, ["ip"])\
          .where("ip is not null")\
          .where("supervertex is not null")\
          .withColumn("num_ip", f.size(f.collect_set(f.col("ip")).over(w)))\
          .withColumn("tot_events", f.sum(f.col("num_events")).over(w))#.where("num_ip<="+n_ip)
  
  ip_group = df.withColumn("info_array", f.array(df.supervertex, df.num_ip, df.num_events, df.tot_events ))\
                .select("ip","info_array")\
                .groupBy("ip")\
                .agg(f.sort_array(f.collect_set(f.col("info_array"))).alias("dev4ip"), f.count(f.col("info_array")).alias("ndev")).where("ndev<51")
  
  friend_connections = ip_group.rdd.flatMap(lambda rec: createConnections(rec)).toDF()\
                            .withColumnRenamed("_1", "ip_comune")\
                            .withColumnRenamed("_2", "conn_weight")\
                            .withColumnRenamed("_3", "superv1")\
                            .withColumnRenamed("_4", "num_ip_dev1")\
                            .withColumnRenamed("_5", "num_events_dev1")\
                            .withColumnRenamed("_6", "tot_events_dev1")\
                            .withColumnRenamed("_7", "superv2")\
                            .withColumnRenamed("_8", "num_ip_dev2")\
                            .withColumnRenamed("_9", "num_events_dev2")\
                            .withColumnRenamed("_10", "tot_events_dev2")
  
  df_pair = friend_connections\
         .groupBy("superv1","num_ip_dev1","tot_events_dev1"\
                  ,"superv2","num_ip_dev2","tot_events_dev2",\
                  "ip_comune")\
         .agg(f.sum("conn_weight").alias("conn_weight"),\
                         f.sum("num_events_dev1").alias("num_events_dev1"),f.sum("num_events_dev2").alias("num_events_dev2"))\
         .select( "ip_comune", "conn_weight", "superv1", "num_ip_dev1", "num_events_dev1", "tot_events_dev1",\
                            "superv2", "num_ip_dev2", "num_events_dev2", "tot_events_dev2")\
         .groupBy("superv1", "superv2","num_ip_dev1", "tot_events_dev1", "num_ip_dev2", "tot_events_dev2")\
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
         .withColumn("common_ip_events1", (f.col("num_events_dev1_on_intersection") +\
                                           f.col("num_events_dev2_on_intersection"))/(f.col("tot_events_dev1") + f.col("tot_events_dev2")))\
         .withColumn("common_ip_events2", f.col("num_events_dev1_on_intersection") / (f.col("tot_events_dev1") +\
                                           f.col("num_events_dev2_on_intersection") / f.col("tot_events_dev2")))\
         .withColumn("ipCountryCode",f.lit(ipCountryCode)).withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d')))

  df_pair.write.partitionBy("ipCountryCode","proc_date").format("parquet").mode("append")\
       .option("replaceWhere", "ipCountryCode = '"+ipCountryCode+"'").save(base_path + "processing/household/df_pair")


# COMMAND ----------

itisNew=False
try:
  df = spark.read.parquet(base_path + "processing/household/predicted_proba/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
except:
  itisNew=True
if itisNew:
  
  filename = household_model

  @pandas_udf("float")

  def predict_category_udf(batch_iter: Iterator[Tuple[pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series]]) -> Iterator[pd.Series]:
  #Load the model in each executor, only once
    model = read_joblib(filename)
  #For each batch, apply transformation
    for num_ip_dev1,\
        tot_events_dev1, num_ip_dev2,\
        tot_events_dev2,\
        common_ip,\
        common_ip_weighted,\
        num_events_dev1_on_intersection, num_events_dev2_on_intersection, dice,\
        jaccard, overlap, common_ip_ratio1,\
        common_ip_ratio2,common_ip_events1,common_ip_events2 in batch_iter:
        df = pd.DataFrame({ 'num_ip_dev1': num_ip_dev1, 'tot_events_dev1': tot_events_dev1, 'num_ip_dev2': num_ip_dev2, 'tot_events_dev2': tot_events_dev2,'common_ip':common_ip, 'common_ip_weighted': common_ip_weighted, 'num_events_dev1_on_intersection': num_events_dev1_on_intersection, 'num_events_dev2_on_intersection': num_events_dev2_on_intersection, 'dice': dice, 'jaccard': jaccard, 'overlap': overlap, 'common_ip_ratio1': common_ip_ratio1,'common_ip_ratio2':common_ip_ratio2 ,'common_ip_events1': common_ip_events1,'common_ip_events2': common_ip_events2} )
        proba = pd.Series(model.predict_proba(df.astype(float))[:,1])
        yield proba
  
  df_pair= spark.read.format("parquet").option("basePath",base_path)\
                      .load(base_path + "processing/household/df_pair/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))

  predicted = df_pair\
            .withColumn("proba", predict_category_udf(f.col("num_ip_dev1").cast("double"),f.col("tot_events_dev1").cast("double"),\
                                                       f.col("num_ip_dev2").cast("double"),f.col("tot_events_dev2").cast("double"),\
                                                       f.col("common_ip").cast("double"),\
                                                       f.col("common_ip_weighted").cast("double"),\
                                                       f.col("num_events_dev1_on_intersection").cast("double"),\
                                                       f.col("num_events_dev2_on_intersection").cast("double"),\
                                                       f.col("dice").cast("double"),f.col("jaccard").cast("double"),f.col("overlap").cast("double"),\
                                                       f.col("common_ip_ratio1").cast("double"),\
                                                       f.col("common_ip_ratio2").cast("double"),f.col("common_ip_events1").cast("double"),\
                                                       f.col("common_ip_events2").cast("double")))
  
  predicted.withColumn("bin",f.bround(f.col("proba")*100/5,0)).write.partitionBy("ipCountryCode","proc_date").format("parquet").mode("append")\
                      .option("replaceWhere", "ipCountryCode = '"+ipCountryCode+"'").save(base_path + "processing/household/predicted_proba")

# COMMAND ----------

itisNew=False
try:
  df = spark.read.parquet(base_path + "processing/household/vert/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
except:
  itisNew=True

pred = spark.read.format("parquet").option("basePath",base_path)\
                      .load(base_path + "processing/household/predicted_proba/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))

good_superv =pred.where("bin>=14").select("superv1","superv2")\
                     .union(pred.where("bin>=14").select("superv2","superv1"))\
                     .groupBy("superv1").agg(f.collect_set("superv2").alias("ids_array"))\
                     .withColumn("size",f.size("ids_array")).select("superv1")#.where("size<"+n).select("superv1")
 
  
step1 = good_superv.join(pred.where("bin>=14"),"superv1")
step2 = good_superv.withColumnRenamed("superv1","superv2").join(step1.where("bin>=14"),"superv2")
#step2.write.mode("overwrite").format("parquet").save("s3://wr-working/Andrea/Output/crossDev/HouseHolder/id5_test/coutries/ipCountryCode="+ipCountryCode+"/outputs/cleaned_predicted_1month_wind_2022_10_25")

itisNew=False
try:
  df = spark.read.parquet(base_path + "processing/household/vert/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
except:
  itisNew=True
if itisNew:
  step2.where("bin>=14").select("superv1").union(step2.where("bin>=14").select("superv2")).select("superv1").distinct()\
       .withColumn("ipCountryCode",f.lit(ipCountryCode)).withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d')))\
       .write.partitionBy("ipCountryCode","proc_date").format("parquet").mode("append")\
       .option("replaceWhere", "ipCountryCode = '"+ipCountryCode+"'").save(base_path + "processing/household/vert")

itisNew=False
try:
  df = spark.read.parquet(base_path + "processing/household/edg/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
except:
  itisNew=True
if itisNew:
  step2.where("bin>=14").select("superv1","superv2").distinct()\
       .withColumn("ipCountryCode",f.lit(ipCountryCode)).withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d')))\
       .write.partitionBy("ipCountryCode","proc_date").format("parquet").mode("append")\
       .option("replaceWhere", "ipCountryCode = '"+ipCountryCode+"'").save(base_path + "processing/household/edg")

# COMMAND ----------

# MAGIC %scala
# MAGIC import spark.implicits._
# MAGIC import sqlContext.implicits._
# MAGIC import org.apache.spark.sql._
# MAGIC import org.apache.spark.sql.types._
# MAGIC import org.apache.spark.sql.Row
# MAGIC import org.apache.spark.graphx._
# MAGIC import org.apache.spark.rdd.RDD
# MAGIC import org.apache.spark.storage._
# MAGIC import scala.collection.mutable.WrappedArray
# MAGIC import org.apache.spark.sql.functions.max

# COMMAND ----------

from pyspark.sql.types import StringType
from datetime import date, timedelta
itisNew=False
try:
  df = spark.read.parquet(base_path + "processing/household/couplelist_graph/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
except:
  itisNew=True

dfCountry = spark.createDataFrame([ipCountryCode,base_path,today.strftime('%Y-%m-%d'),itisNew],StringType())
dfCountry.createOrReplaceTempView("dfCountry")

# COMMAND ----------

# MAGIC %scala
# MAGIC val ipCountryCode = spark.table("dfCountry").collect()(0)(0).toString
# MAGIC val base_path = spark.table("dfCountry").collect()(1)(0).toString
# MAGIC val today = spark.table("dfCountry").collect()(2)(0).toString
# MAGIC val itisNew = spark.table("dfCountry").collect()(3)(0).toString

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions.{col, lit,size,desc} 
# MAGIC 
# MAGIC if (itisNew == "true"){
# MAGIC val vert = spark.read.format("parquet").option("basePath", base_path).load(base_path +
# MAGIC                "processing/household/vert/ipCountryCode="+ipCountryCode+"/proc_date="+today)
# MAGIC           .distinct().where("superv1 is not null")
# MAGIC           
# MAGIC 
# MAGIC val edg1 =  spark.read.format("parquet").option("basePath", base_path).load(base_path +
# MAGIC                "processing/household/edg/ipCountryCode="+ipCountryCode+"/proc_date="+today)
# MAGIC            .where("superv1 is not null")
# MAGIC            .where("superv2 is not null").distinct()
# MAGIC            .rdd.map(row => Edge(row.getAs[Long]("superv1").toLong,row.getAs[Long]("superv2").toLong, 1))
# MAGIC 
# MAGIC val vertex: RDD[(VertexId, String)] = vert.rdd.map(row => (row.getAs[Long]("superv1").toLong,"1"))
# MAGIC val ed: RDD[Edge[Int]] = edg1
# MAGIC val graph = Graph(vertex.persist(StorageLevel.MEMORY_AND_DISK_SER), ed.persist(StorageLevel.MEMORY_AND_DISK_SER))
# MAGIC val cc = graph.connectedComponents(1)
# MAGIC val supervertexToVerticesList = cc.vertices.groupBy(k => k._2).map(f => (f._1, f._2.map(cpl => cpl._1).toSet)).toDF().withColumnRenamed("_1","supervertexHH").withColumnRenamed("_2","sups_array").withColumn("proc_date",lit(today))
# MAGIC 
# MAGIC supervertexToVerticesList.withColumn("ipCountryCode", lit(ipCountryCode))
# MAGIC   .write.partitionBy("ipCountryCode","proc_date").format("parquet").mode("append")
# MAGIC   .option("replaceWhere", "ipCountryCode = '"+ipCountryCode+"'").save(base_path + "processing/household/couplelist_graph")
# MAGIC }

# COMMAND ----------

unique_id_udf = udf(lambda x: hash(x), LongType())

try:
  df = spark.read.parquet(base_path + "output/weekwindow/graph/household/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
except:
  itisNew=True
  
if itisNew:

  reader = spark.read.format("parquet").option("basePath", base_path).load(base_path + \
                    "processing/household/couplelist_graph/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))\
                    .withColumn("HouseHold",unique_id_udf(f.col("supervertexHH"))).withColumn("superv",f.explode("sups_array"))

  individuals =  spark.read.format("parquet").option("basePath", base_path).load(base_path + \
             "output/weekwindow/persisted_graph/individual/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))\
                    .select(f.col("supervertex").alias("superv"),f.col("ipCountryCode"),f.col("proc_date")).distinct()

  toAdd = individuals.join(reader.select("superv").distinct()\
                         .withColumn("there",f.lit(1)), "superv","left").where("there is null").drop("there")\
                   .withColumn("HouseHold",unique_id_udf(f.col("superv"))).select("HouseHold","superv","ipCountryCode","proc_date")

  household = reader.select("HouseHold","superv","ipCountryCode","proc_date").union(toAdd)
  
  household.write.format("parquet").mode("append")\
                         .partitionBy("ipCountryCode","proc_date")\
                         .option("replaceWhere", "ipCountryCode = '"+ipCountryCode+"'")\
                         .save(base_path + "output/weekwindow/graph/household")


# COMMAND ----------


