# Databricks notebook source
def str2bool(v):
  return str(v).lower() in ("yes", "true", "t", "1")

# COMMAND ----------

base_path = dbutils.widgets.get("base_path")
cap_couples = dbutils.widgets.get("cap_couples")
ipCountryCode = dbutils.widgets.get("ipCountryCode")
addendum = str2bool(dbutils.widgets.get("add_previous_month_maid"))
daysago = int(dbutils.widgets.get("daysago"))


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
from datetime import date, timedelta,datetime
date = dbutils.widgets.get("date")

if addendum:
  today = datetime.strptime(date,'%Y-%m-%d').date()#date.today()
  past_date = today - timedelta(days=daysago)
  

MobileExist = True
path_addendum=''
try:
  spark.read.format("parquet").option("basePath", base_path).load(base_path + "input/mobile/aggregated_data/ipCountryCode="+ipCountryCode)
except:
  MobileExist =False
  
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
  
today = date

if MobileExist:
  pathTouse='individual/weekwindow/'
  if addendum:
    path_addendum='_addendum'
else:
  pathTouse='cookie/'
itisNew=False
try:
  df = spark.read.parquet( \
                          base_path + \
                  "processing/"+pathTouse+"/couplelist_graph/ipCountryCode="+ipCountryCode+"/proc_date="+today)#+today.strftime('%Y-%m-%d'))
except:
  itisNew=True
  

dfCountry = spark.createDataFrame([ipCountryCode,base_path,today,pathTouse,itisNew,path_addendum,cap_couples],StringType())
dfCountry.createOrReplaceTempView("dfCountry")

# COMMAND ----------

# MAGIC %scala
# MAGIC val ipCountryCode = spark.table("dfCountry").collect()(0)(0).toString
# MAGIC val base_path = spark.table("dfCountry").collect()(1)(0).toString
# MAGIC val today = spark.table("dfCountry").collect()(2)(0).toString
# MAGIC val pathTouse = spark.table("dfCountry").collect()(3)(0).toString
# MAGIC val itisNew = spark.table("dfCountry").collect()(4)(0).toString
# MAGIC val path_addendum = spark.table("dfCountry").collect()(5)(0).toString
# MAGIC val cap_couples = spark.table("dfCountry").collect()(6)(0).toString

# COMMAND ----------

# MAGIC %scala
# MAGIC val lookup_couples_hash = spark.read.format("parquet").option("basePath", base_path).load(base_path + 
# MAGIC            "processing/"+pathTouse+"/lookup_couples_hash"+path_addendum+"/ipCountryCode="+ipCountryCode+"/proc_date="+today)
# MAGIC             .distinct().where("dev1 is not null").where("dev2 is not null").distinct()
# MAGIC val tonotuse = lookup_couples_hash.select("dev1").union(lookup_couples_hash.select("dev2"))
# MAGIC                .groupBy("dev1").count().where("count>"+cap_couples+"").select("dev1")
# MAGIC tonotuse.createOrReplaceTempView("tonotuse")

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions.{col, lit} 
# MAGIC if (itisNew == "true"){  
# MAGIC val vert = spark.read.format("parquet").option("basePath", base_path).load(base_path +
# MAGIC           "processing/"+pathTouse+"/lookup_id_hash"+path_addendum+"/ipCountryCode="+ipCountryCode+"/proc_date="+today)
# MAGIC           .distinct().where("cookie_id is not null")
# MAGIC 
# MAGIC           //.join(tonotuse.withColumnRenamed("dev1","cookie_id").withColumn("test",lit(1)),Seq("cookie_id"),"left")
# MAGIC           //.where("test is null").drop("test")
# MAGIC 
# MAGIC val edg1 =  spark.read.format("parquet").option("basePath", base_path).load(base_path + 
# MAGIC            "processing/"+pathTouse+"/lookup_couples_hash"+path_addendum+"/ipCountryCode="+ipCountryCode+"/proc_date="+today)
# MAGIC            .join(tonotuse.withColumn("test",lit(1)),Seq("dev1"),"left")
# MAGIC            .where("test is null or (test=1 and level='high') or (test=1 and level='medium')").drop("test")
# MAGIC            .join(tonotuse.withColumnRenamed("dev1","dev2").withColumn("test",lit(1)),Seq("dev2"),"left")
# MAGIC            .where("test is null or (test=1 and level='high') or (test=1 and level='medium')").drop("test")
# MAGIC            .distinct().where("dev1 is not null")
# MAGIC            .where("dev2 is not null or (test=1 and level='high') or (test=1 and level='medium')").distinct()
# MAGIC            .rdd.map(row => Edge(row.getAs[Long]("dev1hash").toLong,row.getAs[Long]("dev2hash").toLong, 1))
# MAGIC val vertex: RDD[(VertexId, String)] = vert.rdd.map(row => (row.getAs[Long]("cookie_id_hash").toLong, row.getAs[String]("cookie_id")))
# MAGIC val ed: RDD[Edge[Int]] = edg1
# MAGIC val graph = Graph(vertex.persist(StorageLevel.MEMORY_AND_DISK_SER), ed.persist(StorageLevel.MEMORY_AND_DISK_SER))
# MAGIC val cc = graph.connectedComponents()
# MAGIC val supervertexToVerticesList = cc.vertices.groupBy(k => k._2).map(f => (f._1, f._2.map(cpl => cpl._1).toSet)).toDF().withColumnRenamed("_1","supervertex").withColumnRenamed("_2","ids_array").withColumn("proc_date",lit(today))
# MAGIC 
# MAGIC supervertexToVerticesList.withColumn("ipCountryCode", lit(ipCountryCode)).write.partitionBy("ipCountryCode","proc_date").format("parquet").mode("append")
# MAGIC   .option("replaceWhere", "ipCountryCode = '"+ipCountryCode+"'").save(base_path + "processing/"+pathTouse+"/couplelist_graph")
# MAGIC vertex.unpersist()
# MAGIC ed.unpersist()
# MAGIC }

# COMMAND ----------

import pyspark.sql.functions as f
from datetime import date, timedelta
from pyspark.sql import Window, Row
from pyspark.sql.types import StringType, IntegerType, BooleanType, MapType, ArrayType
from functools import reduce

# COMMAND ----------

itisNew=False
try:
  df = spark.read.parquet(base_path + "output/weekwindow/graph/individual/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
except:
  itisNew=True

if itisNew:
  tonotuse = spark.table("tonotuse")
  hashed_graph = spark.read.format("parquet").option("basePath", base_path).load(base_path + \
             "processing/"+pathTouse+"/couplelist_graph/ipCountryCode="+ipCountryCode+"/proc_date="+today)\
                  .withColumn("size",f.size("ids_array")).where("size>1")\
                  .withColumn("ids",f.explode("ids_array")).drop("ids_array")
  hashed_graph.count()                           
  lookup_couples_hash = spark.read.format("parquet").option("basePath", base_path)\
                     .load(base_path + \
             "processing/"+pathTouse+"/lookup_couples_hash"+path_addendum+"/ipCountryCode="+ipCountryCode+"/proc_date="+today)\
              .join(tonotuse.withColumn("test",f.lit(1)),["dev1"],"left")\
              .where("test is null or (test=1 and level='high') or (test=1 and level='medium')").drop("test")\
              .join(tonotuse.withColumnRenamed("dev1","dev2").withColumn("test",f.lit(1)),["dev2"],"left")\
              .where("test is null or (test=1 and level='high') or (test=1 and level='medium')").drop("test")
    
  df1 =lookup_couples_hash.join(hashed_graph.withColumnRenamed("ids","dev1hash").select("supervertex","dev1hash").distinct(),"dev1hash")
  df2 =lookup_couples_hash.join(hashed_graph.withColumnRenamed("ids","dev2hash").select("supervertex","dev2hash").distinct(),"dev2hash")

  df1.select("supervertex","dev1","dev2","type_dev1","type_dev2","level","ipCountryCode")\
    .union(df2.select("supervertex","dev1","dev2","type_dev1","type_dev2","level","ipCountryCode"))\
    .withColumn("proc_date",f.lit(today))\
    .write.partitionBy("ipCountryCode","proc_date").format("parquet").mode("append")\
    .option("replaceWhere", "ipCountryCode = '"+ipCountryCode+"'")\
    .save(base_path + "output/weekwindow/graph/individual/")
