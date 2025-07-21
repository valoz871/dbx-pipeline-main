# Databricks notebook source

def str2bool(v):
  return str(v).lower() in ("yes", "true", "t", "1")

# COMMAND ----------


base_path = dbutils.widgets.get("base_path")
ipCountryCode = dbutils.widgets.get("ipCountryCode")


# COMMAND ----------

import pyspark.sql.functions as f
from datetime import date, timedelta,datetime
from pyspark.sql.types import IntegerType
from pyspark.sql import Window, Row
import pyspark.sql.types as T
from pyspark.sql.types import IntegerType
date = dbutils.widgets.get("date")
today = datetime.strptime(date,'%Y-%m-%d').date()


# COMMAND ----------


def dice_coefficient(a, b):
    """dice coefficient"""
    if not len(a) or not len(b): return 0.0
    a = set(a)
    b = set(b)
    overlap = len(a.intersection(b))
    dice_coeff = overlap * 2.0/(len(a) + len(b))
    return dice_coeff
    
dice_coefficient_udf = f.udf(dice_coefficient, T.DoubleType())

def check_contains(array, value):
    if value is None:
        return False
    if value in array:
        return True
    else:
        return False

check_contains_udf = f.udf(check_contains, T.BooleanType())

# COMMAND ----------

MobileExist = True
try:
  spark.read.format("parquet").option("basePath", base_path).load(base_path + "input/mobile/aggregated_data/ipCountryCode="+ipCountryCode)
except:
  MobileExist =False

if MobileExist:
  pathTouse='individual/weekwindow/'
else:
  pathTouse='cookie/'

# COMMAND ----------

oldCountry = True 
try:
  listed = dbutils.fs.ls(base_path + "/output/weekwindow/persisted_graph/individual/ipCountryCode="+ipCountryCode+"/")
except:
  oldCountry = False
try:
  oldgraph = listed[-1][1].split("=")[1].replace("/", "")
except:
  oldCountry = False

# COMMAND ----------

oldCountry

# COMMAND ----------

if oldCountry:
  oldgraph

# COMMAND ----------

if oldCountry:
  oldgraph = listed[-1][1].split("=")[1].replace("/", "")
  old_graph_pre = spark.read.format("parquet").option("basePath", base_path).load(\
                                  base_path + "/output/weekwindow/persisted_graph/individual/ipCountryCode="+ipCountryCode+"/proc_date="+oldgraph)
  old_graph = old_graph_pre.select("supervertex","dev1").union(old_graph_pre.select("supervertex","dev2")).distinct()\
                    .groupBy("supervertex").agg(f.collect_set("dev1").alias("ids_array"))\
                    .select("supervertex", "ids_array")\
                    .withColumn("id", f.explode("ids_array"))\
                    .withColumnRenamed("supervertex", "supervertex_old")\
                    .withColumnRenamed("ids_array", "ids_array_old")\
                    .withColumn("size_old",f.size(f.col("ids_array_old")))\
                    .where("size_old<10000 and size_old>1").drop("size_old")

  new_graph_pre = spark.read.format("parquet").option("basePath", base_path).load( \
                    base_path + "/output/weekwindow/graph/individual/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
  new_graph = new_graph_pre.select("supervertex","dev1","ipCountryCode").union(new_graph_pre.select("supervertex","dev2","ipCountryCode")).distinct()\
                    .groupBy("supervertex","ipCountryCode").agg(f.collect_set("dev1").alias("ids_array"))\
                    .select("supervertex", "ids_array","ipCountryCode")\
                    .withColumn("id", f.explode("ids_array"))\
                    .withColumnRenamed("supervertex", "supervertex_new")\
                    .withColumnRenamed("ids_array", "ids_array_new")\
                    .withColumn("size_new",f.size(f.col("ids_array_new")))\
                    .where("size_new<10000 and size_new>1").drop("size_new")


  temp = old_graph.join(new_graph, "id", how="right")\
                .drop("id")\
                .distinct()\
                .withColumn("ids_array_old", f.coalesce(f.col("ids_array_old"), f.array()))\
                .withColumn("ids_array_new", f.coalesce(f.col("ids_array_new"), f.array()))

  

  users_not_matched_new = temp.withColumn("check",f.count(f.lit(1)).over(Window.partitionBy(f.col("supervertex_new"))))\
                            .where("supervertex_old is null and check=1")\
                            .withColumn("supervertex", f.col("supervertex_new"))\
                            .withColumnRenamed("ids_array_new", "ids_array")\
                            .select("supervertex_old","supervertex_new","supervertex", "ids_array","ipCountryCode")

# COMMAND ----------

if oldCountry:
  w_old = Window.partitionBy(f.col("supervertex_old")).orderBy(f.desc("dice"),"supervertex_new")
  w_new = Window.partitionBy(f.col("supervertex_new")).orderBy(f.desc("dice"),"supervertex_old")

  df = temp.withColumn("check",f.count(f.lit(1)).over(Window.partitionBy(f.col("supervertex_new"))))\
                        .where("!(supervertex_old is null and check=1)")\
                        .withColumn("dice", dice_coefficient_udf(f.col("ids_array_old"), f.col("ids_array_new")))\
                        .withColumn("flag", check_contains_udf(f.col("ids_array_new"), f.col("supervertex_old")))\
                        .withColumn("dice", f.when(f.col("flag"), f.lit(1.0))\
                                                .otherwise(f.col("dice")))\
                        .withColumn("rn", f.row_number().over(w_old))\
                        .where("rn == 1")\
                        .distinct()\
                        .drop("rn")\
                        .withColumn("rn", f.row_number().over(w_new))\
                        .where("rn == 1")\
                        .drop("rn")\
                        .distinct()

# COMMAND ----------

if oldCountry:
  checked = df.withColumn("supervertex", f.col("supervertex_old"))\
                                            .withColumnRenamed("ids_array_new", "ids_array")\
                                            .select("supervertex_old","supervertex_new","supervertex", "ids_array","ipCountryCode")\
                                            .union(users_not_matched_new)\
                                            .distinct()\
                                            .withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d')))
  others = new_graph.join(checked.select(f.col("supervertex_new"),f.lit(1).alias("test")),["supervertex_new"],"left")\
           .where("test is null")
  
  final_lookup = checked.select("supervertex_new","supervertex").union(others.select("supervertex_new",f.col("supervertex_new").alias("supervertex")))\
                 .drop("test")
  
  graph = spark.read.format("parquet").option("basePath", base_path).load(base_path +\
                  "output/weekwindow/graph/individual/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))\
                    .distinct()
  final_graph =  final_lookup\
  .select("supervertex_new","supervertex").join(graph.withColumnRenamed("supervertex","supervertex_new"),"supervertex_new").drop("supervertex_new")
else:
  final_graph = spark.read.format("parquet").option("basePath", base_path).load(base_path + \
                                  "output/weekwindow/graph/individual/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))\
                    .distinct()

itisNew=False
try:
  dftest = spark.read.parquet(base_path + \
                              "output/weekwindow/persisted_graph/individual/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
except:
  itisNew=True

if itisNew:
  final_graph.write.partitionBy("ipCountryCode","proc_date").format("parquet").mode("append")\
    .option("replaceWhere", "ipCountryCode = '"+ipCountryCode+"'")\
    .save(base_path + "output/weekwindow/persisted_graph/individual")

# COMMAND ----------

if oldCountry:
  final_graph = spark.read.parquet(base_path + \
                              "output/weekwindow/persisted_graph/individual/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))
  old_graph = spark.read.parquet(base_path + \
                              "output/weekwindow/persisted_graph/individual/ipCountryCode="+ipCountryCode+"/proc_date="+oldgraph)
  stats = old_graph.select("supervertex").distinct().withColumn("old",f.lit("1"))\
           .join(final_graph.select("supervertex").distinct().withColumn("new",f.lit("1")),["supervertex"],"outer")\
           .withColumn("check",f.when((f.col("old")==1) & (f.col("new")==1), f.lit("persisted") )\
                                      .otherwise(f.when((f.col("old")==1) & (f.col("new").isNull()), f.lit("deprecated"))\
                                                 .otherwise(f.when((f.col("old").isNull()) & (f.col("new")==1), f.lit("new")))))\
           .withColumn("ipCountryCode",f.lit(ipCountryCode))\
           .groupBy("ipCountryCode").pivot("check").agg(f.countDistinct("supervertex").alias("count"))\
           .withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d')))
  itisNew=False
  try:
    dftest = spark.read.parquet(base_path + \
                              "stat/persisted_graph_stats/proc_date="+today.strftime('%Y-%m-%d')+"/ipCountryCode="+ipCountryCode)
  except:
    itisNew=True

  if itisNew:
    stats.write.partitionBy("proc_date","ipCountryCode").format("parquet").mode("append")\
      .option("replaceWhere","proc_date="+today.strftime('%Y-%m-%d')+ " and ipCountryCode = '"+ipCountryCode+"'")\
      .save(base_path + "stat/persisted_graph_stat")

