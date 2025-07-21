# Databricks notebook source
import pyspark.sql.functions as f
from datetime import date, timedelta,datetime
from pyspark.sql import Window, Row
from pyspark.sql.types import StringType, IntegerType, BooleanType, MapType, ArrayType
from functools import reduce
from pyspark.sql import DataFrame


# COMMAND ----------

date=dbutils.widgets.get("date")
ipCountryCode = dbutils.widgets.get("ipCountryCode")
base_path = dbutils.widgets.get("base_path")
max_dim_ip = dbutils.widgets.get("max_dim_ip")
max_dim_ip_mobile = dbutils.widgets.get("max_dim_ip_mobile")
daysago = int(dbutils.widgets.get("daysago"))

# COMMAND ----------

today = datetime.strptime(date,'%Y-%m-%d').date()#date.today()
datetime_object_2 = today #datetime.strptime(today, "%Y-%m-%d")
past_date = today - timedelta(days=daysago)
datetime_object = past_date 

# COMMAND ----------

past_date.strftime('%Y-%m-%d')

# COMMAND ----------

itNotExist = False 
try:
  listed = dbutils.fs.ls(base_path + "stat/input/proc_date/"+today.strftime('%Y-%m-%d')+"/ipCountryCode="+ipCountryCode+"/")
except:
  itNotExist = True

if itNotExist:
  aggregate_data_full_ck= spark.read.format("parquet").option("basePath",base_path)\
                          .load(base_path+"input/cookie/aggregated_data/ipCountryCode="+ipCountryCode)\
                          .where("date>='"+past_date.strftime('%Y-%m-%d')+"' and date<='"+today.strftime('%Y-%m-%d')+"'")

# COMMAND ----------

if itNotExist:
  input_data_id5 = aggregate_data_full_ck.where("id5UID is not null")\
                .groupBy("ipCountryCode","id5UID").agg(f.sum("num_events_per_day").alias("tot_events"),f.countDistinct("ip").alias("count_ip") )\
                .groupBy("ipCountryCode")\
                .agg(f.countDistinct("id5UID").alias("count"),\
                 f.sum("tot_events").alias("tot_events"),\
                 f.avg("tot_events").alias("avg_events"),\
                 f.max("tot_events").alias("max_events"),\
                 f.avg("count_ip").alias("avg_ip_seen"),\
                 f.max("count_ip").alias("max_ip_seen"))\
                 .withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d')))\
                 .withColumn("dev_type",f.lit("id5UID"))
  input_data_first = aggregate_data_full_ck.where("firstPartyUID is not null")\
                .groupBy("ipCountryCode","firstPartyUID").agg(f.sum("num_events_per_day").alias("tot_events"),f.countDistinct("ip").alias("count_ip") )\
                .groupBy("ipCountryCode")\
                .agg(f.countDistinct("firstPartyUID").alias("count"),\
                 f.sum("tot_events").alias("tot_events"),\
                 f.avg("tot_events").alias("avg_events"),\
                 f.max("tot_events").alias("max_events"),\
                 f.avg("count_ip").alias("avg_ip_seen"),\
                 f.max("count_ip").alias("max_ip_seen"))\
                 .withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d')))\
                 .withColumn("dev_type",f.lit("firstPartyUID"))
  input_data_mail = aggregate_data_full_ck.where("email is not null")\
                .groupBy("ipCountryCode","email").agg(f.sum("num_events_per_day").alias("tot_events"),f.countDistinct("ip").alias("count_ip") )\
                .groupBy("ipCountryCode")\
                .agg(f.countDistinct("email").alias("count"),\
                 f.sum("tot_events").alias("tot_events"),\
                 f.avg("tot_events").alias("avg_events"),\
                 f.max("tot_events").alias("max_events"),\
                 f.avg("count_ip").alias("avg_ip_seen"),\
                 f.max("count_ip").alias("max_ip_seen"))\
                 .withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d')))\
                 .withColumn("dev_type",f.lit("email"))
  input_data_gaid = aggregate_data_full_ck.where("gaid is not null")\
                .groupBy("ipCountryCode","gaid").agg(f.sum("num_events_per_day").alias("tot_events"),f.countDistinct("ip").alias("count_ip") )\
                .groupBy("ipCountryCode")\
                .agg(f.countDistinct("gaid").alias("count"),\
                 f.sum("tot_events").alias("tot_events"),\
                 f.avg("tot_events").alias("avg_events"),\
                 f.max("tot_events").alias("max_events"),\
                 f.avg("count_ip").alias("avg_ip_seen"),\
                 f.max("count_ip").alias("max_ip_seen"))\
                 .withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d')))\
                 .withColumn("dev_type",f.lit("gaid"))
  input_data_idfa = aggregate_data_full_ck.where("idfa is not null")\
                .groupBy("ipCountryCode","idfa").agg(f.sum("num_events_per_day").alias("tot_events"),f.countDistinct("ip").alias("count_ip") )\
                .groupBy("ipCountryCode")\
                .agg(f.countDistinct("idfa").alias("count"),\
                 f.sum("tot_events").alias("tot_events"),\
                 f.avg("tot_events").alias("avg_events"),\
                 f.max("tot_events").alias("max_events"),\
                 f.avg("count_ip").alias("avg_ip_seen"),\
                 f.max("count_ip").alias("max_ip_seen"))\
                 .withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d')))\
                 .withColumn("dev_type",f.lit("idfa"))

# COMMAND ----------



# COMMAND ----------

if itNotExist:
  MobileExist = True
  try:
    df = dbutils.fs.ls(base_path + "input/mobile/aggregated_data/ipCountryCode="+ipCountryCode+"/")
  except:
    MobileExist = False
  if MobileExist:
    aggregate_data_full_mobile= spark.read.format("parquet").option("basePath",base_path)\
                            .load(base_path+"input/mobile/aggregated_data/ipCountryCode="+ipCountryCode)\
                            .where("date>='"+past_date.strftime('%Y-%m-%d')+"' and date<='"+today.strftime('%Y-%m-%d')+"'")\
                            .withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d')))

# COMMAND ----------

if itNotExist:
  if MobileExist:
    input_data_mobile = aggregate_data_full_mobile.where("ifa is not null")\
                .groupBy("ipCountryCode","ifa","platform").agg(f.sum("num_events_per_day").alias("tot_events"),f.countDistinct("ip").alias("count_ip") )\
                .groupBy("ipCountryCode","platform")\
                .agg(f.countDistinct("ifa").alias("count"),\
                 f.sum("tot_events").alias("tot_events"),\
                 f.avg("tot_events").alias("avg_events"),\
                 f.max("tot_events").alias("max_events"),\
                 f.avg("count_ip").alias("avg_ip_seen"),\
                 f.max("count_ip").alias("max_ip_seen"))\
                 .withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d')))\
                 .withColumn("dev_type",f.lower(f.col("platform")))\
                 .select("ipCountryCode","count","tot_events","avg_events","max_events","avg_ip_seen","max_ip_seen","proc_date","dev_type")

# COMMAND ----------

if itNotExist:
  if MobileExist:
    ids_data = input_data_id5.union(input_data_first).union(input_data_idfa).union(input_data_gaid).union(input_data_mail).union(input_data_mobile)
  else:
    ids_data = input_data_id5.union(input_data_first).union(input_data_idfa).union(input_data_gaid).union(input_data_mail)

# COMMAND ----------

#.option("replaceWhere", "ipCountryCode = '"+ipCountryCode+"'and proc_date="+today.strftime('%Y-%m-%d'))\
if itNotExist:
  ids_data.write.format("parquet").mode("append").partitionBy("proc_date","ipCountryCode")\
            .option("replaceWhere",  "proc_date="+today.strftime('%Y-%m-%d'))\
            .save(base_path + "stat/input")
  
ids_data = spark.read.format("parquet").option("basePath",base_path).load(base_path + "stat/input/proc_date="+today.strftime('%Y-%m-%d')).where("ipCountryCode='"+ipCountryCode+"'")

# COMMAND ----------

itNotExist = False 
try:
  listed = dbutils.fs.ls(base_path + "stat/effective_firstParty"+today.strftime('%Y-%m-%d')+"/ipCountryCode="+ipCountryCode+"/")
except:
  itNotExist = True

if itNotExist:
  firstParty = spark.read.format("parquet").option("basePath", base_path)\
                .load(base_path + "input/cookie/aggregated_data/ipCountryCode="+ipCountryCode)\
                .where("date>='"+past_date.strftime('%Y-%m-%d')+"' and date<='"+today.strftime('%Y-%m-%d')+"'")\
                .select("firstPartyUID","id5UID","ipCountryCode")\
                .where("firstPartyUID is not null")\
                .groupBy("firstPartyUID","id5UID","ipCountryCode").count().drop("count")\
                .withColumn("id5id_for_first",f.count(f.lit(1)).over(Window.partitionBy("firstPartyUID")))\
                .where("id5id_for_first > 1")\
                .withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d')))\
                .groupBy("proc_date","ipCountryCode").agg(f.countDistinct("firstPartyUID").alias("count"))\
                .withColumn("dev_type",f.lit("firstPartyUID"))
  
  firstParty.write.format("parquet").mode("append").partitionBy("proc_date","ipCountryCode")\
            .option("replaceWhere", "proc_date="+today.strftime('%Y-%m-%d'))\
            .save(base_path + "stat/effective_firstParty")
  
firstParty = spark.read.format("parquet").option("basePath",base_path).load(base_path + "stat/effective_firstParty/proc_date="+today.strftime('%Y-%m-%d')+"/ipCountryCode="+ipCountryCode)

# COMMAND ----------

graphsall = spark.read.format("parquet").option("basePath",base_path)\
           .load(base_path+"output/weekwindow/persisted_graph/individual/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))

# COMMAND ----------

idstorate = ids_data.select("count","dev_type","proc_date","ipCountryCode").where("dev_type!='firstPartyUID'").union(firstParty.distinct())

# COMMAND ----------

supervertex_tab = graphsall\
  .withColumn("couples_per_super",f.count(f.lit(1)).over(Window.partitionBy("ipCountryCode","proc_date","supervertex")))\
  .groupBy("proc_date","ipCountryCode")\
  .agg(f.countDistinct("supervertex").alias("n_supervertrex")\
     ,f.max("couples_per_super").alias("max_couples"),f.avg("couples_per_super").alias("avg_couples"))

# COMMAND ----------

itNotExist = False 
try:
  listed = dbutils.fs.ls(base_path + "stat/supervetex_stat"+today.strftime('%Y-%m-%d')+"/ipCountryCode="+ipCountryCode+"/")
except:
  itNotExist = True

if itNotExist:
  supervertex_tab.write.format("parquet").mode("append").partitionBy("proc_date","ipCountryCode")\
            .option("replaceWhere", "proc_date="+today.strftime('%Y-%m-%d'))\
            .save(base_path + "stat/supervetex_stat")

# COMMAND ----------

ids_graph = graphsall.select("dev1","type_dev1","ipCountryCode","proc_date")\
            .union(graphsall.select("dev2","type_dev2","ipCountryCode","proc_date"))\
            .groupBy("proc_date","ipCountryCode","type_dev1").agg(f.countDistinct("dev1").alias("graph_count"))\
            .withColumnRenamed("type_dev1","dev_type")

# COMMAND ----------

rates = ids_graph.join(idstorate,["proc_date","ipCountryCode","dev_type"])\
        .withColumn("rate",f.bround(f.col("graph_count")/f.col("count"),3))

# COMMAND ----------

itNotExist = False 
try:
  listed = dbutils.fs.ls(base_path + "stat/graphs_counts_and_completeness"+today.strftime('%Y-%m-%d')+"/ipCountryCode="+ipCountryCode+"/")
except:
  itNotExist = True

if itNotExist:
  rates.write.format("parquet").mode("append").partitionBy("proc_date","ipCountryCode")\
            .option("replaceWhere", "proc_date="+today.strftime('%Y-%m-%d'))\
            .save(base_path + "stat/graphs_counts_and_completeness")
  
rates = spark.read.format("parquet").option("basePath",base_path)\
       .load(base_path+"stat/graphs_counts_and_completeness/proc_date="+today.strftime('%Y-%m-%d')+"/ipCountryCode="+ipCountryCode)

# COMMAND ----------

itNotExist = False 
try:
  listed = dbutils.fs.ls(base_path + "stat/graph_levels"+today.strftime('%Y-%m-%d')+"/ipCountryCode="+ipCountryCode+"/")
except:
  itNotExist = True

if itNotExist:
  ids_graph_level_low = graphsall\
                      .where("level ='low' or level ='medium' or level ='high'")\
                      .select("dev1","type_dev1","ipCountryCode","proc_date","level")\
                      .union(graphsall\
                      .where("level ='low' or level ='medium' or level ='high'")\
                      .select("dev2","type_dev2","ipCountryCode","proc_date","level"))\
                      .groupBy("proc_date","ipCountryCode","type_dev1")\
                      .agg(f.countDistinct("dev1").alias("count"))\
                      .withColumnRenamed("type_dev1","dev_type")\
                      .withColumn("cumulative_at",f.lit("low"))
  ids_graph_level_medium = graphsall\
                      .where("level ='medium' or level ='high'")\
                      .select("dev1","type_dev1","ipCountryCode","proc_date","level")\
                      .union(graphsall\
                      .where("level ='medium' or level ='high'")\
                      .select("dev2","type_dev2","ipCountryCode","proc_date","level"))\
                      .groupBy("proc_date","ipCountryCode","type_dev1")\
                      .agg(f.countDistinct("dev1").alias("count"))\
                      .withColumnRenamed("type_dev1","dev_type")\
                      .withColumn("cumulative_at",f.lit("medium"))
  ids_graph_level_high = graphsall\
                      .where("level ='high'")\
                      .select("dev1","type_dev1","ipCountryCode","proc_date","level")\
                      .union(graphsall\
                      .where("level ='high'")\
                      .select("dev2","type_dev2","ipCountryCode","proc_date","level"))\
                      .groupBy("proc_date","ipCountryCode","type_dev1")\
                      .agg(f.countDistinct("dev1").alias("count"))\
                      .withColumnRenamed("type_dev1","dev_type")\
                      .withColumn("cumulative_at",f.lit("high"))

  ids_graph_level = ids_graph_level_high.union(ids_graph_level_medium).union(ids_graph_level_low)
  ids_graph_level.write.format("parquet").mode("append").partitionBy("proc_date","ipCountryCode")\
            .option("replaceWhere", "proc_date="+today.strftime('%Y-%m-%d'))\
            .save(base_path + "stat/graph_levels")


# COMMAND ----------

itNotExist = False 
try:
  listed = dbutils.fs.ls(base_path + "stat/ip_stat_cookie"+today.strftime('%Y-%m-%d')+"/ipCountryCode="+ipCountryCode+"/")
except:
  itNotExist = True

if itNotExist:
  ip_cookie= spark.read.format("parquet").option("basePath",base_path).load(base_path+"input/cookie/aggregated_data/ipCountryCode="+ipCountryCode)\
                        .where("date>='"+past_date.strftime('%Y-%m-%d')+"' and date<='"+today.strftime('%Y-%m-%d')+"'")\
                        .groupBy("ipCountryCode","ip")\
                        .agg(f.sum("num_events_per_day").alias("tot_events"),f.countDistinct("id5UID").alias("count_id5"))\
                        .groupBy("ipCountryCode")\
                        .agg(f.countDistinct("ip").alias("count"),\
                        f.avg("tot_events").alias("avg_events"),\
                        f.max("tot_events").alias("max_events"),\
                        f.avg("count_id5").alias("avg_id5_seen"),\
                        f.max("count_id5").alias("max_id5_seen"))\
                        .withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d')))
  
  ip_cookie.write.format("parquet").mode("append").partitionBy("proc_date","ipCountryCode")\
            .option("replaceWhere", "proc_date="+today.strftime('%Y-%m-%d'))\
            .save(base_path + "stat/ip_stat_cookie")
  
ip_cookie = spark.read.format("parquet").option("basePath",base_path).load(base_path+"stat/ip_stat_cookie/proc_date="+today.strftime('%Y-%m-%d')+"/ipCountryCode="+ipCountryCode)

# COMMAND ----------

itNotExist = False 
try:
  listed = dbutils.fs.ls(base_path +  "stat/ip_rates"+today.strftime('%Y-%m-%d')+"/ipCountryCode="+ipCountryCode+"/")
except:
  itNotExist = True

if itNotExist:
  flat_ip = spark.read.format("parquet").option("basePath", base_path).load(base_path + \
                    "processing/cookie/flat_ip_withcount/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))\
                    .select("ip", "num_id2_per_day","ipCountryCode","proc_date")\
                    .groupBy("ip","ipCountryCode","proc_date").agg(f.max("num_id2_per_day").alias("max_num_id2_per_day"))\
                    .where("max_num_id2_per_day <= " +max_dim_ip)\
                    .select("ip","ipCountryCode","proc_date")\
                    .distinct()
  ratesIp =flat_ip\
        .groupBy("proc_date","ipCountryCode")\
        .agg(f.countDistinct("ip").alias("count"))\
        .join(ip_cookie.select("proc_date","ipCountryCode","count")\
        .withColumnRenamed("count","allIps"),["proc_date","ipCountryCode"])\
        .withColumn("rate",f.bround(f.col("count")/f.col("allIps"),3))
  
  ratesIp.write.format("parquet").mode("append").partitionBy("proc_date","ipCountryCode")\
            .option("replaceWhere", "proc_date="+today.strftime('%Y-%m-%d'))\
            .save(base_path + "stat/ip_rates")

# COMMAND ----------

if MobileExist:
  itNotExist = False 
  try:
    listed = dbutils.fs.ls(base_path + "stat/ip_stat_mobile"+today.strftime('%Y-%m-%d')+"/ipCountryCode="+ipCountryCode+"/")
  except:
    itNotExist = True

  if itNotExist:
    ip_mobile= spark.read.format("parquet").option("basePath",base_path).load(base_path+"input/mobile/")\
                        .where("date>='"+past_date.strftime('%Y-%m-%d')+"' and date<='"+today.strftime('%Y-%m-%d')+"'")\
                        .groupBy("ipCountryCode","ip")\
                        .agg(f.sum("num_events_per_day").alias("tot_events"),f.countDistinct("ifa").alias("count_ifa"))\
                        .groupBy("ipCountryCode")\
                        .agg(f.countDistinct("ip").alias("count"),\
                        f.avg("tot_events").alias("avg_events"),\
                        f.max("tot_events").alias("max_events"),\
                        f.avg("count_ifa").alias("avg_ifa_seen"),\
                        f.max("count_ifa").alias("max_ifa_seen"))\
                        .withColumn("proc_date",f.lit(today.strftime('%Y-%m-%d')))
    ip_mobile.write.format("parquet").mode("append").partitionBy("proc_date","ipCountryCode")\
            .option("replaceWhere", "proc_date="+today.strftime('%Y-%m-%d'))\
            .save(base_path + "stat/ip_stat_mobile")
  ip_mobile =spark.read.format("parquet").option("basePath",base_path).load(base_path+"stat/ip_stat_mobile/proc_date="+today.strftime('%Y-%m-%d'))

# COMMAND ----------

if MobileExist:
  ips_for_ck = spark.read.format("parquet").option("basePath", base_path).load(base_path+\
                          "processing/cookie/flat_ip_withcount/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))\
                          .join(spark.read.format("parquet").option("basePath", base_path).load(base_path + \
                          "processing/mobile/flat_ip_withcount/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))\
                          .select("ip","ipCountryCode"),["ip","ipCountryCode"])

  flat_ip = spark.read.format("parquet").option("basePath", base_path).load(base_path +\
                    "processing/mobile/flat_ip_withcount/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))\
                    .select("ip","ipCountryCode","date","num_id2_per_day","proc_date")\
                    .union(ips_for_ck)\
                    .groupBy("ip","ipCountryCode","proc_date").agg(f.max("num_id2_per_day").alias("max_num_id2_per_day"))\
                    .where("max_num_id2_per_day <=" +max_dim_ip)\
                    .select("ip","ipCountryCode","proc_date")\
                    .distinct()


# COMMAND ----------

if MobileExist:
  itNotExist = False 
  try:
    listed = dbutils.fs.ls(base_path + "stat/ip_mobile_rates"+today.strftime('%Y-%m-%d')+"/ipCountryCode="+ipCountryCode+"/")
  except:
    itNotExist = True

  if itNotExist:
    ratesIp =flat_ip\
        .groupBy("proc_date","ipCountryCode")\
        .agg(f.countDistinct("ip").alias("count"))\
        .join(ip_mobile.select("proc_date","ipCountryCode","count")\
        .withColumnRenamed("count","allIps"),["proc_date","ipCountryCode"])\
        .withColumn("rate_used",f.bround(f.col("count")/f.col("allIps"),3))\
        .join(ips_for_ck.groupBy("proc_date","ipCountryCode").agg(f.countDistinct("ip").alias("common_ip")),["proc_date","ipCountryCode"])\
        .withColumn("rate_common",f.bround(f.col("common_ip")/f.col("allIps"),3))
    ratesIp.write.format("parquet").mode("append").partitionBy("proc_date","ipCountryCode")\
            .option("replaceWhere", "proc_date="+today.strftime('%Y-%m-%d'))\
            .save(base_path + "stat/ip_mobile_rates")

# COMMAND ----------

itNotExist = False 
try:
  listed = dbutils.fs.ls(base_path + "stat/household_stats"+today.strftime('%Y-%m-%d')+"/ipCountryCode="+ipCountryCode+"/")
except:
  itNotExist = True

if itNotExist:
  household_stat = spark.read.format("parquet").option("basePath", base_path)\
                     .load(base_path + "output/weekwindow/graph/household/ipCountryCode="+ipCountryCode+"/proc_date="+today.strftime('%Y-%m-%d'))\
                     .groupBy("proc_date","ipCountryCode").agg(f.countDistinct("HouseHold").alias("HouseHold_count"))
  household_stat.write.format("parquet").mode("append").partitionBy("proc_date","ipCountryCode")\
            .option("replaceWhere", "proc_date="+today.strftime('%Y-%m-%d'))\
            .save(base_path + "stat/household_stats")

# COMMAND ----------


