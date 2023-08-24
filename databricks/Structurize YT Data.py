# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Structurize Youtube Data
# MAGIC This notebook is to load data from bronze layer and create tables based on them, which will be places in silver layer.

# COMMAND ----------

# MAGIC %md
# MAGIC # Loading data

# COMMAND ----------

conn_key = dbutils.secrets.get(scope="mol-secrets", key="storage_key")
storage_name = 'azuredatasandboxadls'
container_name = "mol"

mountPoints = [elem.mountPoint for elem in dbutils.fs.mounts()]

if '/mnt/mol-data' not in mountPoints:
    dbutils.fs.mount(
        source = f"wasbs://{container_name}@{storage_name}.blob.core.windows.net",
        mount_point = "/mnt/mol-data",
        extra_configs = {f"fs.azure.account.key.{storage_name}.blob.core.windows.net": conn_key})
else:
    print("Mount Point already created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading data

# COMMAND ----------

import pyspark.sql.functions as f
import datetime


def flatten_search_data(full_path, date):

    raw_df = spark.read.json(full_path, multiLine=True)
    search_flat = (raw_df
                   .withColumn('search_results', f.explode('items'))
                   .select(f.col("search_results.*"))
                   .select(f.col("snippet.*"))
                   .withColumn("thumbnailUrl", f.col("thumbnails.default.url"))
                   .drop("thumbnails", "title")
                   .withColumn("date", f.lit(date))
                   .repartition(4)
                )

    return search_flat

def flatten_channels_data(full_path, date):

    raw_df = spark.read.json(full_path, multiLine=True)
    channels_flat = (raw_df
                   .withColumn('channels_results', f.explode('items'))
                   .select(f.col("channels_results.*"))
                   .withColumnRenamed("id", "channelId")
                   .select(f.col("channelId"), f.col("statistics.*"), f.col("topicDetails.*"), f.col("contentDetails.*"))
                   .repartition(4)
                )

    return channels_flat

def flatten_videos_data(full_path, date):

    raw_df = spark.read.json(full_path, multiLine=True)
    videos_flat = (raw_df
                   .withColumn('snippet', f.col("items").getField("snippet"))
                   .withColumn("snippet", f.explode("snippet"))
                   .select(f.col("snippet.*"))
                   .withColumn('description', f.col("localized").getField("description"))
                   .withColumn("thumbnailUrl", f.col("thumbnails.medium.url"))
                   .drop("items", "localized", "thumbnails")
                   .repartition(4)
                )

    return videos_flat


files = dbutils.fs.ls("/mnt/mol-data/yt-analysis-data/bronze/")

for file in files:
    full_path = file.path
    date = '-'.join(file.name.split('.')[0].split('-')[-3:])
    data_type = file.name.split('-')[0]

    if data_type == "search":
        flatten_search_data(full_path, date)
    elif data_type == "channels":
        flatten_channels_data(full_path, date)
    elif data_type == "videos":
        flatten_videos_data(full_path, date)
    
    

# COMMAND ----------

# MAGIC %fs ls mnt/mol-data/yt-analysis-data/bronze

# COMMAND ----------

raw_df = spark.read.json("dbfs:/mnt/mol-data/yt-analysis-data/bronze/videos-data-engineering-2023-08-20.json", multiLine=True)
display(raw_df.)

# COMMAND ----------

spark.conf.get("spark.executor.memory")


# COMMAND ----------

partitions = 8

my_json = spark.read.json("dbfs:/mnt/mol-data/yt-analysis-data/bronze/videos-data-engineering-2023-08-20.json", multiLine=True)

my_json.show()


# COMMAND ----------

full_path = "dbfs:/mnt/mol-data/yt-analysis-data/bronze/videos-data-engineering-2023-08-20.json"
raw_df = spark.read.json(full_path, multiLine=True)
videos_flat = (raw_df
                .select("items")
                .withColumn('snippet', f.col("items").getField("snippet"))
                .drop("items")
                .repartition(4)
                .withColumn("snippet", f.explode("snippet"))
                .select(f.col("snippet.*"))
                .withColumn('description', f.col("localized").getField("description"))
                .withColumn("thumbnailUrl", f.col("thumbnails.medium.url"))
                .drop("localized", "thumbnails")
            )

videos_flat.rdd.getNumPartitions()

# COMMAND ----------

full_path = "dbfs:/mnt/mol-data/yt-analysis-data/bronze/channels-data-engineering-2023-08-20.json"

raw_df = spark.read.json(full_path, multiLine=True)
channels_flat = (raw_df
                .withColumn('channels_results', f.explode('items'))
                .select(f.col("channels_results.*"))
                .withColumnRenamed("id", "channelId")
                .select(f.col("channelId"), f.col("statistics.*"), f.col("topicDetails.*"), f.col("contentDetails.*"))
                .repartition(4)
            )
channels_flat.rdd.getNumPartitions()
