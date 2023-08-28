# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Structurize Youtube Data
# MAGIC This notebook is to load data from bronze layer and create tables based on them, which will be places in silver layer.

# COMMAND ----------

# MAGIC %md
# MAGIC # Mounting container

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
# MAGIC # Definig Functions

# COMMAND ----------

import pyspark.sql.functions as f
import datetime

def flatten_search_data(full_path, date):

    raw_df = spark.read.json(full_path, multiLine=True)
    search_flat = (raw_df
                   # flatten dataframe
                   .withColumn('search_results', f.explode('items'))
                   .select(f.col("search_results.*"))
                   .select(f.col("snippet.*"))
                   .withColumn("thumbnailUrl", f.col("thumbnails.default.url"))
                   .drop("thumbnails", "title")
                   # handle data types
                   .withColumn("dateRead", f.to_date(f.lit(date), "yyyy-MM-dd"))
                   .withColumn("publishTime", f.to_timestamp(f.col("publishTime")))
                   .withColumn("publishedAt", f.to_timestamp(f.col("publishedAt")))
                )

    return search_flat

def flatten_channels_data(full_path, date):

    raw_df = spark.read.json(full_path, multiLine=True)
    channels_flat = (raw_df
                   # flatten dataframe
                   .withColumn('channels_results', f.explode('items'))
                   .select(f.col("channels_results.*"))
                   .withColumnRenamed("id", "channelId")
                   .select(f.col("channelId"), f.col("statistics.*"), f.col("topicDetails.*"), f.col("contentDetails.*"))
                   # handle data types
                   .withColumn("dateRead", f.to_date(f.lit(date), "yyyy-MM-dd"))
                   .withColumn("subscriberCount", f.col("subscriberCount").cast("int"))
                   .withColumn("videoCount", f.col("videoCount").cast("int"))
                   .withColumn("viewCount", f.col("viewCount").cast("int"))
                )

    return channels_flat

def flatten_videos_data(full_path, date):

    raw_df = spark.read.json(full_path, multiLine=True)
    videos_flat = (raw_df
                   # flatten dataframe
                   .withColumn('snippet', f.col("items").getField("snippet"))
                   .withColumn("snippet", f.explode("snippet"))
                   .select(f.col("snippet.*"))
                   .withColumn('description', f.col("localized").getField("description"))
                   .withColumn("thumbnailUrl", f.col("thumbnails.medium.url"))
                   .drop("items", "localized", "thumbnails")
                   # handle data types
                   .withColumn("dateRead", f.lit(date))
                   .withColumn("categoryId", f.col("categoryId").cast("int"))
                   .withColumn("publishedAt", f.to_timestamp(f.col("publishedAt")))
                )

    return videos_flat

# COMMAND ----------

# MAGIC %md
# MAGIC # Structurizing bronze data and writing to silver layer

# COMMAND ----------

files = dbutils.fs.ls("/mnt/mol-data/yt-analysis-data/bronze/")

# Reading current silver tables
search_df = spark.read.parquet("/mnt/mol-data/yt-analysis-data/silver/search.parquet")
channels_df = spark.read.parquet("/mnt/mol-data/yt-analysis-data/silver/channels.parquet")
videos_df = spark.read.parquet("/mnt/mol-data/yt-analysis-data/silver/videos.parquet")

# Reading each new bronze file and appending to silver table
for file in files:
    full_path = file.path
    date = '-'.join(file.name.split('.')[0].split('-')[-3:])
    data_type = file.name.split('-')[0]

    if data_type == "search":
        read_search = flatten_search_data(full_path, date)
        search_df = search_df.unionByName(read_search)
    elif data_type == "channels":
        read_channels = flatten_channels_data(full_path, date)
        channels_df = channels_df.unionByName(read_channels)
    elif data_type == "videos":
        read_videos = flatten_videos_data(full_path, date)
        videos_df = videos_df.unionByName(read_videos)

# Deduping updated silver tables
search_df_dedup = search_df.dropDuplicates()
channels_df_dedup = channels_df.dropDuplicates()
videos_df_dedup = videos_df.dropDuplicates()

# Overwrinting existing silver tables
search_df_dedup.write.mode("overwrite").parquet("/mnt/mol-data/yt-analysis-data/silver/search.parquet")
channels_df_dedup.write.mode("overwrite").parquet("/mnt/mol-data/yt-analysis-data/silver/channels.parquet")
videos_df_dedup.write.mode("overwrite").parquet("/mnt/mol-data/yt-analysis-data/silver/videos.parquet")

# COMMAND ----------

flatten_search_data("dbfs:/mnt/mol-data/yt-analysis-data/bronze/search-data-engineering-2023-08-20.json", "2023-08-20").show()

# COMMAND ----------

videos_df = spark.read.parquet("/mnt/mol-data/yt-analysis-data/silver/videos.parquet")
videos_df.printSchema()
