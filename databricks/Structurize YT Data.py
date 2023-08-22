# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Structurize Youtube Data
# MAGIC This notebook is to load data from bronze layer and create tables based on them, which will be places in silver layer.

# COMMAND ----------

# MAGIC %md
# MAGIC # Loading data

# COMMAND ----------

conn_str = dbutils.secrets.get(scope="mol-secrets", key="storage_key")
storage_name = 'azuredatasandboxadls'
container_name = "mol"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mounting my container

# COMMAND ----------

dbutils.fs.mount(
    source = f"wasbs://{container_name}@{storage_name}.blob.core.windows.net",
    mount_point = "/mnt/mol-data",
    extra_configs = {f"fs.azure.account.key.{storage_name}.blob.core.windows.net": conn_str})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading data

# COMMAND ----------

import pyspark.sql.functions as f
import datetime


def flatten_search_data(full_path, date):

    raw_df = spark.read.json(full_path, multiLine=True)
    search_results = raw_df.withColumn('search_results', f.explode('items')).select('search_results')
    search_snippet = search_results.select(f.col("search_results.*")).select('snippet')
    search_df = search_snippet.select(f.col("snippet.*"))
    search_df_with_thumbnail = search_df.withColumn("thumbnailUrl", f.col("thumbnails.default.url")).drop("thumbnails", "title")
    search_flat = search_df_with_thumbnail.withColumn("date", f.lit(date))

    return search_flat

def flatten_channels_data(full_path, date):

    raw_df = spark.read.json(full_path, multiLine=True)
    search_results = raw_df.withColumn('search_results', f.explode('items')).select('search_results')
    search_snippet = search_results.select(f.col("search_results.*")).select('snippet')
    search_df = search_snippet.select(f.col("snippet.*"))
    search_df_with_thumbnail = search_df.withColumn("thumbnailUrl", f.col("thumbnails.default.url")).drop("thumbnails", "title")
    search_flat = search_df_with_thumbnail.withColumn("date", f.lit(date))

    return search_flat


files = dbutils.fs.ls("/mnt/mol-data/yt-analysis-data/bronze/")

for file in files:
    full_path = file.path
    date = '-'.join(file.name.split('.')[0].split('-')[-3:])
    data_type = file.name.split('-')[0]

    if data_type == "search":
        flatten_search_data(full_path, date)
    elif data_type == "channels":
        pass
    elif data_type == "videos":
        pass
    
    

# COMMAND ----------

name = search_data.withColumn('name', f.explode('items')).select('name')

# COMMAND ----------

search_data.show()

# COMMAND ----------

search_df1 = name.select(f.col("name.*"))
search_df2 = search_df1.select(f.col("etag"), f.col("id.*"), f.col("snippet.*"))
display(search_df2)
