# Databricks notebook source
# MAGIC %md
# MAGIC # Aggregates

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading Data

# COMMAND ----------

search_df = spark.read.parquet("/mnt/mol-data/yt-analysis-data/silver/search.parquet")
channels_df = spark.read.parquet("/mnt/mol-data/yt-analysis-data/silver/channels.parquet")
videos_df = spark.read.parquet("/mnt/mol-data/yt-analysis-data/silver/videos.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC # Most popular channels
# MAGIC Here I will try to aggregate columns that will enable identifying most popular channels.
# MAGIC Factors that are important to identify such channels are subscribers_count and view_count. It will be also good to see the average views on a video.

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.window import Window

w = Window.partitionBy('channelId').orderBy(f.desc('dateRead'))

pop_channels = (channels_df
                # join with search to get channelTitles, but don't need dateRead of search_df 
                # and it's name is same as in channels_df so I drop it as well as I drop duplicates
                .join(search_df.drop('dateRead').dropDuplicates(), on="channelId")
                .drop('hiddenSubscriberCount', 'relatedPlaylists', 'liveBroadcastContent')
                .dropDuplicates()
                # choose only most recent rows
                .withColumn('Rank', f.dense_rank().over(w))
                .filter("Rank = 1")
                .drop("Rank")
                .withColumn("viewCountPerVideo", f.round(f.col("viewCount")/f.col("videoCount")))
                .select("channelTitle", "channelId", "videoCount", "viewCount", "viewCountPerVideo", "subscriberCount", "description", "thumbnailUrl")
                )

pop_channels.printSchema()
display(pop_channels)

pop_channels = spark.write.csv("/mnt/mol-data/yt-analysis-data/gold/videos.parquet"")

# COMMAND ----------

# MAGIC %md
# MAGIC # Growing channels
# MAGIC This aggregate table will store data about daily increase of views per channels.

# COMMAND ----------

# MAGIC %md
# MAGIC # Most frequent uploads
# MAGIC This aggregate table will store data about frequency of videos uploads per channel
