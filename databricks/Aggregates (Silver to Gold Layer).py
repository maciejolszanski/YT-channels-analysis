# Databricks notebook source
# MAGIC %md
# MAGIC # Aggregates

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading Data

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.window import Window

search_df = spark.read.parquet("/mnt/mol-data/yt-analysis-data/silver/search.parquet")
channels_df = spark.read.parquet("/mnt/mol-data/yt-analysis-data/silver/channels.parquet")
videos_df = spark.read.parquet("/mnt/mol-data/yt-analysis-data/silver/videos.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC # Most popular channels
# MAGIC Here I will try to aggregate columns that will enable identifying most popular channels.
# MAGIC Factors that are important to identify such channels are subscribers_count and view_count. It will be also good to see the average views on a video.

# COMMAND ----------

w = Window.partitionBy('channelId').orderBy(f.desc('dateRead'))

pop_channels = (channels_df
                # join with search to get channelTitles, but don't need dateRead of search_df 
                # and it's name is same as in channels_df so I drop it as well as I drop duplicates
                .join(search_df.drop('dateRead').dropDuplicates(), on="channelId")
                .drop('hiddenSubscriberCount', 'relatedPlaylists', 'liveBroadcastContent')
                .dropDuplicates()
                # choose only most recent rows
                .withColumn('rank', f.dense_rank().over(w))
                .filter("rank = 1")
                # get columns I want
                .drop("rank")
                .withColumn("viewCountPerVideo", f.round(f.col("viewCount")/f.col("videoCount")))
                .select("channelTitle", "channelId", "videoCount", "viewCount", "viewCountPerVideo", "subscriberCount", "description", "thumbnailUrl")
)

pop_channels.printSchema()
display(pop_channels)

pop_channels.write.mode("overwrite").csv("/mnt/mol-data/yt-analysis-data/gold/pop_channels.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC # Growing channels
# MAGIC This aggregate table will store data about daily increase of views per channels.

# COMMAND ----------

w = Window.partitionBy('channelId').orderBy(f.desc('dateRead'))

growing_channels = (channels_df
                    # join with search to get channelTitles, but don't need dateRead of search_df 
                    # and it's name is same as in channels_df so I drop it as well as I drop duplicates
                    .join(search_df.drop('dateRead').dropDuplicates(), on="channelId")
                    .drop('hiddenSubscriberCount', 'relatedPlaylists', 'liveBroadcastContent')
                    .dropDuplicates()
                    # calculate daily growth of views
                    .withColumn('rank', f.dense_rank().over(w))
                    .withColumn("viewCountPrevious", f.lead(f.col("viewCount")).over(w))
                    .withColumn("dateReadPrevious", f.lead(f.col("dateRead")).over(w))
                    .withColumn("datesDiff", f.datediff(f.col("dateRead"), f.col("dateReadPrevious")))
                    .withColumn("dailyViewsGrow", (f.col("viewCount")-f.col("viewCountPrevious")) / f.col("datesDiff"))
                    # choose only most recent rows and colummns I want
                    .filter("rank = 1")
                    .select("channelTitle", "channelId", "dailyViewsGrow", "description", "thumbnailUrl")
                    .orderBy(f.desc(f.col("dailyViewsGrow")))
)

growing_channels.printSchema()
display(growing_channels)

growing_channels.write.mode("overwrite").csv("/mnt/mol-data/yt-analysis-data/gold/growing_channels.parquet")


# COMMAND ----------

# MAGIC %md
# MAGIC # Most frequent uploads
# MAGIC This aggregate table will store data about frequency of videos uploads per channel

# COMMAND ----------

df_dates = (videos_df
            .withColumn("yearPublished", f.year("publishedAt"))
            .withColumn("monthPublished", f.month("publishedAt"))
            .withColumn("weekPublished", f.weekofyear("publishedAt"))
)

uploads_month = (df_dates
                   .groupby("channelId", "channelTitle", "yearPublished", "monthPublished")
                   .agg(
                       f.count(f.col("title")).alias("videos")
                   )
)

uploads_week = (df_dates
                   .groupby("channelId", "channelTitle", "yearPublished", "weekPublished")
                   .agg(
                       f.count(f.col("title")).alias("videos")
                   )
)

uploads_month.write.mode("overwrite").csv("/mnt/mol-data/yt-analysis-data/gold/uploads_month.parquet")
uploads_week.write.mode("overwrite").csv("/mnt/mol-data/yt-analysis-data/gold/uploads_week.parquet")


# COMMAND ----------

# MAGIC %md
# MAGIC # Create tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS aggregates
# MAGIC LOCATION '/mnt/mol-data/yt-analysis-data/gold/database'

# COMMAND ----------

pop_channels.write.mode("overwrite").saveAsTable("aggregates.pop_channels")
growing_channels.write.mode("overwrite").saveAsTable("aggregates.growing_channels")
uploads_month.write.mode("overwrite").saveAsTable("aggregates.uploads_month")
uploads_week.write.mode("overwrite").saveAsTable("aggregates.uploads_week")
