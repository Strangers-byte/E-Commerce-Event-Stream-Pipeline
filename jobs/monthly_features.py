
import sys, os

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
"""
    Create a Dataframe 
"""
spart_table = os.getenv("silver_table")

df = spark.table(spart_table)
df.head()
from pyspark.sql.functions import col

df_oct = df.filter(
    (col("event_date") >= "2019-10-01") & (col("event_date") < "2019-11-01")
)
df_nov = df.filter(
    (col("event_date") >= "2019-11-01") & (col("event_date") < "2019-12-01")
)
"""
    Creating User-Level features from October data
"""
from pyspark.sql.functions import count, countDistinct, sum as _sum, avg, datediff, lit, dayofweek, hour, when, max as _max

ref_date = "2019-10-31"

oct_features = df_oct.groupBy("user_id").agg(
    count("*").alias("total_events"),
    countDistinct("user_session").alias("total_sessions"),
    countDistinct("event_date").alias("active_days"),
    _sum(when(col("event_type") == "view", 1).otherwise(0)).alias("total_views"),
    _sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("total_carts"),
    _sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases"),
    _sum(when(col("event_type") == "purchase", col("price")).otherwise(0)).alias("total_revenue"),
    avg(col("price")).alias("avg_price"),
    countDistinct("category_type").alias("distinct_categories"),
    countDistinct("brand").alias("distinct_brands"),
    # Recency: days from last event to ref_date
    datediff(lit(ref_date), _max("event_date")).alias("recency_days"),
    # Session depth
    (count("*") / countDistinct("user_session")).alias("session_avg_events"),
    # Weekend ratio
    (_sum(when(dayofweek("event_date").isin(1,7), 1).otherwise(0)) / count("*")).alias("weekend_ratio"),
    # Morning ratio (6-11)
    (_sum(when(hour("event_time").between(6,11), 1).otherwise(0)) / count("*")).alias("morning_ratio"),
    # Category diversity
    (countDistinct("category_type") / countDistinct("event_date")).alias("category_diversity")
)

"""
    Creating label from November data
"""

purchasers_nov = df_nov.filter(
    col("event_type") == "purchase").select("user_id").distinct().withColumn("label", lit(1)
)
features = oct_features.join(purchasers_nov, "user_id", "left").fillna({"label": 0})

user_metrics_path = os.getenv("user_behavior")

features.write.mode("overwrite").parquet(user_metrics_path)
job.commit()