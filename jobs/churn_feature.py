
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
    Creating a dataframe
"""

spart_table = os.getenv("silver_table")

df= spark.table(spart_table)


from pyspark.sql.functions import col

df_oct = df.filter(
    (col("event_date") >= "2019-10-01") & (col("event_date") < '2019-11-01')
)


df_nov = df.filter(
    (col("event_date") >= "2019-11-01") & (col("event_date") < '2019-12-01')
)


from pyspark.sql.functions import col, count, countDistinct, sum as _sum, avg, datediff, lit, dayofweek, hour, when, max as _max

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
    datediff(lit(ref_date), _max("event_date")).alias("recency_days"),
    (count("*") / countDistinct("user_session")).alias("session_avg_events"),
    (_sum(when(dayofweek("event_date").isin(1,7), 1).otherwise(0)) / count("*")).alias("weekend_ratio"),
    (_sum(when(hour("event_time").between(6,11), 1).otherwise(0)) / count("*")).alias("morning_ratio"),
    (countDistinct("category_type") / countDistinct("event_date")).alias("category_diversity")
)


active_nov = df_nov.select("user_id").distinct().withColumn("label", lit(0))


features = oct_features.join(active_nov, "user_id", "left").fillna({"label": 1})

churn_path = os.getenv("user_churn")

features.write.mode("overwrite").parquet(churn_path)
job.commit()