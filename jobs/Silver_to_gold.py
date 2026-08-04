
import sys
import os
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

spark.conf.set("spark.sql.shuffle.partitions", "50")

spart_table = os.getenv("silver_table")
df = spark.table(spart_table)

"""
    Dim_date
"""

from pyspark.sql.functions import hour, year, month, day, min as _min, max as _max, date_format, col, dayofmonth, date_format, dayofweek, when

min_date = df.select(_min("event_date")).collect()[0][0]
max_date = df.select(_max("event_date")).collect()[0][0]

dim_date = spark.sql(f"""
    SELECT explode(sequence(to_date('{min_date}'), to_date('{max_date}'), interval 1 day)) AS date
""")

dim_date = dim_date.select(
    date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
    col("date").alias("event_date"),
    year("date").alias("year"),
    month("date").alias("month"),
    dayofmonth("date").alias("day"),
    date_format("date", "EEEE").alias("weekday"),
    when(dayofweek("date").isin(1,7), True).otherwise(False).alias("is_weekend")
)

dim_date_path = os.getenv("dim_date")

dim_date.coalesce(1).write.mode("overwrite").parquet(dim_date_path)
print("dim_date written")

"""
    Dim_categories
"""
from pyspark.sql.functions import split, col

category_path = os.getenv("dim_categories")

dim_categories = df.withColumn("category_level_1", split(col("category_code"), "\\.").getItem(0)) \
                 .withColumn("category_level_2", split(col("category_code"), "\\.").getItem(1)) \
                 .withColumn("category_level_3", split(col("category_code"), "\\.").getItem(2)) \
                 .withColumn("category_level_4", split(col("category_code"), "\\.").getItem(3)) \
                 .select("category_code", "category_level_1", "category_level_2", "category_level_3", "category_level_4").distinct()
dim_categories.coalesce(4).write.mode("overwrite").parquet(category_path)
print("dim_categories written")

"""
    Dim_products
"""
products_path = os.getenv("dim_products")

dim_products = df.select("product_id").distinct()
dim_products.coalesce(4).write.mode("overwrite").parquet(products_path)
print("dim_products written")

"""
    Dim_brands
"""
dim_brands = df.select("brand").distinct()

brand_path = os.getenv("dim_brands")
dim_brands.coalesce(4).write.mode("overwrite").parquet(brand_path)
print("dim_brands written")

"""
    Dim_users
"""

from pyspark.sql.functions import min as _min, sum as _sum, when

dim_users = df.groupBy("user_id").agg(
    _min("event_date").alias("first_seen_date"),
    _sum(when(col("event_type") == "purchase", col("price")).otherwise(0)).alias("total_lifetime_revenue")
).orderBy("total_lifetime_revenue", ascending=False)

users_path = os.getenv("dim_users")
dim_users.coalesce(4).write.mode("overwrite").parquet(users_path)
print("dim_user written")

"""
    Daily KPI
"""

from pyspark.sql.functions import count, countDistinct, avg

daily_kpi = df.groupBy("event_date").agg(
    count("*").alias("total_events"),
    countDistinct("user_id").alias("unique_users"),
    countDistinct("user_session").alias("total_sessions"),
    _sum(when(col("event_type") == "view", 1).otherwise(0)).alias("views"),
    _sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("carts"),
    _sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
    _sum(when(col("event_type") == "purchase", col("price")).otherwise(0)).alias("total_revenue"),
    avg("price").alias("avg_price")
)

daily_kpi = daily_kpi.withColumn("conversion_rate", col("purchases") / col("views"))

daily_kpi = daily_kpi.join(dim_date, daily_kpi.event_date == dim_date.event_date, "left").select("date_key", "total_events", "unique_users", "total_sessions", "views", "purchases", "total_revenue", "avg_price", "conversion_rate")

daily_kpi_path = os.getenv("daily_kpi")
daily_kpi.coalesce(1).write.mode("overwrite").parquet(daily_kpi_path)
print("daily_kpi written")

"""
    Fact_product_daily
"""

from pyspark.sql.functions import count, countDistinct, avg

fact_product_daily = df.groupBy("product_id", "event_date").agg(
    count("*").alias("total_events"),
    countDistinct("user_id").alias("unique_users"),
    countDistinct("user_session").alias("total_sessions"),
    _sum(when(col("event_type") == "view", 1).otherwise(0)).alias("views"),
    _sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("carts"),
    _sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
    _sum(when(col("event_type") == "purchase", col("price")).otherwise(0)).alias("total_revenue"),
    avg("price").alias("avg_price")
)

fact_product_daily = fact_product_daily.join(
    dim_date, fact_product_daily.event_date == dim_date.event_date, "left"
).select(
    "product_id", "date_key", "total_events", "views", "carts", "purchases", "total_revenue", "avg_price"
)

fact_product = os.getenv("fact_products")
fact_product_daily.coalesce(20).write.mode("overwrite").parquet(fact_product)
print("fact_product_daily written")

"""
    Fact_user_daily
"""

from pyspark.sql.functions import max as _max, datediff, lit

latest_date = df.select(_max("event_date")).collect()[0][0]

fact_user_metrics = df.groupBy("user_id").agg(
    count("*").alias("total_events"),
    countDistinct("event_date").alias("active_days"),
    _sum(when(col("event_type")=="purchase", 1).otherwise(0)).alias("purchases"),
    _sum(when(col("event_type") == "purchase", col("price")).otherwise(0)).alias("total_spent"),
    _max("event_date").alias("last_event_date")
).withColumn("recency_days", datediff(lit(latest_date), col("last_event_date")))

fact_user_path = os.getenv("fact_uesrs")
fact_user_metrics.coalesce(4).write.mode("overwrite").parquet(fact_user_path)

print("fact_user_metrics written")

"""
    Fact_hourly_traffic
"""

fact_hourly_traffic = df.withColumn("hour", hour("event_time")).groupBy("event_date", "hour").agg(
        count("*").alias("total_events"),
        _sum(when(col("event_type")=="view", 1).otherwise(0)).alias("views"),
        _sum(when(col("event_type")=="carts", 1).otherwise(0)).alias("carts"),
        _sum(when(col("event_type")=="purchase", 1).otherwise(0)).alias("purchases")
    )

fact_hourly_traffic = fact_hourly_traffic.join(dim_date, fact_hourly_traffic.event_date == dim_date.event_date, "left") \
               .select("date_key", "hour", "total_events", "views", "carts", "purchases")

fact_hourly_path = os.getenv("fact_hourly")

fact_hourly_traffic.coalesce(5).write.mode("overwrite").partitionBy("date_key").parquet(fact_hourly_path)

print("fact_hourly_traffic written")

job.commit()