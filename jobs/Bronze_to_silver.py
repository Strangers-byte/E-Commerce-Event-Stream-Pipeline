
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

"""
    Creating a Spark dataframe
"""

bronze_table = os.getenv("bronze_table")

df = spark.table(bronze_table)

"""
    Creating a Dynamic frame
"""
from awsglue.dynamicframe import DynamicFrame

df = DynamicFrame.fromDF(df, glueContext, "dynamic_df")
df = ApplyMapping.apply(
    frame=df,
    mappings=[
        ("event_time", "string", "event_time", "timestamp"),
        ("event_type", "string", "event_type", "string"),
        ("product_id", "string", "product_id", "long"),
        ("category_id", "string", "category_id", "long"),
        ("category_code", "string", "category_code", "string"),
        ("brand", "string", "brand", "string"),
        ("price", "string", "price", "float"),
        ("user_id", "string", "user_id", "long"),
        ("user_session", "string", "user_session", "string"),
        ("ingestion_date", "timestamp", "ingestion_date", "timestamp")],
    transformation_ctx='silver_transformation'
)

from pyspark.sql.types import DecimalType
from pyspark.sql.functions import col

df = df.toDF()  # or work directly on original DataFrame
df = df.withColumn("price", col("price").cast(DecimalType(10,2)))

from pyspark.sql.functions import to_date

df=df.withColumn("event_date", to_date("event_time"))

silver_path = os.getenv("silver")

df.write.mode("overwrite").partitionBy("event_date").parquet(silver_path)

job.commit()