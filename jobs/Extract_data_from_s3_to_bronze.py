
import sys
import zipfile, os, boto3
import shutil

from pyspark.sql.functions import current_timestamp
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

s3 = boto3.client('s3')
bucket = os.getenv("bucket")
zip_key = os.getenv("zipkey")        # S3 object key
local_zip = os.getenv("local_zip")            # local temp path
extract_dir = os.getenv("extract_dir")
staging_prefix = os.getenv("staging_prefix")

# 1. Download the zip from S3 to spark driver's local /tmp
print("Downloading zip from S3...")
s3.download_file(bucket, zip_key, local_zip)

# 2. Extract all CSV files
os.makedirs(extract_dir, exist_ok=True)
with zipfile.ZipFile(local_zip, 'r') as zf:
    csv_files = [name for name in zf.namelist() if name.endswith('.csv')]
    print(f"Found {len(csv_files)} CSV files: {csv_files}")
    for csv_file in csv_files:
        zf.extract(csv_file, extract_dir)
        local_csv = os.path.join(extract_dir, csv_file)
        s3_key = staging_prefix + csv_file
        print(f"Uploading {csv_file} to s3://{bucket}/{s3_key}")
        s3.upload_file(local_csv, bucket, s3_key)
        os.remove(local_csv)   # delete local CSV after upload

# 3. Clean up local zip and extraction directory
os.remove(local_zip)

shutil.rmtree(extract_dir)   # remove the now-empty directory
print("Done all CSVs uploaded to S3 staging.")

df = spark.read.option("header", "true").csv(f"s3://{bucket}/{staging_prefix}*.csv")

df = df.withColumn("ingestion_date", current_timestamp())

bronze_path = os.getenv("bronze")

df.write.mode("overwrite").parquet(bronze_path)

print("df written")
s3.delete_object(Bucket=bucket, Key='temp/')
job.commit()