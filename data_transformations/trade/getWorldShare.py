from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import duckdb
import requests
import json
import time

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Comtrade WorldShare Fetcher") \
    .getOrCreate()

# === Step 1: Read Parquet using DuckDB ===
query_plan_path = "query_plan_getWorldShare.parquet"
con = duckdb.connect()
query_result = con.execute(f"SELECT DISTINCT reporter_id, reporter_name FROM parquet_scan('{query_plan_path}')").fetchall()

# === Step 2: Convert DuckDB result to PySpark DataFrame ===
schema = StructType([
    StructField("reporter_id", IntegerType()),
    StructField("reporter_name", StringType())
])
query_plan_df = spark.createDataFrame(query_result, schema=schema)

# Repartition for parallelism
query_plan_df = query_plan_df.repartition(4)

# === Step 3: Function to fetch data from API ===
def fetch_worldshare_data(rows):
    result = []
    for row in rows:
        try:
            time.sleep(1)  # Respect rate limit
            url = f"https://comtradeapi.un.org/public/v1/getWorldShare/C/A/?reporterCode={row.reporter_id}"
            response = requests.get(url)
            response.raise_for_status()
            json_data = response.json()

            result.append((
                row.reporter_id,
		row.reporter_name,
                json.dumps(json_data)  # Store JSON as string
            ))
            print(f"I am doing country: {row.reporter_name}")
        except Exception as e:
            print(f"[ERROR] Failed for reporter_id={row.reporter_id}: {e}")
    return iter(result)

# === Step 4: Apply API call in parallel via RDD ===
results_rdd = query_plan_df.rdd.mapPartitions(fetch_worldshare_data)

# Define the output schema
output_schema = StructType([
    StructField("reporter_id", IntegerType()),
    StructField("reporter_name", StringType()),
    StructField("json_data", StringType()),
])

# Create DataFrame and write to Parquet
results_df = spark.createDataFrame(results_rdd, schema=output_schema)
results_df.write.mode("overwrite").parquet("worldshare_output.parquet")

print("✅ WorldShare data written to: worldshare_output.parquet")

spark.stop()
