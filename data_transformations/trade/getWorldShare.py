import logging
import time
import json
import requests
import duckdb

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F

def run(spark: SparkSession, input_ref_tables_path: str, output_path: str):
    logging.info(f"Reading query plan from: {input_ref_tables_path}")

    # === Step 1: Read Parquet using DuckDB ===
    con = duckdb.connect()
    query_result = con.execute(f"""
        SELECT DISTINCT reporter_id, reporter_name
        FROM parquet_scan('{input_ref_tables_path}')
    """).fetchall()

    # === Step 2: Convert DuckDB result to PySpark DataFrame ===
    schema = StructType([
        StructField("reporter_id", IntegerType()),
        StructField("reporter_name", StringType())
    ])
    query_plan_df = spark.createDataFrame(query_result, schema=schema)
    query_plan_df = query_plan_df.repartition(4)  # Repartition for parallelism

    # === Step 3: Define the function to call the API ===
    def fetch_worldshare_data(rows):
        result = []
        for row in rows:
            try:
                time.sleep(0.5)  # Be kind to the API (rate limit)
                url = f"https://comtradeapi.un.org/public/v1/getWorldShare/C/M/?reporterCode={row.reporter_id}"
                response = requests.get(url)
                response.raise_for_status()
                json_data = response.json()

                result.append((
                    row.reporter_id,
                    row.reporter_name,
                    json.dumps(json_data)
                ))
                logging.info(f"✅ Processed: {row.reporter_name}")
            except Exception as e:
                logging.error(f"[ERROR] Failed for reporter_id={row.reporter_id}: {e}")
        return iter(result)

    # === Step 4: Apply API fetch in parallel ===
    results_rdd = query_plan_df.rdd.mapPartitions(fetch_worldshare_data)

    # Define output schema and save
    output_schema = StructType([
        StructField("reporter_id", IntegerType()),
        StructField("reporter_name", StringType()),
        StructField("json_data", StringType()),
    ])
    df = spark.createDataFrame(results_rdd, schema=output_schema)

# Assuming your DataFrame is named df and the JSON column is named 'json_column'
# First, parse the JSON string into a struct
    df = df.withColumn("parsed_json", F.from_json("json_data", "struct<elapsedTime:string,count:int,data:array<struct<typeCode:string,freqCode:string,period:int,year:int,month:int,datasetDesc:string,reporterCode:int,reporterISO:string,reporterDesc:string,worldShare:double,isAvailable:boolean,dataCount:int,firstReleased:string,lastReleased:string,created:string,updated:string>>,error:string>"))

# Then explode the data array to create separate rows
    df = df.select(    
    F.col("reporter_id"),
    F.col("reporter_name"),
    F.col("parsed_json.elapsedTime").alias("elapsed_time"),
    F.col("parsed_json.error").alias("error"),
    F.explode("parsed_json.data").alias("data")
)

# Finally, select all the fields from the exploded data
    df = df.select(
    "elapsed_time",
    "error",
    "data.typeCode",
    "data.freqCode",
    "data.period",
    "data.year",
    "data.month",
    "data.datasetDesc",
    "data.reporterCode",
    "data.reporterISO",
    "data.reporterDesc",
    "data.worldShare",
    "data.isAvailable",
    "data.dataCount",
    "data.firstReleased",
    "data.lastReleased",
    "data.created",
    "data.updated"
)
    logging.info(f"Writing results to: {output_path}")
    df.write.mode("overwrite").parquet(output_path)

    logging.info("✅ WorldShare data written successfully.")
