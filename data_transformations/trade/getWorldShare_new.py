import logging
import time
import json
import requests
import duckdb

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

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
                time.sleep(1)  # Be kind to the API (rate limit)
                url = f"https://comtradeapi.un.org/public/v1/getWorldShare/C/A/?reporterCode={row.reporter_id}"
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
    results_df = spark.createDataFrame(results_rdd, schema=output_schema)

    logging.info(f"Writing results to: {output_path}")
    results_df.write.mode("overwrite").parquet(output_path)

    logging.info("✅ WorldShare data written successfully.")
