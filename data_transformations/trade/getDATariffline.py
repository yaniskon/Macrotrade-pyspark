import logging
import time
import json
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, LongType, DoubleType
from pyspark.sql import functions as F

def run(spark: SparkSession, output_path: str):
    logging.info("Starting DATariffline data fetch")

    # === Step 1: Define the function to call the API ===
    def fetch_tariffline_data():
        try:
            url = "https://comtradeapi.un.org/public/v1/getDATariffline/C/A/HS"
            response = requests.get(url)
            response.raise_for_status()
            json_data = response.json()
            return json_data
        except Exception as e:
            logging.error(f"[ERROR] Failed to fetch data: {e}")
            return None

    # === Step 2: Fetch data from API ===
    api_data = fetch_tariffline_data()
    if not api_data:
        raise Exception("Failed to fetch data from API")

    # === Step 3: Convert API response to PySpark DataFrame ===
    # Define schema for the data
    schema = StructType([
        StructField("datasetCode", LongType()),
        StructField("typeCode", StringType()),
        StructField("freqCode", StringType()),
        StructField("period", IntegerType()),
        StructField("reporterCode", IntegerType()),
        StructField("reporterISO", StringType()),
        StructField("classificationCode", StringType()),
        StructField("isOriginalClassification", BooleanType()),
        StructField("isExtendedFlowCode", BooleanType()),
        StructField("isExtendedPartnerCode", BooleanType()),
        StructField("isExtendedPartner2Code", BooleanType()),
        StructField("isExtendedCmdCode", BooleanType()),
        StructField("isExtendedCustomsCode", BooleanType()),
        StructField("isExtendedMotCode", BooleanType()),
        StructField("totalRecords", IntegerType()),
        StructField("datasetChecksum", LongType()),
        StructField("lengthCmdCode", IntegerType()),
        StructField("firstReleased", StringType()),
        StructField("lastReleased", StringType())
    ])

    # Create DataFrame from API response
    df = spark.createDataFrame(api_data["data"], schema=schema)

    # Add metadata columns
    df = df.withColumn("elapsedTime", F.lit(api_data["elapsedTime"])) \
           .withColumn("count", F.lit(api_data["count"])) \
           .withColumn("error", F.lit(api_data["error"]))

    # === Step 4: Save to parquet ===
    logging.info(f"Writing results to: {output_path}")
    df.write.mode("overwrite").parquet(output_path)

    logging.info("✅ DATariffline data written successfully.") 