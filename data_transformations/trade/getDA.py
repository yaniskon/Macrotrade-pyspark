import logging
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, LongType
from pyspark.sql import functions as F

def run(spark: SparkSession, output_path: str, subscription_key: str):
    logging.info("Starting DA data fetch")
    # === Step 1: Fetch data from API ===
    def fetch_da_data(subscription_key):
        try:
            url = "https://comtradeapi.un.org/data/v1/getDa/C/A/HS"
            headers = {'Ocp-Apim-Subscription-Key': subscription_key}
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"[ERROR] Failed to fetch data: {e}")
            return None

    api_data = fetch_da_data(subscription_key)
    if not api_data:
        raise Exception("Failed to fetch data from API")

    # === Step 2: Define schema ===
    schema = StructType([
        StructField("datasetCode", LongType()),
        StructField("typeCode", StringType()),
        StructField("freqCode", StringType()),
        StructField("period", IntegerType()),
        StructField("reporterCode", IntegerType()),
        StructField("reporterISO", StringType()),
        StructField("reporterDesc", StringType()),
        StructField("classificationCode", StringType()),
        StructField("classificationSearchCode", StringType()),
        StructField("isOriginalClassification", BooleanType()),
        StructField("isExtendedFlowCode", BooleanType()),
        StructField("isExtendedPartnerCode", BooleanType()),
        StructField("isExtendedPartner2Code", BooleanType()),
        StructField("isExtendedCmdCode", BooleanType()),
        StructField("isExtendedCustomsCode", BooleanType()),
        StructField("isExtendedMotCode", BooleanType()),
        StructField("totalRecords", IntegerType()),
        StructField("datasetChecksum", LongType()),
        StructField("firstReleased", StringType()),
        StructField("lastReleased", StringType())
    ])

    # === Step 3: Create DataFrame ===
    df = spark.createDataFrame(api_data["data"], schema=schema)

    # Add metadata columns
    df = df.withColumn("elapsedTime", F.lit(api_data["elapsedTime"])) \
           .withColumn("count", F.lit(api_data["count"])) \
           .withColumn("error", F.lit(api_data["error"]))

    # === Step 4: Save to parquet ===
    logging.info(f"Writing results to: {output_path}")
    df.write.mode("overwrite").parquet(output_path)

    logging.info("✅ DA data written successfully.") 