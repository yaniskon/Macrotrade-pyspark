import logging
import time
import json
import requests

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F

def fetch_worldshare_data(typeCode: str, freqCode: str):
    """
    Fetches world share data from the API for the given typeCode and freqCode.
    Returns a list of tuples for DataFrame creation.
    """
    result = []
    try:
        time.sleep(0.3)  # Be kind to the API (rate limit)
        url = f"https://comtradeapi.un.org/public/v1/getWorldShare/{typeCode}/{freqCode}"
        response = requests.get(url)
        response.raise_for_status()
        json_data = response.json()
        result.append((typeCode, freqCode, json.dumps(json_data)))
        logging.info(f"✅ Processed: typeCode={typeCode}, freqCode={freqCode}")
        print(f"✅ Processed: typeCode={typeCode}, freqCode={freqCode}")
    except Exception as e:
        logging.error(f"[ERROR] Failed for typeCode={typeCode}, freqCode={freqCode}: {e}")
    return result

def run(spark: SparkSession, output_path: str, typeCode: str, freqCode: str):
    logging.info(f"Fetching world share data for typeCode={typeCode}, freqCode={freqCode}")

    # Fetch data
    data = fetch_worldshare_data(typeCode, freqCode)

    # Define output schema and create DataFrame with correct column names
    output_schema = StructType([
        StructField("typeCode", StringType()),
        StructField("freqCode", StringType()),
        StructField("json_data", StringType()),
    ])
    df = spark.createDataFrame(data, schema=output_schema)

    # Parse the JSON string into a struct
    df = df.withColumn(
        "parsed_json",
        F.from_json(
            "json_data",
            "struct<elapsedTime:string,count:int,data:array<struct<typeCode:string,freqCode:string,period:int,year:int,month:int,datasetDesc:string,reporterCode:int,reporterISO:string,reporterDesc:string,worldShare:double,isAvailable:boolean,dataCount:int,firstReleased:string,lastReleased:string,created:string,updated:string>>,error:string>"
        )
    )

    # Explode the data array to create separate rows
    df = df.select(
        "typeCode",
        "freqCode",
        F.explode("parsed_json.data").alias("data")
    )

    # Select all the fields from the exploded data
    df = df.select(
        "typeCode",
        "freqCode",
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
