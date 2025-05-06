import logging
import time
import json
import requests

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F

def fetch_metadata_data(typeCode: str, freqCode: str, clCode: str):
    """
    Fetches metadata from the API for the given typeCode, freqCode, and clCode.
    Returns a list of tuples for DataFrame creation.
    """
    result = []
    try:
        time.sleep(0.3)  # Be kind to the API (rate limit)
        url = f"https://comtradeapi.un.org/public/v1/getMetadata/{typeCode}/{freqCode}/{clCode}"
        response = requests.get(url)
        response.raise_for_status()
        json_data = response.json()
        result.append((typeCode, freqCode, clCode, json.dumps(json_data)))
        logging.info(f"✅ Processed: typeCode={typeCode}, freqCode={freqCode}, clCode={clCode}")
        print(f"✅ Processed: typeCode={typeCode}, freqCode={freqCode}, clCode={clCode}")
    except Exception as e:
        logging.error(f"[ERROR] Failed for typeCode={typeCode}, freqCode={freqCode}, clCode={clCode}: {e}")
    return result

def run(spark: SparkSession, output_path: str, typeCode: str, freqCode: str, clCode: str):
    logging.info(f"Fetching metadata for typeCode={typeCode}, freqCode={freqCode}, clCode={clCode}")

    # Fetch data
    data = fetch_metadata_data(typeCode, freqCode, clCode)

    # Define output schema and create DataFrame with correct column names
    output_schema = StructType([
        StructField("typeCode", StringType()),
        StructField("freqCode", StringType()),
        StructField("clCode", StringType()),
        StructField("json_data", StringType()),
    ])
    df = spark.createDataFrame(data, schema=output_schema)
    df.show()
    # Parse the JSON string into a struct
    df = df.withColumn(
        "parsed_json",
        F.from_json(
            "json_data",
            """struct<
                elapsedTime:string,
                count:int,
                data:array<struct<
                    reporterCode:int,
                    period:int,
                    typeCode:string,
                    freqCode:string,
                    datasetCode:long,
                    notes:array<struct<
                        datasetCode:long,
                        typeCode:string,
                        freqCode:string,
                        period:int,
                        reporterCode:int,
                        reporterDescription:string,
                        currency:string,
                        importConvFactor:string,
                        exportConvFactor:string,
                        tradeSystem:string,
                        classificationCode:string,
                        importValuation:string,
                        exportValuation:string,
                        importPartnerCountry:string,
                        exportPartnerCountry:string,
                        importPartner2Country:string,
                        exportPartner2Country:string,
                        publicationNote:string,
                        publicationDate:string,
                        publicationDateShort:string
                    >>
                >>,
                error:string
            >"""
        )
    )
    df.show()
    # Explode the data array, then the notes array
    df = df.select(
        "clCode",
        F.explode("parsed_json.data").alias("data")
    ).select(
        "clCode",
        "data.reporterCode",
        "data.period",
        "data.typeCode",
        "data.freqCode",
        "data.datasetCode",
        F.explode("data.notes").alias("note")
    )
    df.show()
    # Select all the fields from the exploded notes
    df = df.select(
        "typeCode",
        "freqCode",
        "clCode",
        "reporterCode",
        "period",
        "datasetCode",
        "note.reporterDescription",
        "note.currency",
        "note.importConvFactor",
        "note.exportConvFactor",
        "note.tradeSystem",
        "note.classificationCode",
        "note.importValuation",
        "note.exportValuation",
        "note.importPartnerCountry",
        "note.exportPartnerCountry",
        "note.importPartner2Country",
        "note.exportPartner2Country",
        "note.publicationNote",
        "note.publicationDate",
        "note.publicationDateShort"
    )

    logging.info(f"Writing results to: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    logging.info("✅ Metadata written successfully.")
