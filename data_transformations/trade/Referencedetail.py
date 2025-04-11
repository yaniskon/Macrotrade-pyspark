import logging
from typing import List
from pathlib import Path

import pandas as pd
import requests
from pyspark.sql import SparkSession

import comtradeapicall

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def sanitize_columns(columns: List[str]) -> List[str]:
    return [col.replace(" ", "_").replace(".", "_").replace(":", "_") for col in columns]


def fetch_reference_links() -> pd.DataFrame:
    logger.info("Fetching reference list from comtradeapicall...")
    return comtradeapicall.listReference()


def fetch_and_convert_to_spark(spark: SparkSession, url: str) -> 'DataFrame':
    logger.info("Fetching JSON from: %s", url)
    response = requests.get(url)
    response.raise_for_status()
    json_data = response.json()

    # If the JSON is a dict, assume the first non-empty list is the data
    if isinstance(json_data, dict):
        for value in json_data.values():
            if isinstance(value, list) and value:
                df = pd.DataFrame(value)
                break
        else:
            raise ValueError("No list-type data found in JSON.")
    elif isinstance(json_data, list):
        df = pd.DataFrame(json_data)
    else:
        raise ValueError("Unsupported JSON structure.")

    logger.info("Converting to Spark DataFrame...")
    spark_df = spark.createDataFrame(df)
    sanitized = sanitize_columns(spark_df.columns)
    return spark_df.toDF(*sanitized)


def run(spark: SparkSession, output_dir: str) -> None:
    reference_df = fetch_reference_links()

    for _, row in reference_df.iterrows():
        category = row['category'].replace(":", "_")
        file_uri = row['fileuri']
        logger.info(f"Processing category: {category}")

        try:
            df = fetch_and_convert_to_spark(spark, file_uri)
            output_path = Path(output_dir) / category
            logger.info(f"Writing to Parquet: {output_path}")
            df.write.mode("overwrite").parquet(str(output_path))
        except Exception as e:
            logger.error(f"Failed to process {category} from {file_uri}: {e}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("ComtradeReferenceIngest").getOrCreate()

    # Example: save output 2 levels up in a folder called 'comtrade_parquet'
    script_path = Path(__file__).resolve()
    output_path = script_path.parent.parent.parent / "resources/reference_tables"

    run(spark, str(output_path))

