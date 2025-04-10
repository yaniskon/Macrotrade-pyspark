import comtradeapicall
from pyspark.sql import SparkSession
import logging
from typing import List
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO)


def sanitize_columns(columns: List[str]) -> List[str]:
    return [col.replace(" ", "_") for col in columns]


def run(spark: SparkSession, output_path: str):
    logging.info("Calling comtradeapicall.listReference()...")
    pandas_df = comtradeapicall.listReference()

    if pandas_df.empty:
        logging.warning("Received empty DataFrame from comtradeapicall.")
        return

    logging.info("Converting to Spark DataFrame...")
    spark_df = spark.createDataFrame(pandas_df)

    logging.info("Sanitizing column names...")
    sanitized_columns = sanitize_columns(spark_df.columns)
    spark_df = spark_df.toDF(*sanitized_columns)

    logging.info("Writing to Parquet at: %s", output_path)
    spark_df.write.mode("overwrite").parquet(output_path)

    logging.info("Write complete.")


# Example usage
if __name__ == "__main__":
    spark = SparkSession.builder.appName("ComtradeReferenceIngest").getOrCreate()
    script_path = Path(__file__).resolve()
    parent_dir = script_path.parent.parent.parent  # Go two levels up
    output_path = str(parent_dir / "resources/ComtradeReference")
    run(spark, output_path)