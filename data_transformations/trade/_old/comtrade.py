import logging
from typing import List
import requests
import pandas as pd

from References.trade import sanitize_columns

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)


def fetch_comtrade_data(reporters: str, partners: str, start_year: int, end_year: int,
                        commodity_code: str = 'TOTAL', trade_flow: str = 'M', classification: str = 'HS') -> pd.DataFrame:
    logging.info("Fetching Comtrade data from API...")
    
    base_url = "https://comtradeapi.un.org/public/v1/preview"
    params = {
        'reporterCode': reporters,
        'partnerCode': partners,
        'freqCode': 'A',
        'clCode': classification,
        'typeCode': trade_flow,
        'cmdCode': commodity_code,
        'reportingYear': f'{start_year}-{end_year}'
    }

    response = requests.get(base_url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        if 'data' in data:
            return pd.DataFrame(data['data'])
        else:
            logging.warning("No data field in response.")
            return pd.DataFrame()
    else:
        logging.error(f"Failed to fetch data. Status code: {response.status_code}")
        return pd.DataFrame()


def run(spark: SparkSession, transformation_path: str,
        reporters: str, partners: str, start_year: int, end_year: int,
        commodity_code: str = 'TOTAL', trade_flow: str = 'M', classification: str = 'HS') -> None:
    
    df = fetch_comtrade_data(reporters, partners, start_year, end_year,
                             commodity_code, trade_flow, classification)

    if df.empty:
        logging.warning("No data to process. Exiting.")
        return

    logging.info("Converting pandas DataFrame to Spark DataFrame...")
    spark_df = spark.createDataFrame(df)
    sanitized_columns = sanitize_columns(spark_df.columns)
    spark_df = spark_df.toDF(*sanitized_columns)

    logging.info("Writing data to Parquet at: %s", transformation_path)
    spark_df.write.mode("overwrite").parquet(transformation_path)

    logging.info("Data written successfully.")


# Example usage
if __name__ == "__main__":
    spark = SparkSession.builder.appName("ComtradeIngest").getOrCreate()

    run(
        spark=spark,
        transformation_path="output/comtrade_data.parquet",
        reporters='840',
        partners='all',
        start_year=2020,
        end_year=2021,
        commodity_code='1001',
        trade_flow='X'
    )
