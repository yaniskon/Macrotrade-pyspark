import logging
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json
import os
import time

def fetch_detailed_trade_data(reporter_code: int, period: int, api_key: str) -> dict or None:
    """Fetches detailed trade data from the UN Comtrade API for a specific reporter and period."""
    try:
        url = f"https://comtradeapi.un.org/data/v1/get/C/A/HS?reporterCode={reporter_code}&period={period}"
        headers = {'Ocp-Apim-Subscription-Key': api_key}
        time.sleep(1)
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        time.sleep(1)
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"[ERROR] Failed to fetch detailed data for reporter {reporter_code} and period {period}: {e}")
        return None

def save_response_to_json(data: dict, reporter_code: int, period: int, output_directory: str):
    """Saves the API response to a JSON file."""
    if data:
        filename = f"reporter_{reporter_code}_period_{period}.json"
        filepath = os.path.join(output_directory, filename)
        try:
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=4)
            logging.info(f"Saved data for reporter {reporter_code} and period {period} to: {filepath}")
        except Exception as e:
            logging.error(f"[ERROR] Failed to save data for reporter {reporter_code} and period {period} to {filepath}: {e}")
    else:
        logging.warning(f"No data to save for reporter {reporter_code} and period {period}.")

def run_loop_requests(spark: SparkSession, input_parquet_path: str, subscription_key: str, output_directory: str):
    """Reads a Parquet file, iterates through rows where totalRecords < 25000,
    makes requests to the UN Comtrade detailed data API, and saves the responses to JSON files."""
    logging.info(f"Starting loop requests based on data from: {input_parquet_path}")
    os.makedirs(output_directory, exist_ok=True)  # Create the output directory if it doesn't exist

    try:
        df = spark.read.parquet(input_parquet_path)
        filtered_df = df.filter(col("totalRecords") < 25000)
        collected_data = filtered_df.select("reporterCode", "period").collect()

        logging.info(f"Found {len(collected_data)} rows with totalRecords < 25000.")

        for row in collected_data:
            reporter_code = row.reporterCode
            period = row.period
            logging.info(f"Fetching detailed data for reporter: {reporter_code}, period: {period}")
            api_response = fetch_detailed_trade_data(reporter_code, period, subscription_key)
            if api_response:
                save_response_to_json(api_response, reporter_code, period, output_directory)
            else:
                logging.warning(f"Failed to fetch detailed data for reporter: {reporter_code}, period: {period}")

        logging.info("✅ Loop requests completed and responses saved to JSON files.")

    except Exception as e:
        logging.error(f"[ERROR] An error occurred during the loop request and save process: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    output_path_from_previous_script = "data/DA/*.parquet"  # Replace with the actual path
    comtrade_subscription_key = "0ef9649f5edf4c49b6b87a9c4ba0605f"  # Replace with your actual API key
    output_json_directory = "data/get/comtrade_data_json"  # Directory to save the JSON files

    spark = SparkSession.builder.appName("ComtradeLoopRequest").getOrCreate()

    run_loop_requests(spark, output_path_from_previous_script, comtrade_subscription_key, output_json_directory)

    spark.stop()