import logging
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json
import os
import time
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobClient

def fetch_detailed_trade_data(reporter_code: int, period: int, api_key: str) -> dict or None:
    """Fetches detailed trade data from the UN Comtrade API for a specific reporter and period."""
    try:
        url = f"https://comtradeapi.un.org/data/v1/get/C/A/HS?reporterCode={reporter_code}&period={period}"
        headers = {'Ocp-Apim-Subscription-Key': api_key}
        time.sleep(2)
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        time.sleep(1)
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"[ERROR] Failed to fetch detailed data for reporter {reporter_code} and period {period}: {e}")
        return None

def save_response_to_json(data: dict, reporter_code: int, period: int, output_directory: str, 
                         storage_account_name: str = None, container_name: str = None,
                         tenant_id: str = None, client_id: str = None, client_secret: str = None):
    """Saves the API response to either a local JSON file or Azure Blob Storage, depending on provided parameters."""
    if not data:
        logging.warning(f"No data to save for reporter {reporter_code} and period {period}.")
        return

    filename = f"reporter_{reporter_code}_period_{period}.json"
    json_string = json.dumps(data, indent=4)

    # Determine storage type based on provided parameters
    azure_credentials = [storage_account_name, container_name, tenant_id, client_id, client_secret]
    has_azure_credentials = all(azure_credentials)
    has_local_path = output_directory is not None

    if has_azure_credentials and has_local_path:
        logging.error("Both local and Azure storage parameters provided. Please choose only one storage option.")
        return
    elif not has_azure_credentials and not has_local_path:
        logging.error("No storage parameters provided. Please specify either local path or Azure credentials.")
        return

    # Save locally if output_directory is provided
    if has_local_path:
        try:
            os.makedirs(output_directory, exist_ok=True)
            filepath = os.path.join(output_directory, filename)
            with open(filepath, 'w') as f:
                f.write(json_string)
            logging.info(f"Saved data locally for reporter {reporter_code} and period {period} to: {filepath}")
        except Exception as e:
            logging.error(f"[ERROR] Failed to save data locally for reporter {reporter_code} and period {period}: {e}")

    # Save to Azure Blob Storage if all required credentials are provided
    elif has_azure_credentials:
        try:
            account_url = f"https://{storage_account_name}.blob.core.windows.net/"
            credential = ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret
            )
            blob_client = BlobClient(account_url, container_name=container_name, blob_name=filename, credential=credential)
            blob_client.upload_blob(json_string, overwrite=True)
            logging.info(f"Saved data to Azure Blob Storage for reporter {reporter_code} and period {period}")
        except Exception as e:
            logging.error(f"[ERROR] Failed to save data to Azure Blob Storage for reporter {reporter_code} and period {period}: {e}")

def run_loop_requests(spark: SparkSession, input_parquet_path: str, subscription_key: str, output_directory: str,
                     storage_account_name: str = None, container_name: str = None,
                     tenant_id: str = None, client_id: str = None, client_secret: str = None):
    """Reads a Parquet file, iterates through rows where totalRecords < 250000,
    makes requests to the UN Comtrade detailed data API, and saves the responses to JSON files."""
    logging.info(f"Starting loop requests based on data from: {input_parquet_path}")

    try:
        df = spark.read.parquet(input_parquet_path)
        filtered_df = df.filter(col("totalRecords") <= 250000)
        collected_data = filtered_df.select("reporterCode", "period").collect()

        logging.info(f"Found {len(collected_data)} rows with totalRecords < 250000.")

        for row in collected_data:
            reporter_code = row.reporterCode
            period = row.period
            logging.info(f"Fetching detailed data for reporter: {reporter_code}, period: {period}")
            api_response = fetch_detailed_trade_data(reporter_code, period, subscription_key)
            if api_response:
                save_response_to_json(
                    api_response, reporter_code, period, output_directory,
                    storage_account_name, container_name, tenant_id, client_id, client_secret
                )
            else:
                logging.warning(f"Failed to fetch detailed data for reporter: {reporter_code}, period: {period}")

        logging.info("✅ Loop requests completed and responses saved to JSON files.")

    except Exception as e:
        logging.error(f"[ERROR] An error occurred during the loop request and save process: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    output_path_from_previous_script = "data/DA/*.parquet" 
    comtrade_subscription_key = "0ef9649f5edf4c49b6b87a9c4ba0605f"
    output_json_directory = "data/get/comtrade_data_json" 

    spark = SparkSession.builder.appName("ComtradeLoopRequest").getOrCreate()

    run_loop_requests(spark, output_path_from_previous_script, comtrade_subscription_key, output_json_directory)

    spark.stop()