import logging
import sys
from pyspark.sql import SparkSession
from data_transformations.trade import get
import os

LOG_FILENAME = 'getExtract.log'
APP_NAME = "get: Extract"

if __name__ == '__main__':
    logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info(sys.argv)

    if len(sys.argv) < 4:
        logging.error("Required arguments: input_path api_key output_path storage_mode [storage_account_name container_name tenant_id client_id client_secret]")
        logging.error("For local storage, only input_path, api_key, output_path, and storage_mode are required")
        logging.error("For azure storage, all parameters are required")
        sys.exit(1)

    storage_mode = sys.argv[4].lower()
    if storage_mode == 'azure' and len(sys.argv) < 9:
        logging.error("For azure storage, all parameters are required: storage_account_name container_name tenant_id client_id client_secret")
        sys.exit(1)
    elif storage_mode not in ['local', 'azure']:
        logging.error("storage_mode should be either 'local' or 'azure'")
        sys.exit(1)
    # Parse command line arguments
    input_DA_path = sys.argv[1]
    api_key = sys.argv[2]
    output_path = sys.argv[3]
    storage_mode = sys.argv[4].lower()
    if storage_mode == 'azure':
        storage_account_name = sys.argv[5]
        container_name = sys.argv[6]
        tenant_id = sys.argv[7]
        client_id = sys.argv[8]
        client_secret = sys.argv[9]
    else:
        storage_account_name = None
        container_name = None
        tenant_id = None
        client_id = None
        client_secret = None

    # Validate storage mode
    if storage_mode not in ['local', 'azure']:
        logging.error("Invalid storage_mode. Must be either 'local' or 'azure'")
        sys.exit(1)

    # Initialize Spark session
    spark = SparkSession.builder.appName(APP_NAME) \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    # Configure Spark to use Azure Storage credentials if needed
    if storage_mode == 'azure':
        spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
        spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id)
        spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
        spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

    sc = spark.sparkContext
    app_name = sc.appName
    logging.info("Application Initialized: " + app_name)
    logging.info(f"Storage mode: {storage_mode}")

    # Set storage parameters based on mode
    storage_params = {
        'storage_account_name': storage_account_name if storage_mode == 'azure' else None,
        'container_name': container_name if storage_mode == 'azure' else None,
        'tenant_id': tenant_id if storage_mode == 'azure' else None,
        'client_id': client_id if storage_mode == 'azure' else None,
        'client_secret': client_secret if storage_mode == 'azure' else None
    }

    # Run the main process with selected storage options
    get.run_loop_requests(
        spark=spark,
        input_parquet_path=input_DA_path,
        subscription_key=api_key,
        output_directory=output_path if storage_mode == 'local' else None,
        **storage_params
    )

    logging.info("Application Done: " + spark.sparkContext.appName)
    spark.stop() 