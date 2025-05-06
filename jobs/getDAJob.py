import logging
import sys
from pyspark.sql import SparkSession
from data_transformations.trade import getDA

LOG_FILENAME = 'DA.log'
APP_NAME = "DA: Ingest"

if __name__ == '__main__':
    logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO)
    logging.info(sys.argv)

    if len(sys.argv) != 3:
        logging.warning("Output path is required")
        sys.exit(1)

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    sc = spark.sparkContext
    app_name = sc.appName
    logging.info("Application Initialized: " + app_name)
    output_path = sys.argv[1]
    api_key = sys.argv[2]
    getDA.run(spark, output_path, api_key)
    logging.info("Application Done: " + spark.sparkContext.appName)
    spark.stop() 