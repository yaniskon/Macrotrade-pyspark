import logging

import sys
from pyspark.sql import SparkSession
import time
from data_transformations.trade import getWorldShare
LOG_FILENAME = 'GetWorldShareTradeCommoditiesAnnualProject.log'
APP_NAME = "GetWorldShare Annual: Ingest"

if __name__ == '__main__':
    logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO)
    logging.info(sys.argv)

    if len(sys.argv) != 2:
        logging.warning("Output path is required")
        sys.exit(1)

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    sc = spark.sparkContext
    app_name = sc.appName
    logging.info("Application Initialized: " + app_name)
    output_path = sys.argv[1]
    getWorldShare.run(spark, output_path, 'C', 'A')
    logging.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()