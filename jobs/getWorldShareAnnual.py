import logging

import sys
from pyspark.sql import SparkSession

from data_transformations.trade import getWorldShare
LOG_FILENAME = 'GetWorldShareTradeProject.log'
APP_NAME = "GetWorldShare Annual: Ingest"

if __name__ == '__main__':
    logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO)
    logging.info(sys.argv)

    if len(sys.argv) != 3:
        logging.warning("Output path are required")
        sys.exit(1)

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    sc = spark.sparkContext
    app_name = sc.appName
    logging.info("Application Initialized: " + app_name)
    input_ref_tables_path = sys.argv[1]
    output_path = sys.argv[2]
    getWorldShare_new.run(spark, input_ref_tables_path, output_path)
    logging.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
