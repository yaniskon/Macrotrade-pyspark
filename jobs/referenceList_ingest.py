import logging

import sys
from pyspark.sql import SparkSession

from data_transformations.trade import ingest_Referencelist

LOG_FILENAME = 'referneceListTradeProject.log'
APP_NAME = "Reference List Tables: Ingest"

if __name__ == '__main__':
    logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO)
    logging.info(sys.argv)

    if len(sys.argv) != 2:
        logging.warning("Output path are required")
        sys.exit(1)

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    sc = spark.sparkContext
    app_name = sc.appName
    logging.info("Application Initialized: " + app_name)
    output_path = sys.argv[1]
    ingest_Referencelist.run(spark, output_path)
    logging.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
