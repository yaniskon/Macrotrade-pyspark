'''
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ComtradeFullDataPull") \
    .getOrCreate(
)

import requests
import pandas as pd

def load_ref_json(url):
    return pd.DataFrame(requests.get(url).json()['results'])

# Load reference data
reporters = load_ref_json("https://comtradeapi.un.org/files/v1/app/reference/Reporters.json")
partners = load_ref_json("https://comtradeapi.un.org/files/v1/app/reference/partnerAreas.json")
flows = load_ref_json("https://comtradeapi.un.org/files/v1/app/reference/tradeRegimes.json")

# You may load commodity codes manually from HS JSON or a CSV
commodity_codes = pd.read_csv("hs6_codes.csv")  # Use your own or scrape

# Years manually defined or scraped
years = list(range(2010, 2024))

'''

import duckdb

# Connect to an in-memory DuckDB database
conn = duckdb.connect()

# Read Parquet files into DuckDB tables
conn.execute("""
    CREATE TABLE reporters AS SELECT * FROM '/../../resources/reference_tables/reporter/*.parquet';
    CREATE TABLE partners AS SELECT * FROM '/../../resources/reference_tables/partner/*.parquet';
    CREATE TABLE flows AS SELECT * FROM '/../../resources/reference_tables/flow/*.parquet';
""")

