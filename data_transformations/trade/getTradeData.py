from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from comtradeapicall import getFinalData
import json

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ComtradeParallelFetcher") \
    .getOrCreate()

# Path to the input query plan and output
query_plan_path = "query_plan.parquet"  # Adjust if needed
output_path = "trade_data_output.parquet"

# Load the query plan Parquet file
query_plan_df = spark.read.parquet(query_plan_path).limit(200)

# OPTIONAL: Repartition for parallelism (tune this number)
query_plan_df = query_plan_df.repartition(4)

# Define the API fetch function
def fetch_data(rows):
    result = []
    for row in rows:
        try:
            data = api.getFinalData(
                reporterCode=str(row.reporter_id),
                partnerCode=str(row.partner_id),
                year=str(row.year),
                tradeFlowCode=str(row.flow_id),
                cmdCode=row.cmdCode,
                classification='HS',
                freq='A',
                format_output='json'
            )
            result.append((
                row.reporter_id,
                row.partner_id,
                row.flow_id,
                row.cmdCode,
                row.year,
                json.dumps(data)
            ))
        except Exception as e:
            print(f"[ERROR] Fetch failed for {row.reporter_id}-{row.partner_id}-{row.flow_id}-{row.cmdCode}-{row.year}: {e}")
    return iter(result)

# Apply the fetch function in parallel
results_rdd = query_plan_df.rdd.mapPartitions(fetch_data)

# Define output schema
output_schema = StructType([
    StructField("reporter_id", StringType()),
    StructField("partner_id", StringType()),
    StructField("flow_id", StringType()),
    StructField("cmdCode", StringType()),
    StructField("year", IntegerType()),
    StructField("json_data", StringType()),
])

# Convert the result RDD back to a DataFrame
results_df = spark.createDataFrame(results_rdd, schema=output_schema)

# Save the results to Parquet
results_df.write.mode("overwrite").parquet(output_path)

print(f"✅ Finished writing results to: {output_path}")

