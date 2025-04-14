from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from comtradeapicall import previewFinalData  # Updated import
import json
import time

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ComtradeParallelPreviewFetcher") \
    .getOrCreate()

# Path to the input query plan and output
query_plan_path = "query_plan.parquet"  # Adjust if needed
output_path = "preview_trade_data_output.parquet"  # Updated output path

# Load the query plan Parquet file
query_plan_df = spark.read.parquet(query_plan_path)

# Filter to 200 rows for testing (optional)
query_plan_df = query_plan_df.limit(200)

# Optional: Repartition for parallelism (e.g., to 10 partitions)
query_plan_df = query_plan_df.repartition(4)

# Define the API fetch function for preview data
def fetch_preview_data(rows):
    result = []
    for row in rows:
        try:
            time.sleep(1)
            # Call Comtrade API for preview data
            data_df = previewFinalData(  # Note the '_df' suffix to indicate DataFrame
                typeCode='C',
                freqCode='A',
                clCode='HS',
                period=str(row.year),
                #reporterCode=str(row.reporter_id),
                reporterCode=None,
                cmdCode=row.cmdCode,
                flowCode=str(row.flow_id),
                partnerCode=str(row.partner_id),
                partner2Code=None,
                customsCode=None,
                motCode=None,
                format_output='JSON'
            )
            # Convert the Pandas DataFrame to a list of dictionaries
            print(data_df.info())           
            data_list = data_df.to_dict('records')
            result.append((
                row.reporter_id,
                row.partner_id,
                row.flow_id,
                row.cmdCode,
                row.year,
                json.dumps(data_list)  # Now serializing a list of dictionaries
            ))
        except Exception as e:
            print(f"[ERROR] Failed to get preview for {row}: {e}")
    return iter(result)

# Apply the fetch function in parallel
results_rdd = query_plan_df.rdd.mapPartitions(fetch_preview_data)

# Define output schema for the DataFrame
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

# Save the preview results to Parquet
results_df.write.mode("overwrite").parquet(output_path)

print(f"✅ Finished writing preview results to: {output_path}")

spark.stop()
