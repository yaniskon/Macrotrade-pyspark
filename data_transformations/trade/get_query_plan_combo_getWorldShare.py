from pathlib import Path
import duckdb

# Setup paths
#base_path = Path(__file__).resolve().parent.parent.parent / "resources"
output_path = Path(__file__).resolve().parent / "query_plan_getWorldShare.parquet"

# Connect to DuckDB
conn = duckdb.connect()

# Read and create tables
conn.execute(f"""
    CREATE TABLE reporters AS SELECT id AS reporter_id, text AS reporter_name FROM '../../resources/reference_tables/reporter/*.parquet';
""")

# Create an in-memory list of years
years = list(range(1962, 2025))
conn.execute(f"""
    CREATE TABLE years(year INTEGER);
""")
for year in years:
    conn.execute(f"INSERT INTO years VALUES ({year});")

# Generate the query plan (Cartesian product) and write to Parquet
conn.execute(f"""
    COPY (
        SELECT
            r.reporter_id,
            r.reporter_name,
	    y.year
        FROM reporters r
	CROSS JOIN years y
    ) TO '{output_path}' (FORMAT PARQUET);
""")

print("✅ Query plan written to:", output_path)

