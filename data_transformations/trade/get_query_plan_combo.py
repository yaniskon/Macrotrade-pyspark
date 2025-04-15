from pathlib import Path
import duckdb

# Setup paths
#base_path = Path(__file__).resolve().parent.parent.parent / "resources"
output_path = Path(__file__).resolve().parent / "query_plan.parquet"

# Connect to DuckDB
conn = duckdb.connect()

# Read and create tables
conn.execute(f"""
    CREATE TABLE reporters AS SELECT id AS reporter_id, text AS reporter_name FROM '../../resources/reference_tables/reporter/*.parquet' WHERE id IN (156, 842);
    CREATE TABLE partners AS SELECT id AS partner_id, text AS partner_name FROM '../../resources/reference_tables/partner/*.parquet' WHERE id IN (156, 842);
    CREATE TABLE flows AS SELECT id AS flow_id, text AS flow_name FROM '../../resources/reference_tables/flow/*.parquet' WHERE id IN ('X', 'M');
    CREATE TABLE commodities AS SELECT id AS commodity_id FROM '../../resources/reference_tables/cmd_HS/*.parquet' WHERE aggrLevel = 0;
""")

# Create an in-memory list of years
years = list(range(2010, 2024))
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
            p.partner_id,
            p.partner_name,
            f.flow_id,
            f.flow_name,
            c.commodity_id AS cmdCode,
            y.year
        FROM reporters r
        CROSS JOIN partners p
        CROSS JOIN flows f
        CROSS JOIN commodities c
        CROSS JOIN years y
        WHERE r.reporter_id != p.partner_id
    ) TO '{output_path}' (FORMAT PARQUET);
""")

print("✅ Query plan written to:", output_path)

