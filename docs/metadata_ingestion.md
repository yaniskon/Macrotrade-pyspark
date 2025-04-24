# Metadata Tables Ingestion

The first step in our data processing pipeline is to extract and store all metadata tables. This process involves:

1. Extracting metadata tables from the source
2. Converting them into parquet format for efficient storage and access
3. Saving them for later use in the pipeline

This is accomplished by running the `referenceTables_ingest` job, which handles the extraction and transformation of these metadata tables into parquet files. These parquet files will serve as a reference for subsequent data processing steps, ensuring we have quick and efficient access to the metadata when needed.

The parquet format is chosen for its:
- Columnar storage efficiency
- Fast query performance
- Schema evolution support
- Compression capabilities

This initial step sets up the foundation for all subsequent data processing operations in our pipeline. 