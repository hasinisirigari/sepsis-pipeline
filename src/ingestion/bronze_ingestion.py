# Bronze Ingestion: MIMIC-IV CSVs → Parquet → S3
# Reads raw CSVs in chunks, converts to Parquet, uploads to Bronze layer.
# Large files (chartevents, labevents) are processed in chunks to avoid OOM.


import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

from src.utils.config import config
from src.utils.logging_config import setup_logging
from src.utils.s3_utils import get_s3_client

log = setup_logging("bronze_ingestion", json_output=False)

TABLES = {
    # ICU tables
    "icustays": {
        "path": "data/mimic-iv/icu/icustays.csv/icustays.csv",
        "chunk_size": None,
    },
    "d_items": {
        "path": "data/mimic-iv/icu/d_items.csv/d_items.csv",
        "chunk_size": None,
    },
    "inputevents": {
        "path": "data/mimic-iv/icu/inputevents.csv/inputevents.csv",
        "chunk_size": 500_000,
    },
    "chartevents": {
        "path": "data/mimic-iv/icu/chartevents.csv/chartevents.csv",
        "chunk_size": 500_000,  
    },
    # Hospital tables
    "patients": {
        "path": "data/mimic-iv/hosp/patients.csv/patients.csv",
        "chunk_size": None,
    },
    "admissions": {
        "path": "data/mimic-iv/hosp/admissions.csv/admissions.csv",
        "chunk_size": None,
    },
    "d_labitems": {
        "path": "data/mimic-iv/hosp/d_labitems.csv/d_labitems.csv",
        "chunk_size": None,
    },
    "labevents": {
        "path": "data/mimic-iv/hosp/labevents.csv/labevents.csv",
        "chunk_size": 500_000,  
    },
    "prescriptions": {
        "path": "data/mimic-iv/hosp/prescriptions.csv/prescriptions.csv",
        "chunk_size": 500_000,
    },
    "microbiologyevents": {
        "path": "data/mimic-iv/hosp/microbiologyevents.csv/microbiologyevents.csv",
        "chunk_size": None,
    },
}


def ingest_small_table(table_name: str, file_path: str) -> int:
    """Read entire CSV into memory, write as single Parquet file to S3."""
    s3 = get_s3_client()
    bucket = config.aws.s3_bucket

    df = pd.read_csv(file_path, low_memory=False)
    s3_key = f"bronze/{table_name}/{table_name}.parquet"

    # Convert to Parquet in memory, upload
    buffer = pa.BufferOutputStream()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buffer, compression="snappy")

    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=buffer.getvalue().to_pybytes(),
    )

    log.info("Uploaded small table", table=table_name, rows=len(df), s3_key=s3_key)
    return len(df)


def ingest_chunked_table(table_name: str, file_path: str, chunk_size: int) -> int:
    s3 = get_s3_client()
    bucket = config.aws.s3_bucket
    total_rows = 0

    reader = pd.read_csv(file_path, chunksize=chunk_size, low_memory=False)

    for i, chunk in enumerate(reader):
        s3_key = f"bronze/{table_name}/part_{i:04d}.parquet"

        buffer = pa.BufferOutputStream()
        table = pa.Table.from_pandas(chunk)
        pq.write_table(table, buffer, compression="snappy")

        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=buffer.getvalue().to_pybytes(),
        )

        total_rows += len(chunk)

        # Progress logging every 10 chunks
        if i % 10 == 0:
            log.info(
                "Chunk uploaded",
                table=table_name,
                chunk=i,
                chunk_rows=len(chunk),
                total_rows=total_rows,
            )

    log.info("Completed chunked upload", table=table_name, total_rows=total_rows, parts=i + 1)
    return total_rows


def run_ingestion():
    #Ingest all MIMIC-IV tables into Bronze layer on S3
    log.info("Starting Bronze ingestion", bucket=config.aws.s3_bucket)

    results = {}

    for table_name, table_config in TABLES.items():
        file_path = table_config["path"]

        if not os.path.exists(file_path):
            log.warning("File not found, skipping", table=table_name, path=file_path)
            continue

        log.info("Ingesting table", table=table_name)

        if table_config["chunk_size"] is None:
            rows = ingest_small_table(table_name, file_path)
        else:
            rows = ingest_chunked_table(table_name, file_path, table_config["chunk_size"])

        results[table_name] = rows

    # Summary
    log.info("Bronze ingestion complete", tables=results)
    print("\nINGESTION SUMMARY")
    for table_name, rows in results.items():
        print(f"  {table_name}: {rows:,} rows")
    print(f"  Total: {sum(results.values()):,} rows")


if __name__ == "__main__":
    run_ingestion()
