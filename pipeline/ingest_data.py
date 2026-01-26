#!/usr/bin/env -S uv run python
# coding: utf-8
import click
import pyarrow.parquet as pq
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
from pathlib import Path


@click.command()
@click.option('--year', default=2025, type=int, help='Year of the data to ingest')
@click.option('--month', default=11, type=int, help='Month of the data to ingest')
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-password', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--batch-size', default=10_000, type=int, help='Batch size for processing')
def run(
    year: int,
    month: int,
    pg_user: str,
    pg_password: str,
    pg_host: str,
    pg_port: int,
    pg_db: str,
    batch_size: int
):
    """Ingest green taxi trip data from parquet files into PostgreSQL."""
    # Create the database engine
    engine = create_engine(f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}")

    # Enforce dtypes (nullable-safe)
    dtype_map = {
        "VendorID": "Int64",
        "passenger_count": "Int64",
        "RatecodeID": "Int64",
        "PULocationID": "Int64",
        "DOLocationID": "Int64",
        "payment_type": "Int64",
        "store_and_fwd_flag": "string",
        "trip_distance": "float64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "congestion_surcharge": "float64",
        "cbd_congestion_fee": "float64"
    }

    # Green taxi typically uses lpep_* datetimes
    date_cols = [
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime",
    ]

    DATA_DIR = Path("data/raw")
    parquet_path = DATA_DIR / f"green_tripdata_{year}-{month:02d}.parquet"
    table_name = f"green_taxi_data_{year}_{month:02d}"

    pf = pq.ParquetFile(parquet_path)
    first = True

    with tqdm(total=pf.metadata.num_rows) as pbar:
        for batch in pf.iter_batches(batch_size=batch_size):
            df = batch.to_pandas()
            pbar.update(len(df))
            
            # Datetime coercion (only if the columns exist)
            for c in date_cols:
                if c in df.columns:
                    df[c] = pd.to_datetime(df[c], errors="coerce")

            # Enforce dtypes (only for columns that exist in this parquet)
            for col, dt in dtype_map.items():
                if col not in df.columns:
                    continue

                if dt == "Int64":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                elif dt == "float64":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
                elif dt == "string":
                    df[col] = df[col].astype("string")

            # Create table on first batch (schema only)
            if first:
                df.head(0).to_sql(name=table_name, con=engine, if_exists="replace", index=False)
                first = False
                print(f"Table created: {table_name}")

            # Append batch
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=batch_size
            )

            print(f"Inserted: {len(df)} rows")

    print(f"Data ingestion complete for {year}-{month:02d}")


if __name__ == "__main__":
    run()