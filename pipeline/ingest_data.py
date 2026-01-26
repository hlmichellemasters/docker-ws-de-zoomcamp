#!/usr/bin/env python
# coding: utf-8
import pyarrow.parquet as pq
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
from pathlib import Path


def run(
    year: int = 2025,
    month: int = 11,
    pg_user: str = "root",
    pg_password: str = "root",
    pg_host: str = "localhost",
    pg_port: int = 5432,
    pg_db: str = "ny_taxi",
    batch_size: int = 10_000
):
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
    table_name = "green_taxi_data"

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