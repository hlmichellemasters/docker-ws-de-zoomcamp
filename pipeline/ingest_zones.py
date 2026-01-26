#!/usr/bin/env python
# filepath: /workspaces/docker-ws-de-zoomcamp/pipeline/ingest_zones.py
# coding: utf-8
import click
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-password', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
def run(
    pg_user: str,
    pg_password: str,
    pg_host: str,
    pg_port: int,
    pg_db: str
):
    """Ingest taxi zone lookup data from CSV into PostgreSQL."""
    # Create the database engine
    engine = create_engine(f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}")

    # Read CSV file
    DATA_DIR = Path("data/raw")
    csv_path = DATA_DIR / "taxi_zone_lookup.csv"
    
    print(f"Reading {csv_path}...")
    df = pd.read_csv(csv_path)
    
    # Clean column names (lowercase, remove spaces)
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    
    table_name = "taxi_zones"
    
    # Load to database (replace if exists)
    print(f"Loading to table: {table_name}")
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False
    )
    
    print(f"Successfully loaded {len(df)} zones into {table_name}")
    print(f"\nSample data:")
    print(df.head())


if __name__ == "__main__":
    run()