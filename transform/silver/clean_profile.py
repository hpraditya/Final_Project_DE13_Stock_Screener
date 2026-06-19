import io
import os

import boto3
import duckdb
import pandas as pd


def _duckdb_endpoint(s3_endpoint: str) -> str:
    """DuckDB s3_endpoint expects host:port without scheme."""
    return s3_endpoint.replace("https://", "").replace("http://", "")


def _s3_client(s3_endpoint: str):
    return boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=os.environ["SEAWEEDFS_ACCESS_KEY"],
        aws_secret_access_key=os.environ["SEAWEEDFS_SECRET_KEY"],
        region_name="us-east-1",
    )


def clean_profile(run_date: str, s3_endpoint: str) -> pd.DataFrame:
    """Read bronze company_profile, standardize ticker and sector, write to silver."""
    access_key = os.environ["SEAWEEDFS_ACCESS_KEY"]
    secret_key = os.environ["SEAWEEDFS_SECRET_KEY"]
    bucket = os.environ["SEAWEEDFS_BUCKET"]

    con = duckdb.connect()
    con.execute(f"""
        INSTALL httpfs; LOAD httpfs;
        SET s3_endpoint='{_duckdb_endpoint(s3_endpoint)}';
        SET s3_access_key_id='{access_key}';
        SET s3_secret_access_key='{secret_key}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)

    df = con.execute(f"""
        SELECT
            UPPER(REPLACE(ticker, '.jk', '')) AS ticker,
            company_name,
            sector,
            sub_sector,
            CAST(market_cap AS DOUBLE) AS market_cap,
            listing_date
        FROM read_parquet(
            's3://{bucket}/bronze/company_profile/run_date={run_date}/data.parquet'
        )
    """).df()
    con.close()

    df = df.reset_index(drop=True)
    df["run_date"] = run_date

    # Write to silver layer
    key = f"silver/company_profile/run_date={run_date}/data.parquet"
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    s3 = _s3_client(s3_endpoint)
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.read())

    return df
