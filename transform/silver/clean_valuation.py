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


def clean_valuation(run_date: str, s3_endpoint: str) -> pd.DataFrame:
    """Read bronze valuation_ratios, normalize nulls, write to silver."""
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
            period,
            CAST(per AS DOUBLE)      AS per,
            CAST(pbv AS DOUBLE)      AS pbv,
            CAST(price AS DOUBLE)    AS price,
            CAST(ev_ebitda AS DOUBLE) AS ev_ebitda
        FROM read_parquet(
            's3://{bucket}/bronze/valuation_ratios/run_date={run_date}/data.parquet'
        )
        WHERE per > 0 AND pbv > 0
    """).df()
    con.close()

    df = df.reset_index(drop=True)
    df["run_date"] = run_date

    # Write to silver layer
    key = f"silver/valuation_ratios/run_date={run_date}/data.parquet"
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
