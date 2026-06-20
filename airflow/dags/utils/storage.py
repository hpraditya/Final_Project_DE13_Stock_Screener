import os
import io
import pandas as pd
import boto3
from botocore.exceptions import ClientError

ENDPOINT   = os.environ["SEAWEEDFS_ENDPOINT"]
ACCESS_KEY = os.environ["SEAWEEDFS_ACCESS_KEY"]
SECRET_KEY = os.environ["SEAWEEDFS_SECRET_KEY"]
BUCKET     = os.environ["SEAWEEDFS_BUCKET"]


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
    )


def _ensure_bucket(client) -> None:
    try:
        client.head_bucket(Bucket=BUCKET)
    except Exception:
        client.create_bucket(Bucket=BUCKET)


def write_parquet_to_bronze(df: pd.DataFrame, domain: str, run_date: str) -> None:
    key = f"bronze/{domain}/run_date={run_date}/data.parquet"
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    client = _s3_client()
    _ensure_bucket(client)
    client.put_object(Bucket=BUCKET, Key=key, Body=buf.read())


def read_parquet_from_bronze(domain: str, run_date: str) -> pd.DataFrame:
    key = f"bronze/{domain}/run_date={run_date}/data.parquet"
    obj = _s3_client().get_object(Bucket=BUCKET, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def write_parquet_to_silver(df: pd.DataFrame, domain: str, run_date: str) -> None:
    key = f"silver/{domain}/run_date={run_date}/data.parquet"
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    client = _s3_client()
    _ensure_bucket(client)
    client.put_object(Bucket=BUCKET, Key=key, Body=buf.read())


def read_parquet_from_silver(domain: str, run_date: str) -> pd.DataFrame:
    key = f"silver/{domain}/run_date={run_date}/data.parquet"
    obj = _s3_client().get_object(Bucket=BUCKET, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def has_bronze_data(domain: str) -> bool:
    """Return True if any Parquet partition exists under bronze/{domain}/ in the data lake.

    Returns False (not raises) when the bucket does not exist yet — this happens on
    the very first pipeline run before any data has been written.
    """
    prefix = f"bronze/{domain}/"
    try:
        resp = _s3_client().list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=1)
        return resp.get("KeyCount", 0) > 0
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchBucket", "404"):
            return False
        raise
