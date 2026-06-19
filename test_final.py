import sys, json, io, os
from dotenv import load_dotenv
load_dotenv()
sys.path.insert(0, '/opt/airflow')
from extraction.client import SectorClient
import boto3, pandas as pd

API_KEY = os.environ["SECTOR_API_KEY"]
client = SectorClient(api_key=API_KEY)
RUN_DATE = '2026-06-18'

# 1. Ticker list - oil/gas/coal only
print("=== Fetching oil/gas/coal tickers ===")
tickers = client._fetch_ticker_list()
print("Total tickers:", len(tickers))
print("Sample:", tickers[:10])

# 2. Fetch company_profile for one ticker
print()
print("=== company_profile BYAN ===")
raw = client.fetch_company_report('BYAN', 'overview')
rows = client._flatten_response('BYAN', 'overview', raw, cutoff_year=2025)
print(json.dumps(rows[0], indent=2, default=str))

# 3. Fetch financials - 1 year back
print()
print("=== financial_statements BYAN (cutoff 2025) ===")
raw_fin = client.fetch_company_report('BYAN', 'financials')
rows_fin = client._flatten_response('BYAN', 'financials', raw_fin, cutoff_year=2025)
print("Periods:", [r['period'] for r in rows_fin])
if rows_fin:
    print(json.dumps(rows_fin[-1], indent=2, default=str))

# 4. Fetch valuation - 1 year back
print()
print("=== valuation_ratios BYAN (cutoff 2025) ===")
raw_val = client.fetch_company_report('BYAN', 'valuation')
rows_val = client._flatten_response('BYAN', 'valuation', raw_val, cutoff_year=2025)
print("Periods:", [r['period'] for r in rows_val])
if rows_val:
    print(json.dumps(rows_val[-1], indent=2, default=str))

# 5. Test SeaweedFS write
print()
print("=== SeaweedFS write test ===")
bucket = os.environ["SEAWEEDFS_BUCKET"]
s3 = boto3.client(
    's3',
    endpoint_url=os.environ["SEAWEEDFS_ENDPOINT"],
    aws_access_key_id=os.environ["SEAWEEDFS_ACCESS_KEY"],
    aws_secret_access_key=os.environ["SEAWEEDFS_SECRET_KEY"],
    region_name='us-east-1',
)
try:
    s3.head_bucket(Bucket=bucket)
    print(f"Bucket '{bucket}' exists")
except Exception:
    s3.create_bucket(Bucket=bucket)
    print(f"Bucket '{bucket}' created")

df_test = pd.DataFrame(rows_fin) if rows_fin else pd.DataFrame([{'ticker': 'TEST'}])
buf = io.BytesIO()
df_test.to_parquet(buf, index=False)
buf.seek(0)
s3.put_object(Bucket=bucket, Key='bronze/financial_statements/run_date=2026-06-18/data.parquet', Body=buf.read())
print("Write OK:", len(df_test), "rows")

print()
print("=== ALL FINAL CHECKS PASSED ===")
