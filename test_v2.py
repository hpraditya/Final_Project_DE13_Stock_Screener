import sys, json, os
from dotenv import load_dotenv
load_dotenv()
sys.path.insert(0, '/opt/airflow')
from extraction.client import SectorClient
import requests

API_KEY = os.environ["SECTOR_API_KEY"]
client = SectorClient(api_key=API_KEY)

print("=== /v2/companies/ (first 5) ===")

r = requests.get('https://api.sectors.app/v2/companies/',
                 headers={'Authorization': API_KEY},
                 params={'limit': 5}, timeout=10)
data = r.json()
for item in data['results']:
    print(' ', item['symbol'])
total = data['pagination']['total_count']
print('  total:', total)

print()
print("=== company/report/BBCA/?sections=overview ===")
report = client.fetch_company_report('BBCA', 'overview')
rows = client._flatten_response('BBCA', 'overview', report)
print(json.dumps(rows[0], indent=2, default=str))

print()
print("=== financial_statements flatten (latest year) ===")
report_fin = client.fetch_company_report('BBCA', 'financials')
rows_fin = client._flatten_response('BBCA', 'financials', report_fin)
print(json.dumps(rows_fin[-1], indent=2, default=str))

print()
print("=== valuation_ratios flatten (latest year) ===")
report_val = client.fetch_company_report('BBCA', 'valuation')
rows_val = client._flatten_response('BBCA', 'valuation', report_val)
print(json.dumps(rows_val[-1], indent=2, default=str))

print()
print("=== daily/BBCA/ (first row) ===")
r2 = requests.get('https://api.sectors.app/v2/daily/BBCA/',
                  headers={'Authorization': API_KEY}, timeout=10)
print(json.dumps(r2.json()[0] if r2.json() else {}, indent=2, default=str))

print()
print("ALL CHECKS PASSED")
