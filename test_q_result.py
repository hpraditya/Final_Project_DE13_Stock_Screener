import requests, json, os
from dotenv import load_dotenv
load_dotenv()

API_KEY = os.environ["SECTOR_API_KEY"]
h = {'Authorization': API_KEY}
BASE = 'https://api.sectors.app/v2'

# Fetch all Oil, Gas & Coal companies using the where= filter (correct syntax)
# NL query (q=) was abandoned — the LLM translation generates invalid WHERE clauses.
where_clause = 'sub_sector = "Oil, Gas & Coal"'
r = requests.get(
    BASE + '/companies/',
    headers=h,
    params={'where': where_clause, 'order_by': '-market_cap', 'limit': 100},
    timeout=15,
)
d = r.json()
all_results = d.get('results', [])
pagination = d.get('pagination', {})

print('Status:', r.status_code)
print('Total count:', pagination.get('total_count'))
print('Showing:', pagination.get('showing'))
print('Has next:', pagination.get('has_next'))
print()
print(f'First 15 companies (sorted by -market_cap):')
for c in all_results[:15]:
    print(f"  {c.get('symbol', '').ljust(10)} {c.get('company_name', '')}")
