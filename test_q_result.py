import requests, json, os
from dotenv import load_dotenv
load_dotenv()

API_KEY = os.environ["SECTOR_API_KEY"]
h = {'Authorization': API_KEY}
BASE = 'https://api.sectors.app/v2'

# Fetch all oil-gas-coal companies via NL query
all_results = []
r = requests.get(BASE + '/companies/', headers=h, params={'q': 'oil gas coal', 'limit': 100}, timeout=15)
d = r.json()
all_results = d.get('results', [])
pagination = d.get('pagination', {})

print('LLM translation:', d.get('llm_translation', ''))
print('Total count:', pagination.get('total_count'))
print('Showing:', pagination.get('showing'))
print()
print('First 15 companies:')
for c in all_results[:15]:
    print(' ', c.get('symbol'), '-', c.get('company_name', ''))
