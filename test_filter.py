import requests, json, os
from dotenv import load_dotenv
load_dotenv()

API_KEY = os.environ["SECTOR_API_KEY"]
h = {'Authorization': API_KEY}
BASE = 'https://api.sectors.app/v2'

tests = [
    {'subsector': 'oil-gas-coal'},
    {'industry': 'oil-gas'},
    {'industry': 'coal'},
    {'industry': 'oil-gas-coal-supports'},
]

for param in tests:
    r = requests.get(BASE + '/companies/', headers=h, params={**param, 'limit': 5}, timeout=10)
    d = r.json()
    if r.status_code == 200:
        count = d.get('pagination', {}).get('total_count', '?')
        sample = [x.get('symbol') for x in d.get('results', [])[:5]]
        print(str(r.status_code) + ' ' + str(param) + ' count=' + str(count) + ' ' + str(sample))
    else:
        msg = d.get('message', d.get('error', ''))
        print(str(r.status_code) + ' ' + str(param) + ' => ' + str(msg))
