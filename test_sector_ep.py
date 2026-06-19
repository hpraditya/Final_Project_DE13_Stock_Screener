import requests, json, os
from dotenv import load_dotenv
load_dotenv()

API_KEY = os.environ["SECTOR_API_KEY"]
h = {'Authorization': API_KEY}
BASE = 'https://api.sectors.app/v2'

candidates = [
    '/subsectors/oil-gas-coal/',
    '/subsectors/oil-gas-coal/companies/',
    '/industries/oil-gas-coal/',
    '/industries/oil-gas-coal/companies/',
    '/industry/oil-gas-coal/companies/',
    '/companies/subsector/oil-gas-coal/',
    '/sector/energy-companies/',
    '/companies/?classification=oil-gas-coal',
    '/companies/?q=oil+gas+coal',
    '/subsector/oil-gas-coal/companies/',
]

for ep in candidates:
    r = requests.get(BASE + ep, headers=h, timeout=10)
    d = r.json()
    if r.status_code == 200:
        if isinstance(d, list):
            print('200 list[' + str(len(d)) + ']  ' + ep + '  sample=' + str(d[:2]))
        else:
            keys = list(d.keys()) if isinstance(d, dict) else str(d)[:80]
            count = d.get('pagination', {}).get('total_count', '?') if isinstance(d, dict) else '?'
            print('200 dict  ' + ep + '  keys=' + str(keys) + ' count=' + str(count))
    else:
        msg = d.get('message', d.get('error', d.get('details', ''))) if isinstance(d, dict) else str(d)[:60]
        print(str(r.status_code) + '  ' + ep + '  ' + str(msg)[:80])
