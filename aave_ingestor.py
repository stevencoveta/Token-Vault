import requests
import pandas as pd
from datetime import datetime
import pytz

# Fetch URL from 
def fetch_apy(chain):
    url = f'https://api.thegraph.com/subgraphs/name/aave/{chain}'

    headers = {
        'authority': 'api.thegraph.com',
        'accept': '*/*',
        'accept-language': 'en-GB,en;q=0.9,en-US;q=0.8,es;q=0.7,fr;q=0.6',
        'content-type': 'application/json',
        'origin': 'https://aavescan.com',
        'referer': 'https://aavescan.com/',
        'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }

    data = {
        'variables': {},
        'query': '{ reserves(first: 100) { id name decimals symbol liquidityRate variableBorrowRate stableBorrowRate totalLiquidity utilizationRate availableLiquidity liquidityIndex totalCurrentVariableDebt price { priceInEth __typename } __typename } }'
    }

    response = requests.post(url, headers=headers, json=data)
    
    if response.status_code != 200:
        print(f'Error {response.status_code}')
    else: 
        return response.json()


def transform_results(df, chain):
    df = pd.DataFrame(df['data']['reserves'])
    
    df.index = df.symbol
    df['apy'] = (df['liquidityRate'].astype(float) / 10 ** 27) * 100
    df = df.reset_index(drop=True)
    
    dff = df[['symbol', 'apy']].copy()
    dff['protocol'] = 'AAVE'
    
    if chain == 'protocol-v3':
        chain = 'ethereum-v3'
    if chain == 'protocol-v2':
        chain = 'ethereum-v2'
    
    dff['chain'] = chain
    dff['last_updated'] = datetime.now(pytz.utc)
    
    return dff



chains = ['protocol-v3','protocol-v2','aave-v2-matic','protocol-v2-avalanche','protocol-v3-arbitrum','protocol-v3-optimism','protocol-v3-polygon','protocol-v3-avalanche']