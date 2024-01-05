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
    print(f'fetching url protocol {chain} ... ')
    response = requests.post(url, headers=headers, json=data)
    
    if response.status_code != 200:
        print(f'Error {response.status_code}')
    else: 
        return response.json()


def transform_results(df, chain):
    print('performing prepro ...')
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
    print('done prepro ...')
    return dff


import pandas as pd
import psycopg2
from psycopg2 import pool

# Adjust the connection parameters based on your PostgreSQL setup
dsn = 'dbname=postgres user=postgres password=napcat host=13.231.138.2 port=5432'

# Function to create the table if it doesn't exist
def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.token_apy (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(50),
                apy FLOAT,
                protocol VARCHAR(50),
                chain VARCHAR(50),
                last_updated TIMESTAMP
            )
        """)
        conn.commit()

# Function to insert trades into the database
def insert_dataframe(pool, dataframe):
    with pool.getconn() as conn:
        create_table(conn)  # Create the table if it doesn't exist

        with conn.cursor() as cur:
            for _, row in dataframe.iterrows():
                query = """
                    INSERT INTO public.token_apy (symbol, apy, protocol, chain, last_updated)
                    VALUES (%s, %s, %s, %s, %s)
                """
                try:
                    cur.execute(query, (row['symbol'], row['apy'], row['protocol'], row['chain'], row['last_updated']))
                except Exception as e:
                    print(f"Error inserting data: {e}")
                    conn.rollback()
                    break
            else:
                conn.commit()

        pool.putconn(conn)

# Example usage
def main():
    # Example DataFrame
    data = {
        'symbol': ['FRAX', 'WBTC', 'MAI', 'wstETH', 'WETH', 'ARB', 'LUSD'],
        'apy': [10.501687, 0.013937, 0.030517, 0.005315, 1.226682, 0.20206, 3.420332],
        'protocol': ['AAVE'] * 7,
        'chain': ['protocol-v3-arbitrum'] * 7,
        'last_updated': ['2024-01-05 15:03:05.295236+00:00'] * 7
    }

    df = pd.DataFrame(data)
    
    connection_pool = psycopg2.pool.SimpleConnectionPool(1, 10, dsn)
    insert_dataframe(connection_pool, df)

if __name__ == '__main__':
    main()
