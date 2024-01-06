import requests
import pandas as pd
from datetime import datetime
import pytz

def all_bridge_fetch_apy():
    try:
        all_chains = []
        data = requests.get('https://core.api.allbridgecoreapi.net/token-info').json()
        chains = ([(k) for k in data.keys()])
        for chain in chains: 
            df = pd.DataFrame(data[chain]['tokens'])[['symbol','apr']]
            df = df.rename(columns = {'apr':'apy'})
            df['apy'] = (df['apy'].astype(float) * 100).round(2)
            df['protocol'] = 'ALLBRIDGE'
            if chain == 'ETH':
                chain = 'ethereum'
            if chain == 'BSC':
                chain = 'bsc'
            if chain == 'POL':
                chain = 'polygon'
            if chain == 'ARB':
                chain = 'arbitrum'
            if chain == 'AVA':
                chain = 'avalanche'
            if chain == 'TRX':
                chain = 'tron'
            if chain == 'SOL':
                chain = 'solana'   
            df['chain'] = chain
            df['last_updated'] = datetime.now(pytz.utc)
            all_chains.append(df)
        return pd.concat(all_chains)
    except Exception as e: 
        print(e)
    

            

