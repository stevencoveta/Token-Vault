from aave_ingestor import * 
from allbridge_ingestor import * 
from db_ingestor import * 


if __name__ == '__main__':
    chains = ['protocol-v3','protocol-v2','aave-v2-matic','protocol-v2-avalanche','protocol-v3-arbitrum','protocol-v3-optimism','protocol-v3-polygon','protocol-v3-avalanche']
    aave_results = {chain: main(transform_results(aave_fetch_apy(chain), chain)) for chain in chains}
    allbridge_results = main(all_bridge_fetch_apy())
