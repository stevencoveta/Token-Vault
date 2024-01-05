import psycopg2
from psycopg2 import pool

# Adjust the connection parameters based on your PostgreSQL setup
dsn = 'dbname=postgres user=postgres password=napcat host=localhost port=5432'

def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.token_apy (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(50),
                protocol VARCHAR(50),
                chain VARCHAR(50),
                apy FLOAT,
                last_updated TIMESTAMP
            )
        """)
        conn.commit()

# Synchronous function to insert trades into the database
def insert_trade(pool, symbol, protocol, chain, apy, last_updated):
    with pool.getconn() as conn:
        create_table(conn)  # Create the table if it doesn't exist

        with conn.cursor() as cur:
            query = """
                INSERT INTO public.token_apy (symbol, protocol, chain, apy, last_updated)
                VALUES (%s, %s, %s, %s, %s)
            """
            cur.execute(query, (symbol, protocol, chain, apy, last_updated))
            conn.commit()

        pool.putconn(conn)

# Example usage
def main():
    connection_pool = psycopg2.pool.SimpleConnectionPool(1, 10, dsn)
    insert_trade(connection_pool, 'symbol', 'AAVE', 'ethereum-v3', 0.5, '2024-01-05 15:03:05.295236+00:00')

if __name__ == '__main__':
    main()
