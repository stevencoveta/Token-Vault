
import pandas as pd
import psycopg2
from psycopg2 import pool

# Adjust the connection parameters based on your PostgreSQL setup
dsn = 'dbname=postgres user=postgres password=napcat host=localhost port=5432'

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
        print('done creating table ...')

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
                cur.execute(query, (row['symbol'], row['apy'], row['protocol'], row['chain'], row['last_updated']))
            conn.commit()

        pool.putconn(conn)

def main(df):
    connection_pool = psycopg2.pool.SimpleConnectionPool(1, 10, dsn)
    insert_dataframe(connection_pool, df)


