import psycopg2
import pandas as pd
from sql_queries import analysis_queries

def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=huiren password=1234")
    cur = conn.cursor()

    for query in analysis_queries:
        table = pd.read_sql(query, conn)
        print(table)
        
    conn.close()

if __name__ == "__main__":
    main()
