import configparser
import psycopg2
import pandas as pd
from sql_queries import analysis_queries

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    for query in analysis_queries:
        table = pd.read_sql(query, conn)
        print(table)

    conn.close()

if __name__ == "__main__":
    main()
