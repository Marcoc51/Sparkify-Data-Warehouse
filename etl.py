import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    1. Load data from json files that are located in AWS s3 buckets.
    2. Copy that data into staging area in AWS redshift using the queries in `copy_table_queries` list.
    
    INPUTS: 
    * cur: the cursor variable
    * conn: the connection variable
    """
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except:
            error_query = """
            SELECT *
            FROM stl_load_errors
            WHERE tablename = 'staging_events';"""
            cur.execute(error_query)


def insert_tables(cur, conn):
    """
    1. Load data from staging tables.
    2. insert that data into fact and dimensions tables in AWS redshift using the queries in `insert_table_queries` list.
    
    INPUTS: 
    * cur: the cursor variable
    * conn: the connection variable
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
     """
    - Establishes a config instance to read the configuration file to connect to AWS redshift cluster.
    
    - Establishes connection with the sparkify database and gets cursor to it.  
    
    - Load data from json files that are located in AWS s3 buckets into staging area in AWS redshift.  
    
    - Load data from staging tables that are located in AWS redshift into fact and dimensions tables in AWS redshift.
    
    - Finally, closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()