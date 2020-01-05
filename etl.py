import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ Run queries to load staging tables in Redshift cluster with data from S3 files.

    Args:
        cur(psycopg2.cursor): Cursor object from Psycopg2's connection.
        conn(psycopg2.connection): Connection object of Psycopg2.
    Returns: None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ Run queries to insert data from staging tables to analytical tables in the Redshift cluster.

    Args:
        cur(psycopg2.cursor): Cursor object from Psycopg2's connection.
        conn(psycopg2.connection): Connection object of Psycopg2.
    Returns: None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ This script's main function.
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