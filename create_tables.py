import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """ Send queries for dropping tables to Redshift cluster.
    
    Args:
        cur(psycopg2.cursor): Cursor object from Psycopg2's connection.
        conn(psycopg2.connection): Connection object of Psycopg2.
    Returns: None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ Send queries for creating tables to Redshift cluster.

    Args:
        cur(psycopg2.cursor): Cursor object from Psycopg2's connection.
        conn(psycopg2.connection): Connection object of Psycopg2.
    Returns: None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ This script's main function.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()