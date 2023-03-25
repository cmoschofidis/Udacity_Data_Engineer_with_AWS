import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import logging
import os
from pathlib import Path


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        try:
            cur.execute(query)
            logging.info(f"{query} is created successfully")
        except Exception as e:
            logging.info(f"{query} could not be created: {e}")
        conn.commit()


def main():
    path = Path(__file__)
    ROOT_DIR = path.parent.absolute()
    config_path = os.path.join(ROOT_DIR, "dwh.cfg")
    config = configparser.ConfigParser()
    config.read(config_path)

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DB'].values()))
        logging.info('Connection to database successful')
    except Exception as e:
        logging.info(f"Could not establish connection: {e}")
    try:
        cur = conn.cursor()
        logging.info('Succesfully connected the cursor')
    except Exception as e:
        logging.info(f"Could not connect to: {e}")

    # Dropping tables before recreating them
    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()