import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import logging
import os
from pathlib import Path


def drop_tables(cur, conn):
    '''
    This function is used to drop the tables
    from the drop_table_queries list

    arguments: cur-->cursor, conn--> connection to the database
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
     '''
    This function is used to create the required tables
    from the drop_table_queries list

    arguments: cur-->cursor, conn--> connection to the database
    '''
    for query in create_table_queries:
        try:
            cur.execute(query)
            logging.info(f"{query} is created successfully")
        except Exception as e:
            logging.info(f"{query} could not be created: {e}")
        conn.commit()


def main():
    '''
    This function is used to combine the functions above.
    Loads the credentials from the config file, drops
    the tables if they exist and then creates them
    '''
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