#!/usr/bin/env python3
"""Refresh materialized views used for analytics."""
import os
import psycopg2
from dotenv import load_dotenv
from src.utils import load_config


def refresh_mv(config):
    db_params = {
        'host': config['database']['host'],
        'port': config['database']['port'],
        'database': config['database']['database'],
        'user': config['database']['user'],
        'password': config['database']['password']
    }

    conn = psycopg2.connect(**db_params)
    try:
        with conn.cursor() as cur:
            cur.execute('REFRESH MATERIALIZED VIEW CONCURRENTLY mv_product_performance;')
            conn.commit()
            print('Materialized view refreshed successfully')
    finally:
        conn.close()


if __name__ == '__main__':
    load_dotenv()
    script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(script_dir, 'config', 'config.yaml')
    config = load_config(config_path)
    refresh_mv(config)
