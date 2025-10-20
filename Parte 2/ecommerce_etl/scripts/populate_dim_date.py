"""Script para poblar dim_date usando el SQL ya presente en `sql/populate_dim_date.sql`.

Usage:
    python scripts/populate_dim_date.py --config config/config.yaml --execute

Si se pasa --execute intentará conectarse a la DB y ejecutar el SQL (requiere psycopg2 y credenciales en config).
"""
import argparse
import os
import yaml
import psycopg2


def load_config(path):
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def read_sql(path):
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()


def execute_sql(db_config, sql_text):
    conn = None
    try:
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config.get('port', 5432),
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password']
        )
        cur = conn.cursor()
        cur.execute(sql_text)
        conn.commit()
        cur.close()
        print('SQL ejecutado correctamente')
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='config/config.yaml')
    parser.add_argument('--sql', default='sql/populate_dim_date.sql')
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, '..', args.config)
    sql_path = os.path.join(script_dir, '..', args.sql)

    cfg = load_config(config_path)
    sql_text = read_sql(sql_path)

    if args.execute:
        db_cfg = cfg.get('database')
        if not db_cfg:
            raise SystemExit('No se encontró sección database en config')
        execute_sql(db_cfg, sql_text)
    else:
        print(sql_text[:1000])
        print('\n----\nUso: --execute para ejecutar contra la DB (asegúrate de tener credenciales en config)')
