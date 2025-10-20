import os
import psycopg2
from dotenv import load_dotenv
from utils import load_config

def init_database(config):
    """Initialize database with required tables"""
    # Database connection parameters
    db_params = {
        'host': config['database']['host'],
        'port': config['database']['port'],
        'database': config['database']['database'],
        'user': config['database']['user'],
        'password': config['database']['password']
    }
    
    # Read SQL file
    script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sql_file = os.path.join(script_dir, 'sql', 'create_tables.sql')
    print(f"Loading SQL script from: {sql_file}")
    
    with open(sql_file, 'r') as f:
        sql_script = f.read()
    
    # Execute SQL script
    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_script)
        print("Database initialized successfully!")
    except Exception as e:
        print(f"Error initializing database: {str(e)}")
        raise

    # Optionally execute views/indexes SQL if present
    views_sql = os.path.join(script_dir, 'sql', 'create_views.sql')
    if os.path.exists(views_sql):
        try:
            with open(views_sql, 'r') as f:
                views_script = f.read()
            with psycopg2.connect(**db_params) as conn:
                with conn.cursor() as cur:
                    cur.execute(views_script)
            print("Views and indexes created/refreshed successfully!")
        except Exception as e:
            print(f"Error creating views/indexes: {str(e)}")
            raise

if __name__ == "__main__":
    load_dotenv()
    # Get absolute path to config file
    script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(script_dir, 'config', 'config.yaml')
    print(f"Loading config from: {config_path}")
    config = load_config(config_path)
    init_database(config)