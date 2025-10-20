# -*- coding: utf-8 -*-

# load.py - módulo generado automáticamente
import psycopg2
import logging
import psycopg2.extras
from contextlib import contextmanager
import os

class DataLoader:
    def __init__(self, config):
        """Inicializa el cargador de datos."""
        # Store complete config
        self.config = config
        
        # Database connection config
        self.db_config = {
            'host': config['database']['host'],
            'port': config['database']['port'],
            'database': config['database']['database'],
            'user': config['database']['user'],
            'password': config['database']['password']
        }
        
        # Table mapping
        self.table_mapping = {
            'dates': 'dim_date',
            'products': 'dim_products',
            'users': 'dim_users',
            'geography': 'dim_geography',
            'sales': 'fact_sales'
        }
        
        self.schema = config['database'].get('target_schema', 'public')
        self.batch_size = config['etl']['batch_size']
        self.logger = logging.getLogger(__name__)

    @contextmanager
    def _get_connection(self):
        """Creates and manages database connection."""
        conn = None
        try:
            conn = psycopg2.connect(**self.db_config)
            yield conn
        except Exception as e:
            self.logger.error(f"Error de conexión: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()

    def load_data(self, data_type, data):
        """Carga datos en la tabla correspondiente."""
        if not data:
            return
            
        table_name = self.table_mapping.get(data_type)
        if not table_name:
            raise ValueError(f"Unknown data type: {data_type}")
            
        self.logger.info(f"Cargando {len(data)} registros en {table_name}")
        
        try:
            batch_size = self.config['etl']['batch_size']
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                self._insert_batch(table_name, batch)
        except Exception as e:
            self.logger.error(f"Error cargando lote: {str(e)}")
            raise

    def _insert_batch(self, table_name, batch):
        """Inserta un lote de registros en la tabla especificada."""
        if not batch:
            return

        # Column mappings for each table
        table_columns = {
            'dim_products': ['product_id', 'title', 'price', 'description', 'category', 'image_url', 'rating_rate', 'rating_count'],
            'dim_date': ['date_key', 'date', 'day', 'month', 'year', 'quarter', 'iso_week', 'day_of_week', 'day_name', 'month_name'],
            'dim_users': ['user_id', 'email', 'username', 'first_name', 'last_name', 'phone'],
            'dim_geography': ['geography_id', 'city', 'street', 'number', 'zipcode', 'lat', 'long'],
            'fact_sales': ['sale_id', 'date_key', 'product_id', 'user_id', 'quantity', 'total_amount']
        }

        # Get valid columns for this table
        valid_columns = table_columns.get(table_name, [])
        if not valid_columns:
            raise ValueError(f"No column mapping defined for table {table_name}")

        # Filter data to only include valid columns
        filtered_batch = []
        for record in batch:
            filtered_record = {k: v for k, v in record.items() if k in valid_columns}
            filtered_batch.append(filtered_record)

        # Get columns from filtered record
        columns = list(filtered_batch[0].keys())
        
        # Build query
        placeholders = ','.join(['%s'] * len(columns))
        column_names = ','.join(columns)
        query = f"""
                INSERT INTO {table_name} ({column_names})
                VALUES ({placeholders})
        """
        
        # Convert records to tuples
        data_tuples = [tuple(record[col] for col in columns) for record in filtered_batch]
        
        # Execute batch insert
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    psycopg2.extras.execute_batch(cursor, query, data_tuples)
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    raise