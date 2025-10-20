# -*- coding: utf-8 -*-

# load.py - módulo generado automáticamente
import psycopg2
import logging
from typing import Dict, List, Any
import psycopg2.extras
from contextlib import contextmanager
import os

class DataLoader:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.db_config = self._get_db_config()
    
    def _get_db_config(self) -> Dict[str, str]:
        """Obtiene configuración de base de datos."""
        return {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'fakestore_dw'),
            'user': os.getenv('DB_USER', 'etl_user'),
            'password': os.getenv('DB_PASSWORD', 'etl_password'),
            'schema': os.getenv('DB_SCHEMA', 'public')
        }
    
    @contextmanager
    def _get_connection(self):
        """Context manager para conexión a base de datos."""
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
    
    def load_data(self, data_type: str, data: List[Dict]):
        """Carga datos a la base de datos según el tipo."""
        batch_size = self.config['etl']['batch_size']
        
        if not data:
            self.logger.warning(f"No hay datos para cargar de tipo {data_type}")
            return
        
        # Mapeo de tipos de datos a tablas
        table_mapping = {
            'products': 'dim_product',
            'users': 'dim_user',
            'geography': 'dim_geography',
            'dates': 'dim_date',
            'sales': 'fact_sales'
        }
        
        table_name = table_mapping.get(data_type)
        if not table_name:
            raise ValueError(f"Tipo de datos no soportado: {data_type}")
        
        self.logger.info(f"Cargando {len(data)} registros en {table_name}")
        
        # Cargar en lotes
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            try:
                self._insert_batch(table_name, batch)
                self.logger.debug(f"Lote {i//batch_size + 1} cargado exitosamente")
            except Exception as e:
                self.logger.error(f"Error cargando lote {i//batch_size + 1}: {str(e)}")
                raise
    
    def _insert_batch(self, table_name: str, batch: List[Dict]):
        """Inserta un lote de datos en la tabla especificada."""
        if not batch:
            return
        
        # Obtener columnas del primer registro
        columns = list(batch[0].keys())
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        
        query = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT DO NOTHING
        """
        
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                # Preparar datos para inserción
                data_tuples = [tuple(record[col] for col in columns) for record in batch]
                
                psycopg2.extras.execute_batch(cursor, query, data_tuples)
                conn.commit()
                
            self.logger.debug(f"Insertados {len(batch)} registros en {table_name}")
    
    def truncate_table(self, table_name: str):
        """Trunca una tabla (útil para ETL full refresh)."""
        query = f"TRUNCATE TABLE {table_name} CASCADE"
        
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                conn.commit()
        
        self.logger.info(f"Tabla {table_name} truncada")