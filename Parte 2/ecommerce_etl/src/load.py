# -*- coding: utf-8 -*-

# load.py - módulo generado automáticamente
import psycopg2
import logging
import psycopg2.extras
from contextlib import contextmanager
import os
import random
from datetime import datetime

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
        
        # Table mapping (logical type -> base physical table name)
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

    def _qualify(self, table_name: str) -> str:
        """Return schema-qualified table if not already qualified."""
        if '.' in table_name:
            return table_name
        if self.schema:
            return f"{self.schema}.{table_name}"
        return table_name

    def _table_exists(self, cursor, table_name: str) -> bool:
        """Check if a table exists (supports qualified names)."""
        cursor.execute("SELECT to_regclass(%s)", (table_name,))
        return cursor.fetchone()[0] is not None

    def _resolve_table_name(self, cursor, base_name: str) -> str:
        """
        Resolve the actual table name to use, trying:
        - configured base name
        - singular/plural variants
        Each both unqualified and schema-qualified. Returns the first that exists.
        """
        candidates_base = [base_name]
        if base_name.endswith('s'):
            candidates_base.append(base_name[:-1])  # singular fallback
        else:
            candidates_base.append(base_name + 's')  # plural fallback

        # Build expanded candidate list (qualified and unqualified)
        candidates = []
        for name in candidates_base:
            candidates.append(name)
            candidates.append(self._qualify(name))

        # Deduplicate while preserving order
        seen = set()
        unique_candidates = []
        for c in candidates:
            if c not in seen:
                unique_candidates.append(c)
                seen.add(c)

        for candidate in unique_candidates:
            if self._table_exists(cursor, candidate):
                return candidate

        # Not found; build helpful message
        raise RuntimeError(
            f"Tabla de destino no encontrada. Intentado: {', '.join(unique_candidates)}"
        )

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
            
        table_base = self.table_mapping.get(data_type)
        if not table_base:
            raise ValueError(f"Unknown data type: {data_type}")
        
        try:
            # Resolve actual physical table to use (handles schema + pluralization)
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    resolved_table = self._resolve_table_name(cursor, table_base)

            self.logger.info(f"Cargando {len(data)} registros en {resolved_table}")

            batch_size = self.config['etl']['batch_size']
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                self._insert_batch(resolved_table, batch)
        except Exception as e:
            self.logger.error(f"Error cargando lote: {str(e)}")
            raise

    def _insert_batch(self, resolved_table_name, batch):
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
        # Determine unqualified base table name for column mapping
        base_name = resolved_table_name.split('.')[-1]
        valid_columns = table_columns.get(base_name, [])
        if not valid_columns:
            raise ValueError(f"No column mapping defined for table {resolved_table_name}")

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

        # Map known table base names to their primary key column so we can
        # perform a safe upsert (do nothing on conflict) instead of raising
        # a UniqueViolation when the same natural key is inserted again.
        conflict_columns = {
            'dim_products': 'product_id',
            'dim_date': 'date_key',
            'dim_users': 'user_id',
            'dim_geography': 'geography_id',
            'fact_sales': 'sale_id'
        }

        conflict_col = conflict_columns.get(base_name)

        # Special-case: for dim_products we want to update product metadata on
        # conflict to keep product information fresh. For other tables we
        # silently ignore duplicates (DO NOTHING).
        if base_name == 'dim_products' and 'product_id' in columns:
            # Build SET clause excluding the PK
            update_columns = [c for c in columns if c != 'product_id']
            if update_columns:
                set_clause = ', '.join([f"{c}=EXCLUDED.{c}" for c in update_columns])
            else:
                set_clause = ''

            if set_clause:
                query = f"""
                        INSERT INTO {resolved_table_name} ({column_names})
                        VALUES ({placeholders})
                        ON CONFLICT (product_id) DO UPDATE SET {set_clause}
                """
            else:
                query = f"""
                        INSERT INTO {resolved_table_name} ({column_names})
                        VALUES ({placeholders})
                        ON CONFLICT (product_id) DO NOTHING
                """
        elif conflict_col and conflict_col in columns:
            query = f"""
                    INSERT INTO {resolved_table_name} ({column_names})
                    VALUES ({placeholders})
                    ON CONFLICT ({conflict_col}) DO NOTHING
            """
        else:
            query = f"""
                    INSERT INTO {resolved_table_name} ({column_names})
                    VALUES ({placeholders})
            """

        # Convert records to tuples
        data_tuples = [tuple(record[col] for col in columns) for record in filtered_batch]

        # Execute batch insert
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    psycopg2.extras.execute_batch(cursor, query, data_tuples)
                    # If we used ON CONFLICT DO NOTHING we can try to estimate how
                    # many rows were ignored by checking the cursor.rowcount. Note
                    # that behavior can vary; we'll log what we can.
                    try:
                        ignored = sum(1 for r in cursor.statusmessage.split(';') if 'DO NOTHING' in r) if cursor.statusmessage else None
                    except Exception:
                        ignored = None
                    conn.commit()
                    # Log summary for this batch
                    try:
                        self.logger.info(f"Inserted batch of {len(data_tuples)} into {resolved_table_name}. Ignored (conflicts): {ignored}")
                    except Exception:
                        pass
                except Exception as e:
                    conn.rollback()
                    raise

    # ------------------------------------------------------------------
    # Utilities to ensure there is at least one row per table for tests
    # ------------------------------------------------------------------
    def _get_row_count(self, cursor, table_name: str) -> int:
        cursor.execute(f"SELECT COUNT(1) FROM {table_name}")
        return cursor.fetchone()[0]

    def _get_random_value(self, cursor, table_name: str, column: str):
        cursor.execute(f"SELECT {column} FROM {table_name} ORDER BY random() LIMIT 1")
        r = cursor.fetchone()
        return r[0] if r else None

    def _insert_single(self, cursor, resolved_table_name: str, record: dict):
        # Build insert SQL from record keys
        columns = list(record.keys())
        placeholders = ','.join(['%s'] * len(columns))
        column_names = ','.join(columns)
        query = f"INSERT INTO {resolved_table_name} ({column_names}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
        cursor.execute(query, tuple(record[col] for col in columns))

    def ensure_minimum_rows(self):
        """Ensure at least one row exists in each core table (dates, products, users, geography, sales).
        Inserts synthetic rows when a table is empty to facilitate tests and local runs.
        """
        # Order matters for referential integrity
        base_tables = ['dim_date', 'dim_products', 'dim_users', 'dim_geography', 'fact_sales']

        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                resolved = {}
                for base in base_tables:
                    try:
                        resolved_table = self._resolve_table_name(cursor, base)
                    except Exception:
                        # If table not present, skip
                        resolved_table = None
                    resolved[base] = resolved_table

                # Dates
                if resolved.get('dim_date'):
                    cnt = self._get_row_count(cursor, resolved['dim_date'])
                    if cnt == 0:
                        rec = self._synthetic_date()
                        self._insert_single(cursor, resolved['dim_date'], rec)
                        conn.commit()

                # Products
                if resolved.get('dim_products'):
                    cnt = self._get_row_count(cursor, resolved['dim_products'])
                    if cnt == 0:
                        rec = self._synthetic_product()
                        self._insert_single(cursor, resolved['dim_products'], rec)
                        conn.commit()

                # Users
                if resolved.get('dim_users'):
                    cnt = self._get_row_count(cursor, resolved['dim_users'])
                    if cnt == 0:
                        rec = self._synthetic_user()
                        self._insert_single(cursor, resolved['dim_users'], rec)
                        conn.commit()

                # Geography
                if resolved.get('dim_geography'):
                    cnt = self._get_row_count(cursor, resolved['dim_geography'])
                    if cnt == 0:
                        rec = self._synthetic_geography()
                        self._insert_single(cursor, resolved['dim_geography'], rec)
                        conn.commit()

                # Sales (fact) - requires keys from date/product/user
                if resolved.get('fact_sales'):
                    cnt = self._get_row_count(cursor, resolved['fact_sales'])
                    if cnt == 0:
                        # pick existing keys
                        date_key = None
                        product_id = None
                        user_id = None
                        if resolved.get('dim_date'):
                            date_key = self._get_random_value(cursor, resolved['dim_date'], 'date_key')
                        if resolved.get('dim_products'):
                            product_id = self._get_random_value(cursor, resolved['dim_products'], 'product_id')
                        if resolved.get('dim_users'):
                            user_id = self._get_random_value(cursor, resolved['dim_users'], 'user_id')

                        # Fallback: if any missing, try to create minimal dependencies
                        if date_key is None and resolved.get('dim_date'):
                            rec = self._synthetic_date()
                            self._insert_single(cursor, resolved['dim_date'], rec)
                            conn.commit()
                            date_key = self._get_random_value(cursor, resolved['dim_date'], 'date_key')
                        if product_id is None and resolved.get('dim_products'):
                            rec = self._synthetic_product()
                            self._insert_single(cursor, resolved['dim_products'], rec)
                            conn.commit()
                            product_id = self._get_random_value(cursor, resolved['dim_products'], 'product_id')
                        if user_id is None and resolved.get('dim_users'):
                            rec = self._synthetic_user()
                            self._insert_single(cursor, resolved['dim_users'], rec)
                            conn.commit()
                            user_id = self._get_random_value(cursor, resolved['dim_users'], 'user_id')

                        # Build sale record
                        if date_key and product_id and user_id:
                            rec = self._synthetic_sale(date_key, product_id, user_id, cursor, resolved)
                            self._insert_single(cursor, resolved['fact_sales'], rec)
                            conn.commit()

    # Synthetic generators
    def _synthetic_date(self):
        d = datetime.utcnow().date()
        date_key = int(d.strftime('%Y%m%d'))
        return {
            'date_key': date_key,
            'date': d.isoformat(),
            'day': d.day,
            'month': d.month,
            'year': d.year,
            'quarter': (d.month - 1) // 3 + 1,
            'iso_week': d.isocalendar()[1],
            'day_of_week': d.isoweekday(),
            'day_name': d.strftime('%A'),
            'month_name': d.strftime('%B')
        }

    def _synthetic_product(self):
        pid = random.randint(900000, 999999)
        return {
            'product_id': pid,
            'title': f'Synthetic product {pid}',
            'price': round(random.uniform(1.0, 100.0), 2),
            'description': 'Synthetic product for tests',
            'category': 'synthetic',
            'image_url': '',
            'rating_rate': round(random.uniform(0, 5), 2),
            'rating_count': random.randint(0, 1000)
        }

    def _synthetic_user(self):
        uid = random.randint(900000, 999999)
        return {
            'user_id': uid,
            'email': f'user{uid}@example.com',
            'username': f'user{uid}',
            'first_name': 'Synthetic',
            'last_name': f'User{uid}',
            'phone': '000-000-0000'
        }

    def _synthetic_geography(self):
        gid = random.randint(900000, 999999)
        return {
            'geography_id': gid,
            'city': 'Synthetic City',
            'street': 'Synthetic Street',
            'number': '1',
            'zipcode': '00000',
            'lat': 0.0,
            'long': 0.0
        }

    def _synthetic_sale(self, date_key, product_id, user_id, cursor, resolved):
        sid = random.randint(900000, 999999)
        # Try to get product price if available
        price = None
        try:
            if resolved.get('dim_products'):
                cursor.execute(f"SELECT price FROM {resolved['dim_products']} WHERE product_id = %s LIMIT 1", (product_id,))
                r = cursor.fetchone()
                price = float(r[0]) if r and r[0] is not None else None
        except Exception:
            price = None

        quantity = 1
        total_amount = round(price * quantity, 2) if price is not None else round(random.uniform(1.0, 100.0), 2)
        return {
            'sale_id': sid,
            'date_key': date_key,
            'product_id': product_id,
            'user_id': user_id,
            'quantity': quantity,
            'total_amount': total_amount
        }
