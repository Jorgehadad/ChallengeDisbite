# -*- coding: utf-8 -*-

# main.py - módulo generado automáticamente
import logging
import yaml
import os
import json
from datetime import datetime
from dotenv import load_dotenv
import sys
from collections import defaultdict
from typing import Any, Iterable, List

import pandas as pd

# Ejecutar tests programáticamente
try:
    import pytest  # type: ignore
except Exception:
    pytest = None

from src.extract import APIDataExtractor
from src.transform import DataTransformer
from src.load import DataLoader
from src.data_quality import DataQualityChecker
from src.utils import setup_logging, load_config

class ETLPipeline:
    def __init__(self, config_path="config/config.yaml", force_refresh=False):
        """Inicializa el pipeline ETL con configuración."""
        # Get the directory containing the script
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        # Create absolute path to config file
        config_path = os.path.join(self.script_dir, config_path)

        # Load environment and config
        load_dotenv()
        self.force_refresh = force_refresh
        self.config = load_config(config_path)
        setup_logging()
        self.logger = logging.getLogger(__name__)
        
        # Seleccionar un directorio base escribible para cache/raw/processed.
        # Algunos entornos (p. ej. el contenedor Airflow) pueden montar el repo
        # como read-only. Intentamos varios candidatos y usamos el primero
        # que sea escribible.
        import tempfile

        candidate_bases = [
            self.script_dir,
            '/opt/airflow',
            os.path.join(os.path.expanduser('~'), 'ecommerce_etl_data'),
            tempfile.gettempdir(),
        ]

        base_dir = None
        for d in candidate_bases:
            try:
                if d is None:
                    continue
                test_dir = os.path.join(d, 'tmp_etl_test')
                os.makedirs(test_dir, exist_ok=True)
                test_file = os.path.join(test_dir, '.write_test')
                with open(test_file, 'w', encoding='utf-8') as f:
                    f.write('ok')
                os.remove(test_file)
                # remove test dir if empty
                try:
                    os.rmdir(test_dir)
                except Exception:
                    pass
                base_dir = d
                break
            except Exception:
                base_dir = None

        if base_dir is None:
            # Ultimate fallback to system temp
            base_dir = tempfile.gettempdir()

        # Crear directorio de caché si no existe
        self.cache_dir = os.path.join(base_dir, 'ecommerce_etl', 'cache')
        os.makedirs(self.cache_dir, exist_ok=True)

        # Directorios para persistir datos raw y procesados
        self.raw_dir = os.path.join(base_dir, 'ecommerce_etl', 'data', 'raw')
        self.processed_dir = os.path.join(base_dir, 'ecommerce_etl', 'data', 'processed')
        os.makedirs(self.raw_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
        
        # Inicializar componentes
        self.extractor = APIDataExtractor(self.config)
        self.transformer = DataTransformer(self.config)
        self.loader = DataLoader(self.config)
        self.dq_checker = DataQualityChecker(self.config)
        
        self.stats = {
            'start_time': None,
            'end_time': None,
            'records_processed': 0,
            'errors': []
        }

    def _log_sample(self, data: Any, label: str, max_cols: int = 10) -> None:
        """Loggea una muestra de los datos usando pandas si es posible."""
        try:
            if data is None:
                self.logger.info(f"Muestra {label}: <sin datos>")
                return

            if isinstance(data, dict):
                sample_dict = {k: data[k] for k in list(data.keys())[:max_cols]}
                self.logger.info(
                    f"Muestra {label}: {json.dumps(sample_dict, ensure_ascii=False, default=str)}"
                )
                return

            if isinstance(data, Iterable) and not isinstance(data, (str, bytes)):
                data_list = list(data)
                if not data_list:
                    self.logger.info(f"Muestra {label}: []")
                    return

                first = data_list[0]
                if isinstance(first, dict):
                    df = pd.DataFrame(data_list[:1])
                    if df.empty:
                        self.logger.info(f"Muestra {label}: <sin filas>")
                        return
                    if df.shape[1] > max_cols:
                        df = df.iloc[:, :max_cols]
                    sample_repr = df.to_string(index=False)
                    self.logger.info(f"Muestra {label} (primer registro):\n{sample_repr}")
                    return

                self.logger.info(f"Muestra {label}: {first}")
                return

            self.logger.info(f"Muestra {label}: {data}")
        except Exception as exc:
            self.logger.warning(f"No se pudo loggear muestra para {label}: {exc}")

    def _get_cache_path(self, endpoint_name):
        """Retorna la ruta del archivo de caché para un endpoint."""
        return os.path.join(self.cache_dir, f"{endpoint_name}.json")

    def _load_from_cache(self, endpoint_name):
        """Carga datos desde el caché si existe."""
        cache_path = self._get_cache_path(endpoint_name)
        if os.path.exists(cache_path):
            self.logger.info(f"Cargando {endpoint_name} desde caché")
            with open(cache_path, 'r') as f:
                return json.load(f)
        return None

    def _save_to_cache(self, endpoint_name, data):
        """Guarda datos en el caché."""
        cache_path = self._get_cache_path(endpoint_name)
        self.logger.info(f"Guardando {endpoint_name} en caché")
        with open(cache_path, 'w') as f:
            json.dump(data, f)

    def run(self):
        """Ejecuta el pipeline ETL completo."""
        self.stats['start_time'] = datetime.now()
        self.logger.info("Iniciando pipeline ETL")
        
        try:
            # EXTRACT
            raw_data = self._extract_phase()
            
            # TRANSFORM
            transformed_data = self._transform_phase(raw_data)
            
            # DATA QUALITY
            dq_ok = self._data_quality_phase(transformed_data)
            if not dq_ok:
                self.logger.warning("Data Quality detectó problemas; registros inválidos fueron omitidos del LOAD.")

            # TESTS (pytest)
            tests_ok = self._tests_phase()
            if not tests_ok:
                self.logger.warning("Tests fallidos. Se registraron errores, pero el LOAD continuará omitiendo registros inválidos.")

            # (no synthetic fallback records by design)

            # LOAD
            self._load_phase(transformed_data)
            
            self.stats['end_time'] = datetime.now()
            self._log_summary()
            
        except Exception as e:
            self.logger.error(f"Error en pipeline ETL: {str(e)}")
            self.stats['errors'].append(str(e))
            raise

    def _extract_phase(self):
        """Fase de extracción de datos desde la API o caché."""
        self.logger.info("Iniciando fase EXTRACT")
        
        raw_data = {}
        endpoints = self.config['api']['endpoints']
        
        for endpoint_name, endpoint_path in endpoints.items():
            self.logger.info(f"Procesando datos de {endpoint_name}")
            try:
                # Solo usar caché si no se fuerza actualización
                cached_data = None if self.force_refresh else self._load_from_cache(endpoint_name)
                
                if cached_data is not None:
                    raw_data[endpoint_name] = cached_data
                    self.logger.info(f"Datos de {endpoint_name} cargados desde caché")
                else:
                    # Si no hay caché o se fuerza actualización, extraer de la API
                    self.logger.info(f"Extrayendo datos de {endpoint_name} desde API")
                    data = self.extractor.fetch_endpoint(endpoint_name, endpoint_path)
                    raw_data[endpoint_name] = data
                    # Guardar en caché para futura referencia
                    self._save_to_cache(endpoint_name, data)
                
                # Persistir raw en disco
                try:
                    raw_path = os.path.join(self.raw_dir, f"{endpoint_name}.json")
                    with open(raw_path, 'w', encoding='utf-8') as f:
                        json.dump(raw_data[endpoint_name], f, indent=2, ensure_ascii=False)
                    self.logger.info(f"Raw data guardada en {raw_path}")
                except Exception as e:
                    self.logger.warning(f"No se pudo guardar raw data {endpoint_name}: {e}")
                
                self._log_sample(raw_data[endpoint_name], f"raw->{endpoint_name}")
                self.logger.info(f"Procesados {len(raw_data[endpoint_name])} registros de {endpoint_name}")
                
            except Exception as e:
                error_msg = f"Error procesando {endpoint_name}: {str(e)}"
                self.logger.error(error_msg)
                self.stats['errors'].append(error_msg)
                raise
        
        return raw_data

    def _transform_phase(self, raw_data):
        """Fase de transformación de datos."""
        self.logger.info("Iniciando fase TRANSFORM")
        
        transformed_data = {}
        
        # Transformar productos
        if 'products' in raw_data:
            self.logger.info("Transformando datos de productos")
            transformed_data['products'] = self.transformer.transform_products(
                raw_data['products']
            )
            self._log_sample(transformed_data['products'], "transform->products")
        
        # Transformar usuarios
        if 'users' in raw_data:
            self.logger.info("Transformando datos de usuarios")
            users_data = self.transformer.transform_users(raw_data['users'])
            transformed_data['users'] = users_data['users']
            transformed_data['geography'] = users_data['geography']
            self._log_sample(transformed_data['users'], "transform->users")
            self._log_sample(transformed_data['geography'], "transform->geography")
        
        # Transformar carritos
        if 'carts' in raw_data:
            self.logger.info("Transformando datos de carritos")
            transformed_data['sales'] = self.transformer.transform_carts(
                raw_data['carts'], raw_data.get('products', [])
            )
            self._log_sample(transformed_data['sales'], "transform->sales")
        
        # Debugging de dimensión de tiempo
        self.logger.info("Generando dimensión de tiempo")
        dates = self.transformer.generate_date_dimension(
            transformed_data.get('sales', [])
        )
        transformed_data['dates'] = dates
        self._log_sample(transformed_data['dates'], "transform->dates")

        # Persistir datos procesados en disk
        try:
            for key, value in transformed_data.items():
                path = os.path.join(self.processed_dir, f"{key}.json")
                with open(path, 'w', encoding='utf-8') as f:
                    json.dump(value, f, indent=2, ensure_ascii=False, default=str)
                self.logger.info(f"Processed data guardada en {path}")
        except Exception as e:
            self.logger.warning(f"No se pudo guardar processed data: {e}")
        
        return transformed_data

    def _data_quality_phase(self, transformed_data):
        """Fase de validacion de calidad de datos."""
        self.logger.info("Iniciando fase DATA QUALITY")

        validation_results = self.dq_checker.validate_full_dataset(transformed_data)

        if not validation_results['is_valid']:
            self.logger.warning("Problemas de calidad de datos detectados:")
            for error in validation_results['errors']:
                self.logger.warning(f"  - {error}")

        report = self.dq_checker.generate_dq_report(validation_results)
        self.logger.info("\n" + report)

        self._apply_dq_exclusions(
            transformed_data,
            validation_results.get('error_details', [])
        )

        self.stats['records_processed'] = validation_results['records_checked']
        self.stats['errors'].extend(validation_results['errors'])

        return validation_results['is_valid']

    def _apply_dq_exclusions(self, transformed_data, error_details):
        # Remove invalid records flagged by data quality checks before LOAD.
        if not error_details:
            return

        skipped = defaultdict(int)
        products_reasons = defaultdict(list)
        users_reasons = defaultdict(list)
        geography_reasons = defaultdict(list)
        sales_index_reasons = defaultdict(list)
        sales_key_reasons = defaultdict(list)

        for detail in error_details:
            dataset = detail.get('dataset')
            message = detail.get('message') or f"{dataset} validation failed"
            if dataset == 'products' and detail.get('record_id') is not None:
                products_reasons[detail['record_id']].append(message)
            elif dataset == 'users' and detail.get('record_id') is not None:
                users_reasons[detail['record_id']].append(message)
            elif dataset == 'geography' and detail.get('record_id') is not None:
                geography_reasons[detail['record_id']].append(message)
            elif dataset == 'sales':
                if detail.get('record_index') is not None:
                    sales_index_reasons[detail['record_index']].append(message)
                cart = detail.get('cart_id')
                prod = detail.get('product_id')
                if cart is not None and prod is not None:
                    sales_key_reasons[(cart, prod)].append(message)

        if products_reasons and 'products' in transformed_data:
            filtered_products = []
            for record in transformed_data['products']:
                pid = record.get('product_id')
                if pid in products_reasons:
                    for msg in sorted(set(products_reasons[pid])):
                        self.logger.warning(f"Omitiendo products product_id={pid}: {msg}")
                    skipped['products'] += 1
                    continue
                filtered_products.append(record)
            transformed_data['products'] = filtered_products

        if users_reasons and 'users' in transformed_data:
            filtered_users = []
            for record in transformed_data['users']:
                uid = record.get('user_id') or record.get('id')
                if uid in users_reasons:
                    for msg in sorted(set(users_reasons[uid])):
                        self.logger.warning(f"Omitiendo users user_id={uid}: {msg}")
                    skipped['users'] += 1
                    continue
                filtered_users.append(record)
            transformed_data['users'] = filtered_users

        invalid_user_ids = set(users_reasons.keys())
        invalid_geo_ids = set(geography_reasons.keys()) | invalid_user_ids
        if invalid_geo_ids and 'geography' in transformed_data:
            filtered_geo = []
            for record in transformed_data['geography']:
                uid = record.get('user_id')
                if uid in invalid_geo_ids:
                    combined = geography_reasons.get(uid, []) + users_reasons.get(uid, [])
                    reasons = sorted(set(combined)) or ['Usuario marcado como invalido']
                    for msg in reasons:
                        self.logger.warning(f"Omitiendo geography user_id={uid}: {msg}")
                    skipped['geography'] += 1
                    continue
                filtered_geo.append(record)
            transformed_data['geography'] = filtered_geo

        invalid_product_ids = set(products_reasons.keys())
        if 'sales' in transformed_data:
            original_sales = transformed_data.get('sales', [])
            filtered_sales = []
            for idx, sale in enumerate(original_sales):
                key = (sale.get('cart_id'), sale.get('product_id'))
                reasons: List[str] = []
                reasons.extend(sales_index_reasons.get(idx, []))
                if None not in key:
                    reasons.extend(sales_key_reasons.get(key, []))
                pid = sale.get('product_id')
                uid = sale.get('user_id')
                if pid in invalid_product_ids:
                    reasons.append(f"Producto {pid} descartado por DQ")
                if uid in invalid_user_ids:
                    reasons.append(f"Usuario {uid} descartado por DQ")
                if reasons:
                    for msg in sorted(set(reasons)):
                        self.logger.warning(
                            f"Omitiendo sales cart_id={sale.get('cart_id')} product_id={sale.get('product_id')}: {msg}"
                        )
                    skipped['sales'] += 1
                    continue
                filtered_sales.append(sale)
            transformed_data['sales'] = filtered_sales

        if skipped:
            summary = ', '.join(f"{k}={v}" for k, v in sorted(skipped.items()))
            self.logger.info(f"Registros omitidos por DQ: {summary}")


    def _tests_phase(self):
        """Ejecuta la suite de tests (pytest) como validacion previa a LOAD."""
        self.logger.info("Iniciando fase TESTS (pytest): validando reglas y transformaciones")
        tests_dir = os.path.join(self.script_dir, 'tests')
        if not os.path.isdir(tests_dir):
            self.logger.warning("Carpeta de tests no encontrada; omitiendo fase TESTS")
            return True

        if pytest is None:
            self.logger.error("pytest no esta disponible. Instalarlo para ejecutar tests previos a LOAD.")
            return False

        try:
            code = pytest.main(['-q', tests_dir])
        except SystemExit as exc:
            code = int(getattr(exc, 'code', 1) or 1)

        if code != 0:
            self.logger.error(f"Tests fallaron (exit code={code}). Revisar detalles arriba.")
            return False

        self.logger.info("Tests OK. Continuando con fase LOAD.")
        return True

    def _load_phase(self, transformed_data):
        """Fase de carga a base de datos."""
        self.logger.info("Iniciando fase LOAD")
        
        # Cargar en orden correcto para respetar constraints
        load_order = ['dates', 'products', 'users', 'geography', 'sales']
        
        # (no synthetic-row insertion)

        for data_type in load_order:
            if data_type in transformed_data and transformed_data[data_type]:
                self.logger.info(f"Cargando {len(transformed_data[data_type])} registros de {data_type}")
                self._log_sample(transformed_data[data_type], f"load->{data_type}")
                try:
                    self.loader.load_data(data_type, transformed_data[data_type])
                    self.stats['records_processed'] += len(transformed_data[data_type])
                except Exception as e:
                    error_msg = f"Error cargando {data_type}: {str(e)}"
                    self.logger.error(error_msg)
                    self.stats['errors'].append(error_msg)
                    raise

    # (synthetic-record insertion removed by user request)

    def _log_summary(self):
        """Registra resumen de la ejecución."""
        duration = self.stats['end_time'] - self.stats['start_time']
        self.logger.info(f"Pipeline ETL completado en {duration}")
        self.logger.info(f"Registros procesados: {self.stats['records_processed']}")
        self.logger.info(f"Errores encontrados: {len(self.stats['errors'])}")
        
        if self.stats['errors']:
            for error in self.stats['errors']:
                self.logger.warning(f"Error: {error}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Execute ETL pipeline')
    parser.add_argument('--force-refresh', action='store_true', 
                       help='Force refresh data from API instead of using cache')
    args = parser.parse_args()
    
    pipeline = ETLPipeline(force_refresh=args.force_refresh)
    pipeline.run()
