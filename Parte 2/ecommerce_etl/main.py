# -*- coding: utf-8 -*-

# main.py - módulo generado automáticamente
import logging
import yaml
import os
import json
from datetime import datetime
from dotenv import load_dotenv

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
        
        self.force_refresh = force_refresh
        load_dotenv()
        self.config = load_config(config_path)
        setup_logging()
        self.logger = logging.getLogger(__name__)
        
        # Crear directorio de caché si no existe
        self.cache_dir = os.path.join(self.script_dir, 'cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        
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
            self._data_quality_phase(transformed_data)
            
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
        
        # Transformar usuarios
        if 'users' in raw_data:
            self.logger.info("Transformando datos de usuarios")
            users_data = self.transformer.transform_users(raw_data['users'])
            transformed_data['users'] = users_data['users']
            transformed_data['geography'] = users_data['geography']
        
        # Transformar carritos
        if 'carts' in raw_data:
            self.logger.info("Transformando datos de carritos")
            transformed_data['sales'] = self.transformer.transform_carts(
                raw_data['carts'], raw_data.get('products', [])
            )
        
        # Debugging de dimensión de tiempo
        self.logger.info("Generando dimensión de tiempo")
        dates = self.transformer.generate_date_dimension(
            transformed_data.get('sales', [])
        )
        # Agregar logging de debug
        if dates:
            sample_date = dates[0]
            self.logger.debug(f"Muestra de fecha transformada: {sample_date}")
            self.logger.debug(f"Columnas generadas: {list(sample_date.keys())}")
        
        transformed_data['dates'] = dates
        
        return transformed_data

    def _data_quality_phase(self, transformed_data):
        """Fase de validación de calidad de datos."""
        self.logger.info("Iniciando fase DATA QUALITY")
        
        # Validar todo el conjunto de datos
        validation_results = self.dq_checker.validate_full_dataset(transformed_data)
        
        if not validation_results['is_valid']:
            self.logger.warning("Problemas de calidad de datos detectados:")
            for error in validation_results['errors']:
                self.logger.warning(f"  - {error}")
        
        # Generar reporte
        report = self.dq_checker.generate_dq_report(validation_results)
        self.logger.info("\n" + report)
        
        # Actualizar estadísticas
        self.stats['records_processed'] = validation_results['records_checked']
        self.stats['errors'].extend(validation_results['errors'])
        
        return validation_results['is_valid']

    def _load_phase(self, transformed_data):
        """Fase de carga a base de datos."""
        self.logger.info("Iniciando fase LOAD")
        
        # Cargar en orden correcto para respetar constraints
        load_order = ['dates', 'products', 'users', 'geography', 'sales']
        
        for data_type in load_order:
            if data_type in transformed_data and transformed_data[data_type]:
                self.logger.info(f"Cargando {len(transformed_data[data_type])} registros de {data_type}")
                try:
                    self.loader.load_data(data_type, transformed_data[data_type])
                    self.stats['records_processed'] += len(transformed_data[data_type])
                except Exception as e:
                    error_msg = f"Error cargando {data_type}: {str(e)}"
                    self.logger.error(error_msg)
                    self.stats['errors'].append(error_msg)
                    raise

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