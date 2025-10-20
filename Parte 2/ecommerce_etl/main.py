# -*- coding: utf-8 -*-

# main.py - módulo generado automáticamente
import logging
import yaml
import os
from datetime import datetime
from dotenv import load_dotenv

from src.extract import APIDataExtractor
from src.transform import DataTransformer
from src.load import DataLoader
from src.data_quality import DataQualityChecker
from src.utils import setup_logging, load_config

class ETLPipeline:
    def __init__(self, config_path="config/config.yaml"):
        """Inicializa el pipeline ETL con configuración."""
        load_dotenv()
        self.config = load_config(config_path)
        setup_logging()
        self.logger = logging.getLogger(__name__)
        
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
        """Fase de extracción de datos desde la API."""
        self.logger.info("Iniciando fase EXTRACT")
        
        raw_data = {}
        endpoints = self.config['api']['endpoints']
        
        for endpoint_name, endpoint_path in endpoints.items():
            self.logger.info(f"Extrayendo datos de {endpoint_name}")
            try:
                data = self.extractor.fetch_endpoint(endpoint_name, endpoint_path)
                raw_data[endpoint_name] = data
                self.logger.info(f"Extraídos {len(data)} registros de {endpoint_name}")
            except Exception as e:
                error_msg = f"Error extrayendo {endpoint_name}: {str(e)}"
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
        
        # Generar dimensión de tiempo
        self.logger.info("Generando dimensión de tiempo")
        transformed_data['dates'] = self.transformer.generate_date_dimension(
            transformed_data.get('sales', [])
        )
        
        return transformed_data

    def _data_quality_phase(self, transformed_data):
        """Fase de validación de calidad de datos."""
        self.logger.info("Iniciando fase DATA QUALITY")
        
        for data_type, data in transformed_data.items():
            if data:  # Solo verificar si hay datos
                self.logger.info(f"Validando calidad de datos para {data_type}")
                try:
                    results = self.dq_checker.validate_data(data_type, data)
                    if not results['is_valid']:
                        error_msg = f"Fallaron validaciones DQ para {data_type}: {results['errors']}"
                        self.logger.error(error_msg)
                        self.stats['errors'].extend(results['errors'])
                except Exception as e:
                    error_msg = f"Error en validación DQ para {data_type}: {str(e)}"
                    self.logger.error(error_msg)
                    self.stats['errors'].append(error_msg)

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
    pipeline = ETLPipeline()
    pipeline.run()