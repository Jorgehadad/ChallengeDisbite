# -*- coding: utf-8 -*-

# extract.py - módulo generado automáticamente
import requests
import logging
from typing import Dict, List, Any
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class APIDataExtractor:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.base_url = config['api']['base_url']
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """Crea sesión con política de reintentos."""
        session = requests.Session()
        retry_config = self.config['api']['retry']
        
        retry_strategy = Retry(
            total=retry_config['max_retries'],
            backoff_factor=retry_config['backoff_factor'],
            status_forcelist=retry_config['status_forcelist'],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def fetch_endpoint(self, endpoint_name: str, endpoint_path: str) -> List[Dict]:
        """Extrae datos de un endpoint específico con manejo de errores."""
        url = f"{self.base_url}{endpoint_path}"
        self.logger.info(f"Extrayendo datos de: {url}")
        
        try:
            response = self.session.get(
                url, 
                timeout=int(os.getenv('API_TIMEOUT', 30))
            )
            response.raise_for_status()
            
            data = response.json()
            self.logger.info(f"Extraídos {len(data)} registros de {endpoint_name}")
            return data
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error API para {endpoint_name}: {str(e)}")
            raise
        except ValueError as e:
            self.logger.error(f"Error parseando JSON de {endpoint_name}: {str(e)}")
            raise
    
    def fetch_all_data(self) -> Dict[str, List[Dict]]:
        """Extrae datos de todos los endpoints."""
        endpoints = self.config['api']['endpoints']
        all_data = {}
        
        for endpoint_name, endpoint_path in endpoints.items():
            try:
                data = self.fetch_endpoint(endpoint_name, endpoint_path)
                all_data[endpoint_name] = data
            except Exception as e:
                self.logger.error(f"Fallo extracción de {endpoint_name}: {str(e)}")
                # Decidir si continuar o fallar completamente
                raise
        
        return all_data