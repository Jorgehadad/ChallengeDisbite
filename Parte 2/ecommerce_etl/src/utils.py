# -*- coding: utf-8 -*-

# utils.py - módulo generado automáticamente
import logging
import yaml
import os
from typing import Dict, Any

def setup_logging():
    """Configura el sistema de logging."""
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    log_file = os.getenv('LOG_FILE', 'logs/etl.log')
    
    # Crear directorio de logs si no existe
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

def load_config(config_path: str) -> Dict[str, Any]:
    """Carga configuración desde archivo YAML."""
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        return config
    except Exception as e:
        logging.error(f"Error cargando configuración: {str(e)}")
        raise

def handle_exception(exc_type, exc_value, exc_traceback):
    """Manejador global de excepciones."""
    if issubclass(exc_type, KeyboardInterrupt):
        # No logear KeyboardInterrupt
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    
    logging.critical(
        "Excepción no capturada:",
        exc_info=(exc_type, exc_value, exc_traceback)
    )

# Configurar manejador global de excepciones
import sys
sys.excepthook = handle_exception