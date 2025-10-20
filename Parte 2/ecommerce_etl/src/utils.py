# -*- coding: utf-8 -*-

# utils.py - módulo generado automáticamente
import logging
import yaml
import os
from typing import Dict, Any
from datetime import datetime, timedelta

def setup_logging():
    """Configura el sistema de logging."""
    # Crear directorio de logs si no existe
    # Prefer repo/logs, but if it is not writable (mounted read-only inside a
    # container), fall back to common writable locations such as
    # /opt/airflow/logs (Airflow runtime) or the system temp directory.
    candidate_dirs = [
        os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs'),
        '/opt/airflow/logs',
        os.path.join(os.path.expanduser('~'), 'logs'),
        None  # placeholder for tempfile.gettempdir()
    ]

    import tempfile
    candidate_dirs[-1] = tempfile.gettempdir()

    chosen_log_dir = None
    for d in candidate_dirs:
        try:
            if d is None:
                continue
            os.makedirs(d, exist_ok=True)
            # try to open a test file to ensure it's writable
            test_path = os.path.join(d, '.write_test')
            with open(test_path, 'w', encoding='utf-8') as f:
                f.write('ok')
            os.remove(test_path)
            chosen_log_dir = d
            break
        except Exception:
            # not writable, try next
            chosen_log_dir = None

    # If none of the candidate dirs worked, fall back to stream-only logging
    log_handlers = [logging.StreamHandler()]

    # Configure the file handler if we found a writable directory
    if chosen_log_dir:
        log_file = os.path.join(chosen_log_dir, 'etl.log')
        try:
            file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
            log_handlers.append(file_handler)
        except Exception:
            # If it fails, keep console-only handler
            logging.getLogger(__name__).warning(
                f"No se pudo crear handler de archivo en {log_file}, usando consola."
            )

    # Configurar el formato del log
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # Configurar el logger raíz
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=log_handlers
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

def is_cache_valid(cache_path, max_age_hours=24):
    """Check if cache file is valid based on age."""
    if not os.path.exists(cache_path):
        return False
        
    file_time = datetime.fromtimestamp(os.path.getmtime(cache_path))
    age = datetime.now() - file_time
    
    return age < timedelta(hours=max_age_hours)

# Configurar manejador global de excepciones
import sys
sys.excepthook = handle_exception