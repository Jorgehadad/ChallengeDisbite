#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FakeStore API Extractor
Parte 1 - ETL: Extracción
-------------------------------------
Descarga los datos desde los 3 endpoints:
  - /products
  - /carts
  - /users

Guarda la información en archivos JSON (uno por endpoint)
para su posterior uso en el diseño dimensional y carga al DWH.
"""

import os
import json
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime

# =========================
# Configuración general
# =========================
BASE_URL = "https://fakestoreapi.com"
ENDPOINTS = {
    "products": "/products",
    "carts": "/carts",
    "users": "/users"
}
OUTPUT_DIR = "data_raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# =========================
# Sesión con reintentos
# =========================
def build_session():
    retry_strategy = Retry(
        total=5,
        backoff_factor=0.5,             # reintentos exponenciales (0.5, 1, 2, 4…)
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "FakeStoreExtractor/1.0"
    })
    return session


# =========================
# Función de extracción
# =========================
def fetch_endpoint(session, endpoint_name, endpoint_path):
    """Descarga datos de un endpoint específico."""
    url = f"{BASE_URL}{endpoint_path}"
    print(f"[INFO] Descargando datos de: {url}")
    try:
        response = session.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        print(f"[OK] {endpoint_name}: {len(data)} registros descargados")
        return data
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Falló la descarga de {endpoint_name}: {e}")
        return []


# =========================
# Ejecución principal
# =========================
def main():
    start_time = time.time()
    session = build_session()

    all_data = {}

    for name, path in ENDPOINTS.items():
        data = fetch_endpoint(session, name, path)
        all_data[name] = data

        # Guardar JSON individual
        output_file = os.path.join(OUTPUT_DIR, f"{name}.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"[SAVE] {output_file}")

    total_time = round(time.time() - start_time, 2)
    print(f"\n[END] Extracción completada en {total_time} segundos.")
    print(f"Archivos guardados en: {os.path.abspath(OUTPUT_DIR)}")


if __name__ == "__main__":
    main()
