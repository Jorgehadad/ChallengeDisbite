# Challenge Disbite 2025 — FakeStore ETL

Proyecto de práctica de ingeniería de datos basado en la API pública FakeStore. El repositorio incluye:

- Extracción de datos de productos, usuarios y carritos.
- Transformaciones y validaciones de calidad.
- Carga a un Data Warehouse PostgreSQL (modelo dimensional con tablas `dim_*` y `fact_sales`).
- Pruebas unitarias y flujo de CI con GitHub Actions.


**Contenido**

- Parte 1: scripts y modelos (transaccional y dimensional).
- Parte 2: proyecto ETL completo en `Parte 2/ecommerce_etl` (código, SQL y tests).
- Parte 3: consultas SQL de análisis.


**Estructura Del Repo**

- `Parte 1/` — Scripts y diagramas del modelo.
- `Parte 2/ecommerce_etl/` — Proyecto ETL principal:
  - `main.py` — Orquestación del pipeline (Extract → Transform → Data Quality → Load).
  - `src/` — Módulos `extract.py`, `transform.py`, `load.py`, `data_quality.py`, `init_db.py`, `utils.py`.
  - `sql/` — DDL/DML: `create_tables.sql`, `populate_dim_date.sql`, vistas, etc.
  - `tests/` — Pruebas unitarias (pytest).
  - `logs/` — Logs del ETL.
- `Parte 3/` — SQL de análisis.
- `data_raw/`, `data_clean/` — Datos locales para apoyo.
- `requirements.txt` — Dependencias raíz (con marcadores por plataforma para CI).


**Requisitos**

- Python 3.12+ (recomendado)
- PostgreSQL 14+ en `localhost` (o ajustar conexión)
- Acceso a red para consultar `https://fakestoreapi.com`


**Configuración**

- Config del ETL: `Parte 2/ecommerce_etl/config/config.yaml`
  - Parámetros de API, base de datos y ETL (batch size, etc.).
  - Puedes usar variables de entorno para logging: `LOG_LEVEL`, `LOG_FILE`.
- Variables de entorno opcionales vía `.env` en `Parte 2/ecommerce_etl/` (usado por `python-dotenv`).


**Instalación De Dependencias**

Opción recomendada (solo ETL):

- `cd Parte 2/ecommerce_etl`
- `pip install -r requirements.txt`

O bien, desde la raíz si prefieres usar el archivo global:

- `pip install -r requirements.txt`
- Nota: el paquete `pywin32` está condicionado a Windows para que el CI en Linux no falle.


**Inicializar Base De Datos**

1) Crea la BD (si no existe) y ajusta credenciales en `Parte 2/ecommerce_etl/config/config.yaml`.
2) Ejecuta el script de DDL:

- `python "Parte 2/ecommerce_etl/src/init_db.py"`

El script ejecuta `sql/create_tables.sql` y deja las tablas listas: `dim_date`, `dim_products`, `dim_users`, `dim_geography`, `fact_sales`.


**Ejecutar El Pipeline**

- `python "Parte 2/ecommerce_etl/main.py"`

El pipeline:

- Lee de caché local si existe (`Parte 2/ecommerce_etl/cache/`), o consulta la API.
- Transforma y valida los datos (reglas básicas de DQ).
- Genera dimensión de fechas a partir de ventas.
- Carga en PostgreSQL respetando el orden de dependencias.


**Pruebas**

- `cd Parte 2/ecommerce_etl`
- `pytest -q`

Las pruebas no requieren base de datos real (se usan fixtures/mocks donde aplica).


**Integración Continua (CI)**

- Workflow en `.github/workflows/ci.yml` con Python 3.12.
- Instala dependencias y ejecuta una verificación básica/imports o tests.
- Notas:
  - `pywin32` está condicionado: `pywin32==311; platform_system == "Windows"`.
  - Si añades dependencias que no compilan en Linux, usa marcadores de entorno o ajusta versiones.


**Solución De Problemas**

- Error de tabla inexistente (dimensión de productos):
  - Las tablas creadas por DDL son plurales (`dim_products`). El cargador resuelve y califica el esquema automáticamente.
  - Reejecuta `init_db.py` si cambiaste la estructura.
- Conexión a PostgreSQL:
  - Verifica host/puerto/usuario/clave en `config.yaml`.
  - Asegúrate de que el servicio PostgreSQL está activo y accesible.


**Licencia**

Proyecto académico para el Challenge Disbite 2025. Úsalo libremente con fines educativos.

