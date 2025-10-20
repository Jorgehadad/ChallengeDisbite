Airflow local (docker-compose)

Este directorio contiene una configuración mínima para ejecutar Airflow localmente usando Docker Compose.

Estructura:
- docker-compose.yml  -> Levanta la imagen oficial de Airflow en modo standalone
- dags/               -> Coloca aquí DAGs. Se incluye `fakestore_etl_dag.py` que ejecuta el pipeline `main.py`.

Instrucciones rápidas:
1. Asegúrate de tener Docker y Docker Compose instalados.
2. Desde este directorio ejecuta:

   docker compose up

3. Accede al Webserver en http://localhost:8080 (user/password: admin/admin si solicitado).

Notas:
- El container monta el repo en `/opt/airflow/repo` y los `dags` en `/opt/airflow/dags`.
- El DAG ejecuta: `python /opt/airflow/repo/Parte\ 2/ecommerce_etl/main.py --force-refresh`.
  Ajusta la ruta en `orchestrator/dags/fakestore_etl_dag.py` si tu estructura difiere.
- Para ejecutar manualmente el SQL de `dim_date` desde el container, copia la credencial de DB en `config/config.yaml` y usa el script `scripts/populate_dim_date.py --execute`.

Limitaciones:
- Esta configuración usa la imagen oficial de Airflow. No incluye una imagen de la DB ni persiste volúmenes por defecto. Para producción o pruebas de integración, se recomienda añadir un servicio Postgres y un volumen para `airflow/`.
