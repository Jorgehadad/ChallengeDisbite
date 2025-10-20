import os

# Ruta base del proyecto
BASE_PATH = r".\Challenge Disbite 2025\Desarrollo\Parte 2"

# Estructura de carpetas del proyecto
STRUCTURE = {
    "ecommerce_etl": {
        "config": ["config.yaml", ".env"],
        "src": [
            "__init__.py",
            "extract.py",
            "transform.py",
            "load.py",
            "data_quality.py",
            "utils.py",
        ],
        "logs": ["etl.log"],
        "data": {
            "raw": [],
            "processed": []
        },
        "": ["main.py", "requirements.txt"]
    }
}

def create_structure(base, structure):
    for folder, content in structure.items():
        folder_path = os.path.join(base, folder)
        os.makedirs(folder_path, exist_ok=True)
        print(f"📁 Carpeta creada: {folder_path}")

        # Crear archivos directamente en la carpeta
        if isinstance(content, list):
            for file in content:
                file_path = os.path.join(folder_path, file)
                with open(file_path, "w", encoding="utf-8") as f:
                    if file.endswith(".py"):
                        f.write("# -*- coding: utf-8 -*-\n\n")
                        f.write(f"# {file} - módulo generado automáticamente\n")
                    elif file.endswith(".yaml"):
                        f.write("# Configuración ETL (rellenar valores según entorno)\n")
                    elif file == ".env":
                        f.write("# Variables de entorno del proyecto ETL\n")
                    elif file.endswith(".txt"):
                        f.write("# Requerimientos del proyecto ETL\n")
                    else:
                        f.write("")  # crear vacío
                print(f"   📄 Archivo creado: {file_path}")

        # Si hay subcarpetas (diccionario anidado)
        elif isinstance(content, dict):
            create_structure(folder_path, content)

if __name__ == "__main__":
    print(f"Creando estructura ETL en:\n{BASE_PATH}\n")
    create_structure(BASE_PATH, STRUCTURE)
    print("\n✅ Estructura ETL creada correctamente.")
