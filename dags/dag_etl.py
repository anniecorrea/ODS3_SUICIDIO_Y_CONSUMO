from airflow.decorators import dag, task
from datetime import datetime
import logging
import sys
from pathlib import Path

# Configurar rutas base para el entorno Docker de Airflow
BASE_DIR = Path("/opt/airflow") 
SRC_DIR = BASE_DIR / "src"
DATA_DIR = BASE_DIR / "notebooks" / "data"
OUTPUT_DIR = BASE_DIR / "data_out"

# Agregar el directorio src al path para importar módulos
sys.path.insert(0, str(SRC_DIR))

logger = logging.getLogger("airflow.task")

@dag(
    dag_id="ods3_etl_suicidio_consumo",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    
)
def etl_pipeline():
    
    @task()
    def extract_conducta_suicida():
        """Extrae datos de conducta suicida desde la API de Datos Abiertos Bogotá."""
        try:
            logger.info("Iniciando extracción de datos de conducta suicida desde API...")
            
            # Asegurar que el path esté configurado
            import sys
            sys.path.insert(0, str(SRC_DIR))
            
            from extract.extract_api import extraer_datos_api, guardar_datos_csv
            
            # Extraer datos de la API
            datos = extraer_datos_api(limit_por_pagina=32000)
            
            # Crear directorio si no existe
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            
            # Guardar en CSV
            guardar_datos_csv(
                datos, 
                nombre_archivo="conductasuicida.csv",
                carpeta=str(DATA_DIR)
            )
            
            logger.info(f"Extracción de conducta suicida completada: {datos['result']['total']} registros")
            return {"status": "success", "records": datos['result']['total']}
            
        except Exception as e:
            logger.error(f"Error en extracción de conducta suicida: {e}")
            raise

    @task()
    def extract_consumo_sustancias():
        """Extrae datos de consumo de sustancias desde CSV local."""
        try:
            logger.info("Iniciando extracción de datos de consumo de sustancias desde CSV...")
            
            # Asegurar que el path esté configurado
            import sys
            sys.path.insert(0, str(SRC_DIR))
            
            from extract.extract_csv import extraer_csv
            
            # Ruta del archivo CSV
            csv_path = DATA_DIR / "consumosustancias.csv"
            
            # Extraer datos del CSV
            datos = extraer_csv(str(csv_path))
            
            logger.info(f"Extracción de consumo de sustancias completada: {datos.shape[0]} registros")
            return {"status": "success", "records": datos.shape[0]}
            
        except Exception as e:
            logger.error(f"Error en extracción de consumo de sustancias: {e}")
            raise

    @task()
    def transform_conducta_suicida():
        """Transforma datos de conducta suicida a formato tidy."""
        try:
            logger.info("Iniciando transformación de conducta suicida...")
            
            # Ejecutar script de transformación directamente
            import subprocess
            import json
            
            # Crear directorio de salida si no existe
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            
            script_path = SRC_DIR / "transform" / "conductasuicida-transform.py"
            input_path = DATA_DIR / "conductasuicida.csv"
            output_path = OUTPUT_DIR / "suicida_tidy.csv"
            qc_path = OUTPUT_DIR / "suicida_qc.json"
            
            result = subprocess.run([
                "python",
                str(script_path),
                "--input", str(input_path),
                "--output", str(output_path),
                "--split-clasif",
                "--report", str(qc_path)
            ], capture_output=True, text=True, cwd=str(BASE_DIR))
            
            if result.returncode != 0:
                logger.error(f"Error ejecutando transformación: {result.stderr}")
                raise Exception(f"Transformación falló: {result.stderr}")
            
            logger.info(result.stdout)
            
            # Leer QC para retornar
            with open(qc_path, "r", encoding="utf-8") as f:
                qc = json.load(f)
            
            logger.info(f"Transformación de conducta suicida completada")
            logger.info(f"QC: {qc}")
            
            return {"status": "success", "rows": qc.get("rows", 0), "qc": qc}
            
        except Exception as e:
            logger.error(f"Error en transformación de conducta suicida: {e}")
            raise

    @task()
    def transform_consumo_spa():
        """Transforma datos de consumo de sustancias (SPA) a formato tidy."""
        try:
            logger.info("Iniciando transformación de consumo de sustancias (SPA)...")
            
            # Ejecutar script de transformación directamente
            import subprocess
            import json
            
            # Crear directorio de salida si no existe
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            
            script_path = SRC_DIR / "transform" / "SPA-transform.py"
            input_path = DATA_DIR / "consumosustancias.csv"
            output_path = OUTPUT_DIR / "spa_tidy.csv"
            qc_path = OUTPUT_DIR / "spa_qc.json"
            
            result = subprocess.run([
                "python",
                str(script_path),
                "--input", str(input_path),
                "--output", str(output_path),
                "--report", str(qc_path)
            ], capture_output=True, text=True, cwd=str(BASE_DIR))
            
            if result.returncode != 0:
                logger.error(f"Error ejecutando transformación: {result.stderr}")
                raise Exception(f"Transformación falló: {result.stderr}")
            
            logger.info(result.stdout)
            
            # Leer QC para retornar
            with open(qc_path, "r", encoding="utf-8") as f:
                qc = json.load(f)
            
            logger.info(f"Transformación de SPA completada")
            logger.info(f"QC: {qc}")
            
            return {"status": "success", "rows": qc.get("rows", 0), "qc": qc}
            
        except Exception as e:
            logger.error(f"Error en transformación de SPA: {e}")
            raise

    @task()
    def merge_datasets():
        """Realiza el merge de los datasets transformados de SPA y Conducta Suicida."""
        try:
            logger.info("Iniciando merge de datasets SPA y Conducta Suicida...")
            
            # Ejecutar script de merge directamente
            import subprocess
            import json
            
            # Crear directorio de salida si no existe
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            
            script_path = SRC_DIR / "transform" / "merged_transform.py"
            spa_path = OUTPUT_DIR / "spa_tidy.csv"
            sui_path = OUTPUT_DIR / "suicida_tidy.csv"
            output_path = OUTPUT_DIR / "merged_spa_suicidas.csv"
            qc_path = OUTPUT_DIR / "merge_qc.json"
            
            result = subprocess.run([
                "python",
                str(script_path),
                "--spa", str(spa_path),
                "--sui", str(sui_path),
                "--output", str(output_path),
                "--how", "inner",
                "--add-pcts",
                "--report", str(qc_path)
            ], capture_output=True, text=True, cwd=str(BASE_DIR))
            
            if result.returncode != 0:
                logger.error(f"Error ejecutando merge: {result.stderr}")
                raise Exception(f"Merge falló: {result.stderr}")
            
            logger.info(result.stdout)
            
            # Leer QC para retornar
            with open(qc_path, "r", encoding="utf-8") as f:
                qc = json.load(f)
            
            logger.info(f"Merge completado exitosamente")
            logger.info(f"QC del merge: {qc}")
            
            return {"status": "success", "rows": qc.get("rows_merged", 0), "qc": qc}
            
        except Exception as e:
            logger.error(f"Error en merge de datasets: {e}")
            raise

    @task()
    def load_results():
        """Carga final - Valida y reporta los resultados del ETL."""
        try:
            logger.info("Iniciando carga y validación de resultados...")
            
            import pandas as pd
            import json
            
            # Verificar archivos generados
            files_to_check = [
                OUTPUT_DIR / "suicida_tidy.csv",
                OUTPUT_DIR / "spa_tidy.csv",
                OUTPUT_DIR / "merged_spa_suicidas.csv",
                OUTPUT_DIR / "suicida_qc.json",
                OUTPUT_DIR / "spa_qc.json",
                OUTPUT_DIR / "merge_qc.json"
            ]
            
            validation_results = {}
            for file_path in files_to_check:
                if file_path.exists():
                    if file_path.suffix == '.csv':
                        df = pd.read_csv(file_path)
                        validation_results[str(file_path)] = {
                            "exists": True,
                            "rows": len(df),
                            "columns": len(df.columns)
                        }
                    elif file_path.suffix == '.json':
                        with open(file_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        validation_results[str(file_path)] = {
                            "exists": True,
                            "content": data
                        }
                else:
                    validation_results[str(file_path)] = {"exists": False}
            
            logger.info("Validación de archivos completada:")
            for file, result in validation_results.items():
                logger.info(f"  - {file}: {result}")
            
            # Guardar resumen de ejecución
            summary = {
                "execution_date": datetime.now().isoformat(),
                "validation_results": validation_results,
                "status": "completed"
            }
            
            logger.info("Carga completada exitosamente. ETL finalizado.")
            
            return {"status": "success", "summary": validation_results}
            
        except Exception as e:
            logger.error(f"Error en carga de resultados: {e}")
            raise

    # Definir dependencias del pipeline
    # Extracciones en paralelo
    extract_sui = extract_conducta_suicida()
    extract_spa = extract_consumo_sustancias()
    
    # Transformaciones (dependen de sus respectivas extracciones)
    transform_sui = transform_conducta_suicida()
    transform_spa_task = transform_consumo_spa()
    
    # Merge (depende de ambas transformaciones)
    merge_task = merge_datasets()
    
    # Load final (depende del merge)
    load_task = load_results()
    
    # Definir el flujo de dependencias
    extract_sui >> transform_sui >> merge_task
    extract_spa >> transform_spa_task >> merge_task
    merge_task >> load_task


# Instanciar el DAG
etl_dag = etl_pipeline()
