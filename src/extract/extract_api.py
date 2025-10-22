import urllib.request
import json
import csv
import os
import pandas as pd


def extraer_datos_api(limit_por_pagina=32000):
    
    base_url = "https://datosabiertos.bogota.gov.co/api/3/action/datastore_search"
    resource_id = "555a3bea-e358-4e77-be49-7b448774324b"
    
    todos_los_registros = []
    offset = 0
    total_registros = None
    
    print("Iniciando extracción de datos...")
    
    try:
        while True:
            # Construir URL con offset y limit
            url = f"{base_url}?resource_id={resource_id}&limit={limit_por_pagina}&offset={offset}"
            
            # Realizar petición
            with urllib.request.urlopen(url) as response:
                data = json.loads(response.read().decode('utf-8'))
            
            if not data.get('success'):
                raise Exception("La API retornó un error")
            
            result = data.get('result', {})
            records = result.get('records', [])
            
            # Guardar el total de registros disponibles
            if total_registros is None:
                total_registros = result.get('total', 0)
                print(f"Total de registros disponibles: {total_registros}")
            
            # Si no hay más registros, salir
            if not records:
                break
            
            # Agregar registros a la lista
            todos_los_registros.extend(records)
            print(f"Descargados: {len(todos_los_registros)} / {total_registros} registros...")
            
            # Si ya obtuvimos todos los registros, salir
            if len(todos_los_registros) >= total_registros:
                break
            
            # Incrementar offset para la siguiente página
            offset += limit_por_pagina
        
        # Construir respuesta completa
        respuesta_completa = {
            'success': True,
            'result': {
                'records': todos_los_registros,
                'total': total_registros,
                'fields': data.get('result', {}).get('fields', [])
            }
        }
        
        return respuesta_completa
        
    except Exception as e:
        print(f"Error al consultar la API: {e}")
        raise


def guardar_datos_csv(datos, nombre_archivo="conductasuicida.csv", carpeta="notebooks/data"):
    
    if not datos.get('success'):
        print("No hay datos para guardar")
        return
    
    records = datos.get('result', {}).get('records', [])
    
    if not records:
        print("No hay registros para guardar")
        return
    
    # Crear la carpeta si no existe
    os.makedirs(carpeta, exist_ok=True)
    
    # Ruta completa del archivo
    ruta_completa = os.path.join(carpeta, nombre_archivo)
    
    # Obtener las columnas del primer registro
    columnas = list(records[0].keys())
    
    # Escribir el archivo CSV
    with open(ruta_completa, 'w', newline='', encoding='utf-8-sig') as archivo_csv:
        writer = csv.DictWriter(archivo_csv, fieldnames=columnas)
        writer.writeheader()
        writer.writerows(records)
    
    print(f"\nDatos guardados exitosamente en: {ruta_completa}")
    print(f"Total de registros guardados: {len(records)}")
    print(f"Columnas: {len(columnas)}")


if __name__ == "__main__":
    # Llamar a la API
    print("Extrayendo datos de la API...")
    datos = extraer_datos_api()
    
    # Mostrar información básica
    if datos.get('success'):
        result = datos.get('result', {})
        records = result.get('records', [])
        print(f"\nDatos extraídos exitosamente")
        print(f"Total de registros obtenidos: {len(records)}")
        print(f"Total de registros disponibles: {result.get('total', 0)}")
        
        # Guardar los datos en CSV
        guardar_datos_csv(datos)
        
        # Mostrar el primer registro como ejemplo
        if records:
            print(f"\nPrimer registro:")
            print(json.dumps(records[0], indent=2, ensure_ascii=False))
    else:
        print("La API retornó un error")