import pandas as pd
import numpy as np
import json
from pathlib import Path
import argparse
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def generar_reporte_columna(df, col):
    """Genera un reporte de calidad para una columna específica."""
    serie = df[col]
    tipo = serie.dtype

    # Métricas básicas
    nulos = serie.isna().sum()
    total = len(serie)
    unicos = serie.nunique()
    duplicados = total - serie.drop_duplicates().shape[0]

    # Observaciones automáticas
    obs_tipo = "Correcto" if not pd.api.types.is_object_dtype(serie) else "Verificar formato de texto"
    obs_nulos = "No presenta datos faltantes" if nulos == 0 else f"Presenta {nulos} nulos"
    obs_unicos = "Cumple unicidad total" if unicos == total else f"{unicos} valores únicos"
    obs_dups = "Sin duplicados" if duplicados == 0 else f"{duplicados} duplicados encontrados"

    # Ejemplos de problemas (nulos o tipos extraños)
    if nulos > 0:
        ejemplo_problemas = serie[serie.isna()].head(3).tolist()
    elif serie.duplicated().any():
        ejemplo_problemas = serie[serie.duplicated()].head(3).tolist()
    else:
        ejemplo_problemas = "Ninguno"

    return {
        "Métrica": "Tipo de dato", "Valor": str(tipo), "Observaciones": obs_tipo
    }, {
        "Métrica": "Valores nulos", "Valor": nulos, "Observaciones": obs_nulos
    }, {
        "Métrica": "Valores únicos", "Valor": unicos, "Observaciones": obs_unicos
    }, {
        "Métrica": "Duplicados", "Valor": duplicados, "Observaciones": obs_dups
    }, {
        "Métrica": "Ejemplos de problemas", "Valor": str(ejemplo_problemas), "Observaciones": "Columna confiable" if ejemplo_problemas == "Ninguno" else "Revisar valores"
    }


def calcular_completitud(df):
    """Porcentaje de valores no nulos en todo el dataset."""
    total = df.size
    no_nulos = df.notna().sum().sum()
    return round((no_nulos / total) * 100, 2)


def calcular_exactitud(df):
    """Porcentaje de valores entre 0 y 1 en columnas tipo porcentaje."""
    pct_cols = [c for c in df.columns if "pct_" in c]
    if not pct_cols:
        return np.nan
    total = df[pct_cols].size
    validos = df[pct_cols].apply(lambda x: x.between(0, 1)).sum().sum()
    return round((validos / total) * 100, 2)


def calcular_consistencia(df):
    """Evalúa la consistencia de los datos según reglas de CASOS_SPA y CASOS_SUI."""
    
    # REGLA CASOS_SPA == 0
    spa_vars = [
        "SITIOHABITUALCONSUMO_VIVIENDA",
        "SITIOHABITUALCONSUMO_PARQUE",
        "SITIOHABITUALCONSUMO_EST_EDUCATIVO",
        "SITIOHABITUALCONSUMO_BARES_TABERNAS",
        "SITIOHABITUALCONSUMO_VIA_PUBLICA",
        "SITIOHABITUALCONSUMO_CASA_AMIGOS",
    ]

    cond_spa_0 = df["casos_spa"] == 0
    if cond_spa_0.sum() > 0:
        incons_spa = df.loc[cond_spa_0, spa_vars].sum(axis=1) != 0
        consistencia_spa = 1 - (incons_spa.sum() / len(df[cond_spa_0]))
    else:
        consistencia_spa = 1.0

    # REGLA CASOS_SUI == 0
    sui_vars = [
        "enfermedades_dolorosas",
        "maltrato_sexual",
        "muerte_familiar",
        "conflicto_pareja",
        "problemas_economicos",
        "esc_educ",
        "problemas_juridicos",
        "problemas_laborales",
        "suicidio_amigo",
    ]

    cond_sui_0 = df["casos_sui"] == 0
    if cond_sui_0.sum() > 0:
        incons_sui = df.loc[cond_sui_0, sui_vars].sum(axis=1) != 0
        consistencia_sui = 1 - (incons_sui.sum() / len(df[cond_sui_0]))
    else:
        consistencia_sui = 1.0

    consistencia_global = (consistencia_spa + consistencia_sui) / 2
    return round(consistencia_global * 100, 2)


def calcular_unicidad(df):
    """Porcentaje de registros únicos."""
    total = len(df)
    unicos = len(df.drop_duplicates())
    return round((unicos / total) * 100, 2)


def extraer_nivel_educativo_num(nivel):
    """Extrae el número inicial del nivel educativo (ej: '5. secundaria' → 5)."""
    try:
        return int(str(nivel).split('.')[0])
    except:
        return np.nan


def calcular_validez(df):
    """Evalúa coherencia entre ciclo de vida y nivel educativo."""
    df = df.copy()
    df["nivel_num"] = df["nivel_educativo"].apply(extraer_nivel_educativo_num)

    condiciones_validas = (
        ((df["ciclo_vida"] == "infancia") & (df["nivel_num"] <= 5)) |
        ((df["ciclo_vida"] == "adolescencia") & (df["nivel_num"] <= 7)) |
        (df["ciclo_vida"].isin(["juventud", "adultez", "vejez"]))
    )
    total = df["nivel_num"].notna().sum()
    validos = condiciones_validas.sum()
    return round((validos / total) * 100, 2)


def calcular_actualidad(df):
    """Porcentaje de registros entre 2015 y 2025."""
    if "anio" not in df.columns:
        return np.nan
    total = df["anio"].notna().sum()
    validos = df["anio"].between(2015, 2025).sum()
    return round((validos / total) * 100, 2)


def ejecutar_quality_check(input_csv, output_report=None, output_excel=None):
    """
    Ejecuta el quality check completo sobre el dataset.
    
    Args:
        input_csv: Ruta al archivo CSV a analizar
        output_report: Ruta donde guardar el reporte JSON (opcional)
        output_excel: Ruta donde guardar el reporte Excel detallado (opcional)
    
    Returns:
        dict: Diccionario con las métricas de calidad
    """
    logger.info(f"Cargando dataset desde {input_csv}")
    df = pd.read_csv(input_csv)
    logger.info(f"Dataset cargado: {df.shape[0]} filas, {df.shape[1]} columnas")
    
    # Variables importantes a analizar
    variables = [
        "anio", "upz", "sexo", "ciclo_vida", "nivel_educativo", "casos_spa",
        "SITIOHABITUALCONSUMO_VIVIENDA", "SITIOHABITUALCONSUMO_PARQUE",
        "SITIOHABITUALCONSUMO_EST_EDUCATIVO", "SITIOHABITUALCONSUMO_BARES_TABERNAS",
        "SITIOHABITUALCONSUMO_VIA_PUBLICA", "SITIOHABITUALCONSUMO_CASA_AMIGOS",
        "clasificacion", "casos_sui", "enfermedades_dolorosas", "maltrato_sexual",
        "muerte_familiar", "conflicto_pareja", "problemas_economicos", "esc_educ",
        "problemas_juridicos", "problemas_laborales", "suicidio_amigo",
        "pct_enfermedades_dolorosas", "pct_maltrato_sexual", "pct_muerte_familiar",
        "pct_conflicto_pareja", "pct_problemas_economicos", "pct_esc_educ",
        "pct_problemas_juridicos", "pct_problemas_laborales", "pct_suicidio_amigo",
        "pct_sitiohabitualconsumo_vivienda", "pct_sitiohabitualconsumo_parque",
        "pct_sitiohabitualconsumo_est_educativo", "pct_sitiohabitualconsumo_bares_tabernas",
        "pct_sitiohabitualconsumo_via_publica", "pct_sitiohabitualconsumo_casa_amigos"
    ]
    
    # Generar reporte por columna
    logger.info("Generando reportes por columna...")
    reporte_columnas = {}
    for col in variables:
        if col in df.columns:
            filas = generar_reporte_columna(df, col)
            reporte_columnas[col] = pd.DataFrame(filas)
        else:
            logger.warning(f"Columna '{col}' no encontrada en el dataset")
    
    # Calcular métricas globales
    logger.info("Calculando métricas de calidad...")
    completitud = calcular_completitud(df)
    exactitud = calcular_exactitud(df)
    consistencia = calcular_consistencia(df)
    unicidad = calcular_unicidad(df)
    validez = calcular_validez(df)
    actualidad = calcular_actualidad(df)
    
    reporte_metricas = {
        "completitud": {
            "valor": completitud,
            "meta": 95,
            "cumple": completitud >= 95,
            "observaciones": "No existen valores faltantes" if completitud == 100 else "Algunas columnas con valores nulos"
        },
        "exactitud": {
            "valor": exactitud,
            "meta": 99,
            "cumple": exactitud >= 99,
            "observaciones": "Todos los valores correctos" if exactitud == 100 else "Algunos valores ligeramente fuera de rango"
        },
        "consistencia": {
            "valor": consistencia,
            "meta": 95,
            "cumple": consistencia >= 95,
            "observaciones": "Perfectamente consistentes" if consistencia == 100 else "Leves diferencias en la suma de porcentajes"
        },
        "unicidad": {
            "valor": unicidad,
            "meta": 100,
            "cumple": unicidad == 100,
            "observaciones": "No hay duplicados" if unicidad == 100 else "Existen filas duplicadas"
        },
        "validez": {
            "valor": validez,
            "meta": 98,
            "cumple": validez >= 98,
            "observaciones": "Datos válidos en todos los casos" if validez == 100 else "Algunos infantes/adolescentes con niveles educativos altos"
        },
        "actualidad": {
            "valor": actualidad,
            "meta": 100,
            "cumple": actualidad == 100,
            "observaciones": "Todos los registros dentro del rango esperado" if actualidad == 100 else "Algunos años fuera del rango"
        }
    }
    
    # Crear reporte resumido
    logger.info("\n📊 REPORTE DE CALIDAD DE DATOS\n")
    logger.info(f"Completitud: {completitud}% {'✅' if completitud >= 95 else '❌'}")
    logger.info(f"Exactitud: {exactitud}% {'✅' if exactitud >= 99 else '❌'}")
    logger.info(f"Consistencia: {consistencia}% {'✅' if consistencia >= 95 else '❌'}")
    logger.info(f"Unicidad: {unicidad}% {'✅' if unicidad == 100 else '❌'}")
    logger.info(f"Validez: {validez}% {'✅' if validez >= 98 else '❌'}")
    logger.info(f"Actualidad: {actualidad}% {'✅' if actualidad == 100 else '❌'}")
    
    # Guardar reporte JSON
    if output_report:
        logger.info(f"Guardando reporte JSON en {output_report}")
        with open(output_report, 'w', encoding='utf-8') as f:
            json.dump(reporte_metricas, f, indent=2, ensure_ascii=False)
    
    # Guardar reporte Excel detallado
    if output_excel:
        logger.info(f"Guardando reporte Excel en {output_excel}")
        with pd.ExcelWriter(output_excel) as writer:
            for col, df_col in reporte_columnas.items():
                df_col.to_excel(writer, sheet_name=col[:31], index=False)
    
    return reporte_metricas


def main():
    parser = argparse.ArgumentParser(description='Quality Check para datasets ODS3')
    parser.add_argument('--input', required=True, help='Archivo CSV de entrada')
    parser.add_argument('--report', help='Archivo JSON para guardar el reporte')
    parser.add_argument('--excel', help='Archivo Excel para guardar el reporte detallado')
    
    args = parser.parse_args()
    
    try:
        metricas = ejecutar_quality_check(args.input, args.report, args.excel)
        
        # Verificar si todas las métricas cumplen
        todas_cumplen = all(m['cumple'] for m in metricas.values())
        
        if todas_cumplen:
            logger.info("✅ Quality Check APROBADO - Todas las métricas cumplen los estándares")
            return 0
        else:
            logger.warning("⚠️ Quality Check con ADVERTENCIAS - Algunas métricas no cumplen los estándares")
            return 0  # No falla el pipeline, solo advierte
            
    except Exception as e:
        logger.error(f"Error ejecutando Quality Check: {e}")
        raise


if __name__ == "__main__":
    main()
