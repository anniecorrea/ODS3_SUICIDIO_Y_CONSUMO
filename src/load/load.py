import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def crear_base_y_tablas(cursor, database):
    """
    Crea la base de datos y las tablas del modelo dimensional si no existen.
    """
    logger.info(f"Verificando existencia de la base de datos: {database}")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    cursor.execute(f"USE {database}")
    logger.info("✓ Base de datos lista")

    # Crear tablas
    tablas_sql = [

        # DIM TIEMPO
        """
        CREATE TABLE IF NOT EXISTS dim_tiempo (
            id_tiempo INT AUTO_INCREMENT PRIMARY KEY,
            anio INT NOT NULL,
            UNIQUE (anio)
        )
        """,

        # DIM UBICACION
        """
        CREATE TABLE IF NOT EXISTS dim_ubicacion (
            id_ubicacion INT AUTO_INCREMENT PRIMARY KEY,
            upz VARCHAR(100) NOT NULL,
            UNIQUE (upz)
        )
        """,

        # DIM PERFIL DEMOGRÁFICO
        """
        CREATE TABLE IF NOT EXISTS dim_perfil_demografico (
            id_perfil INT AUTO_INCREMENT PRIMARY KEY,
            sexo VARCHAR(20),
            ciclo_vida VARCHAR(50),
            nivel_educativo VARCHAR(100),
            UNIQUE (sexo, ciclo_vida, nivel_educativo)
        )
        """,

        # DIM CLASIFICACION
        """
        CREATE TABLE IF NOT EXISTS dim_clasificacion (
            id_clasificacion INT AUTO_INCREMENT PRIMARY KEY,
            clasificacion VARCHAR(100) NOT NULL,
            UNIQUE (clasificacion)
        )
        """,

        # FACT TABLE
        """
        CREATE TABLE IF NOT EXISTS fact_casos (
            id_fact INT AUTO_INCREMENT PRIMARY KEY,

            id_tiempo INT NOT NULL,
            id_ubicacion INT NOT NULL,
            id_perfil INT NOT NULL,
            id_clasificacion INT NOT NULL,

            casos_spa INT,
            casos_sui INT,

            sitio_vivienda INT,
            sitio_parque INT,
            sitio_est_educativo INT,
            sitio_bares_tabernas INT,
            sitio_via_publica INT,
            sitio_casa_amigos INT,

            pct_sitio_vivienda FLOAT,
            pct_sitio_parque FLOAT,
            pct_sitio_est_educativo FLOAT,
            pct_sitio_bares_tabernas FLOAT,
            pct_sitio_via_publica FLOAT,
            pct_sitio_casa_amigos FLOAT,

            enfermedades_dolorosas INT,
            maltrato_sexual INT,
            muerte_familiar INT,
            conflicto_pareja INT,
            problemas_economicos INT,
            esc_educ INT,
            problemas_juridicos INT,
            problemas_laborales INT,
            suicidio_amigo INT,

            pct_enfermedades_dolorosas FLOAT,
            pct_maltrato_sexual FLOAT,
            pct_muerte_familiar FLOAT,
            pct_conflicto_pareja FLOAT,
            pct_problemas_economicos FLOAT,
            pct_esc_educ FLOAT,
            pct_problemas_juridicos FLOAT,
            pct_problemas_laborales FLOAT,
            pct_suicidio_amigo FLOAT,

            FOREIGN KEY (id_tiempo) REFERENCES dim_tiempo(id_tiempo),
            FOREIGN KEY (id_ubicacion) REFERENCES dim_ubicacion(id_ubicacion),
            FOREIGN KEY (id_perfil) REFERENCES dim_perfil_demografico(id_perfil),
            FOREIGN KEY (id_clasificacion) REFERENCES dim_clasificacion(id_clasificacion)
        )
        """
    ]

    for sql in tablas_sql:
        cursor.execute(sql)
    logger.info("✓ Tablas creadas o verificadas correctamente")


def cargar_datos_modelo_dimensional(ruta_csv, host, database, user, password, port=3306, batch_size=1000):
    """
    Carga datos limpios desde CSV al modelo dimensional en MySQL
    
    Args:
        ruta_csv (str): Ruta al archivo CSV con datos limpios
        host (str): Host del servidor MySQL
        database (str): Nombre de la base de datos
        user (str): Usuario de MySQL
        password (str): Contraseña de MySQL
        port (int): Puerto de MySQL (default: 3306)
        batch_size (int): Tamaño de lote para commits (default: 1000)
    
    Returns:
        dict: Estadísticas de la carga
    """
    
    # Cachés para IDs de dimensiones
    cache_tiempo = {}
    cache_ubicacion = {}
    cache_perfil = {}
    cache_clasificacion = {}
    
    connection = None
    cursor = None
    
    try:
        # Leer CSV
        logger.info(f"Leyendo archivo CSV: {ruta_csv}")
        df = pd.read_csv(ruta_csv)
        logger.info(f"Total de registros a cargar: {len(df)}")
        
        # Conectar a MySQL
        logger.info("Conectando a MySQL...")
        connection = mysql.connector.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        cursor = connection.cursor()
        logger.info("✓ Conexión establecida")
        
        
        def get_or_create_tiempo(anio):
            if anio in cache_tiempo:
                return cache_tiempo[anio]
            
            cursor.execute("SELECT id_tiempo FROM dim_tiempo WHERE anio = %s", (anio,))
            result = cursor.fetchone()
            
            if result:
                id_tiempo = result[0]
            else:
                cursor.execute("INSERT INTO dim_tiempo (anio) VALUES (%s)", (anio,))
                id_tiempo = cursor.lastrowid
            
            cache_tiempo[anio] = id_tiempo
            return id_tiempo
        
        def get_or_create_ubicacion(upz):
            if upz in cache_ubicacion:
                return cache_ubicacion[upz]
            
            cursor.execute("SELECT id_ubicacion FROM dim_ubicacion WHERE upz = %s", (upz,))
            result = cursor.fetchone()
            
            if result:
                id_ubicacion = result[0]
            else:
                cursor.execute("INSERT INTO dim_ubicacion (upz) VALUES (%s)", (upz,))
                id_ubicacion = cursor.lastrowid
            
            cache_ubicacion[upz] = id_ubicacion
            return id_ubicacion
        
        def get_or_create_perfil(sexo, ciclo_vida, nivel_educativo):
            key = (sexo, ciclo_vida, nivel_educativo)
            if key in cache_perfil:
                return cache_perfil[key]
            
            cursor.execute(
                "SELECT id_perfil FROM dim_perfil_demografico WHERE sexo = %s AND ciclo_vida = %s AND nivel_educativo = %s",
                key
            )
            result = cursor.fetchone()
            
            if result:
                id_perfil = result[0]
            else:
                cursor.execute(
                    "INSERT INTO dim_perfil_demografico (sexo, ciclo_vida, nivel_educativo) VALUES (%s, %s, %s)",
                    key
                )
                id_perfil = cursor.lastrowid
            
            cache_perfil[key] = id_perfil
            return id_perfil
        
        def get_or_create_clasificacion(clasificacion):
            if clasificacion in cache_clasificacion:
                return cache_clasificacion[clasificacion]
            
            cursor.execute("SELECT id_clasificacion FROM dim_clasificacion WHERE clasificacion = %s", (clasificacion,))
            result = cursor.fetchone()
            
            if result:
                id_clasificacion = result[0]
            else:
                cursor.execute("INSERT INTO dim_clasificacion (clasificacion) VALUES (%s)", (clasificacion,))
                id_clasificacion = cursor.lastrowid
            
            cache_clasificacion[clasificacion] = id_clasificacion
            return id_clasificacion
        
        # ==========================================
        # CARGA DE TABLA DE HECHOS
        # ==========================================
        
        logger.info("Iniciando carga de datos...")
        
        insert_query = """
        INSERT INTO fact_casos (
            id_tiempo, id_ubicacion, id_perfil, id_clasificacion,
            casos_spa, casos_sui,
            sitio_vivienda, sitio_parque, sitio_est_educativo,
            sitio_bares_tabernas, sitio_via_publica, sitio_casa_amigos,
            pct_sitio_vivienda, pct_sitio_parque, pct_sitio_est_educativo,
            pct_sitio_bares_tabernas, pct_sitio_via_publica, pct_sitio_casa_amigos,
            enfermedades_dolorosas, maltrato_sexual, muerte_familiar,
            conflicto_pareja, problemas_economicos, esc_educ,
            problemas_juridicos, problemas_laborales, suicidio_amigo,
            pct_enfermedades_dolorosas, pct_maltrato_sexual, pct_muerte_familiar,
            pct_conflicto_pareja, pct_problemas_economicos, pct_esc_educ,
            pct_problemas_juridicos, pct_problemas_laborales, pct_suicidio_amigo
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        registros_procesados = 0
        
        for idx, row in df.iterrows():
            # Obtener IDs de dimensiones
            id_tiempo = get_or_create_tiempo(int(row['anio']))
            id_ubicacion = get_or_create_ubicacion(str(row['upz']))
            id_perfil = get_or_create_perfil(
                str(row['sexo']),
                str(row['ciclo_vida']),
                str(row['nivel_educativo'])
            )
            id_clasificacion = get_or_create_clasificacion(str(row['clasificacion']))
            
            # Preparar valores para inserción
            valores = (
                id_tiempo, id_ubicacion, id_perfil, id_clasificacion,
                int(row['casos_spa']), int(row['casos_sui']),
                int(row.get('SITIOHABITUALCONSUMO_VIVIENDA', 0)),
                int(row.get('SITIOHABITUALCONSUMO_PARQUE', 0)),
                int(row.get('SITIOHABITUALCONSUMO_EST_EDUCATIVO', 0)),
                int(row.get('SITIOHABITUALCONSUMO_BARES_TABERNAS', 0)),
                int(row.get('SITIOHABITUALCONSUMO_VIA_PUBLICA', 0)),
                int(row.get('SITIOHABITUALCONSUMO_CASA_AMIGOS', 0)),
                float(row.get('pct_sitiohabitualconsumo_vivienda', 0)),
                float(row.get('pct_sitiohabitualconsumo_parque', 0)),
                float(row.get('pct_sitiohabitualconsumo_est_educativo', 0)),
                float(row.get('pct_sitiohabitualconsumo_bares_tabernas', 0)),
                float(row.get('pct_sitiohabitualconsumo_via_publica', 0)),
                float(row.get('pct_sitiohabitualconsumo_casa_amigos', 0)),
                int(row.get('enfermedades_dolorosas', 0)),
                int(row.get('maltrato_sexual', 0)),
                int(row.get('muerte_familiar', 0)),
                int(row.get('conflicto_pareja', 0)),
                int(row.get('problemas_economicos', 0)),
                int(row.get('esc_educ', 0)),
                int(row.get('problemas_juridicos', 0)),
                int(row.get('problemas_laborales', 0)),
                int(row.get('suicidio_amigo', 0)),
                float(row.get('pct_enfermedades_dolorosas', 0)),
                float(row.get('pct_maltrato_sexual', 0)),
                float(row.get('pct_muerte_familiar', 0)),
                float(row.get('pct_conflicto_pareja', 0)),
                float(row.get('pct_problemas_economicos', 0)),
                float(row.get('pct_esc_educ', 0)),
                float(row.get('pct_problemas_juridicos', 0)),
                float(row.get('pct_problemas_laborales', 0)),
                float(row.get('pct_suicidio_amigo', 0))
            )
            
            cursor.execute(insert_query, valores)
            registros_procesados += 1
            
            # Commit por lotes
            if registros_procesados % batch_size == 0:
                connection.commit()
                logger.info(f"Procesados {registros_procesados}/{len(df)} registros ({(registros_procesados/len(df)*100):.1f}%)")
        
        # Commit final
        connection.commit()
        
        # Estadísticas finales
        estadisticas = {
            'registros_procesados': registros_procesados,
            'dimensiones_tiempo': len(cache_tiempo),
            'dimensiones_ubicacion': len(cache_ubicacion),
            'dimensiones_perfil': len(cache_perfil),
            'dimensiones_clasificacion': len(cache_clasificacion)
        }
        
        logger.info(f"✓ Carga completa: {registros_procesados} registros insertados")
        logger.info(f"  - Dimensiones tiempo: {estadisticas['dimensiones_tiempo']}")
        logger.info(f"  - Dimensiones ubicación: {estadisticas['dimensiones_ubicacion']}")
        logger.info(f"  - Dimensiones perfil: {estadisticas['dimensiones_perfil']}")
        logger.info(f"  - Dimensiones clasificación: {estadisticas['dimensiones_clasificacion']}")
        
        return estadisticas
        
    except Exception as e:
        logger.error(f"✗ Error durante la carga: {e}")
        if connection:
            connection.rollback()
        raise
        
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            logger.info("Conexión cerrada")


# ==============================================
#USO
# ==============================================
if __name__ == "__main__":
    
    # Configuración
    RUTA_CSV = '../../data_out/merged_spa_suicidas.csv'
    DB_HOST = 'localhost'
    DB_NAME = 'ODS3_SPA_SUICIDAS'
    DB_USER = 'root'
    DB_PASSWORD = 'annie'
    DB_PORT = 3306
    BATCH_SIZE = 1000

    try:
        # Paso 1: Conectar al servidor (sin base)
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        cursor = connection.cursor()
        logger.info("Conexión inicial al servidor MySQL establecida")

        # Paso 2: Crear base de datos y tablas
        crear_base_y_tablas(cursor, DB_NAME)
        connection.commit()
        cursor.close()
        connection.close()
        logger.info("✓ Base de datos y tablas creadas/verificadas")

        # Paso 3: Ejecutar la carga
        stats = cargar_datos_modelo_dimensional(
            ruta_csv=RUTA_CSV,
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
            batch_size=BATCH_SIZE
        )

    except Error as e:
        logger.error(f"Error general: {e}")

    finally:
        if connection.is_connected():
            connection.close()
