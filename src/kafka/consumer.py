import mysql.connector
from kafka import KafkaConsumer
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_TOPIC = 'ods3_topic'
KAFKA_SERVER = 'localhost:9092'

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'root',
    'database': 'ods3_kpis'
}

def create_database_and_tables():
    """Create the database and tables if they do not exist."""
    connection = mysql.connector.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password']
    )
    cursor = connection.cursor()

    # Create database if not exists
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
    cursor.execute(f"USE {DB_CONFIG['database']}")

    # Create table if not exists
    create_table_query = (
        "CREATE TABLE IF NOT EXISTS fact_casos ("
        "id_fact INT AUTO_INCREMENT PRIMARY KEY,"
        "id_tiempo INT,"
        "anio INT,"
        "id_ubicacion INT,"
        "upz VARCHAR(100),"  # Nueva columna
        "id_perfil INT,"
        "sexo VARCHAR(20),"  # Nueva columna
        "ciclo_vida VARCHAR(50),"  # Nueva columna
        "nivel_educativo VARCHAR(100),"  # Nueva columna
        "id_clasificacion INT,"
        "clasificacion VARCHAR(100),"  # Nueva columna
        "casos_spa INT,"
        "casos_sui INT,"
        "sitio_vivienda INT,"
        "sitio_parque INT,"
        "sitio_est_educativo INT,"
        "sitio_bares_tabernas INT,"
        "sitio_via_publica INT,"
        "sitio_casa_amigos INT,"
        "pct_sitio_vivienda FLOAT,"
        "pct_sitio_parque FLOAT,"
        "pct_sitio_est_educativo FLOAT,"
        "pct_sitio_bares_tabernas FLOAT,"
        "pct_sitio_via_publica FLOAT,"
        "pct_sitio_casa_amigos FLOAT,"
        "enfermedades_dolorosas INT,"
        "maltrato_sexual INT,"
        "muerte_familiar INT,"
        "conflicto_pareja INT,"
        "problemas_economicos INT,"
        "esc_educ INT,"
        "problemas_juridicos INT,"
        "problemas_laborales INT,"
        "suicidio_amigo INT,"
        "pct_enfermedades_dolorosas FLOAT,"
        "pct_maltrato_sexual FLOAT,"
        "pct_muerte_familiar FLOAT,"
        "pct_conflicto_pareja FLOAT,"
        "pct_problemas_economicos FLOAT,"
        "pct_esc_educ FLOAT,"
        "pct_problemas_juridicos FLOAT,"
        "pct_problemas_laborales FLOAT,"
        "pct_suicidio_amigo FLOAT"
        ")"
    )
    cursor.execute(create_table_query)

    connection.commit()
    cursor.close()
    connection.close()

def save_to_db(filtered_data):
    """Save filtered data to the database."""
    connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor()

    insert_query = (
        "INSERT INTO fact_casos (id_tiempo, anio, id_ubicacion, upz, id_perfil, sexo, ciclo_vida, nivel_educativo, id_clasificacion, clasificacion, "
        "casos_spa, casos_sui, sitio_vivienda, sitio_parque, sitio_est_educativo, sitio_bares_tabernas, sitio_via_publica, sitio_casa_amigos, "
        "pct_sitio_vivienda, pct_sitio_parque, pct_sitio_est_educativo, pct_sitio_bares_tabernas, pct_sitio_via_publica, pct_sitio_casa_amigos, "
        "enfermedades_dolorosas, maltrato_sexual, muerte_familiar, conflicto_pareja, problemas_economicos, esc_educ, problemas_juridicos, problemas_laborales, suicidio_amigo, "
        "pct_enfermedades_dolorosas, pct_maltrato_sexual, pct_muerte_familiar, pct_conflicto_pareja, pct_problemas_economicos, pct_esc_educ, pct_problemas_juridicos, pct_problemas_laborales, pct_suicidio_amigo) "
        "VALUES (%(id_tiempo)s, %(anio)s, %(id_ubicacion)s, %(upz)s, %(id_perfil)s, %(sexo)s, %(ciclo_vida)s, %(nivel_educativo)s, %(id_clasificacion)s, %(clasificacion)s, "
        "%(casos_spa)s, %(casos_sui)s, %(sitio_vivienda)s, %(sitio_parque)s, %(sitio_est_educativo)s, %(sitio_bares_tabernas)s, %(sitio_via_publica)s, %(sitio_casa_amigos)s, "
        "%(pct_sitio_vivienda)s, %(pct_sitio_parque)s, %(pct_sitio_est_educativo)s, %(pct_sitio_bares_tabernas)s, %(pct_sitio_via_publica)s, %(pct_sitio_casa_amigos)s, "
        "%(enfermedades_dolorosas)s, %(maltrato_sexual)s, %(muerte_familiar)s, %(conflicto_pareja)s, %(problemas_economicos)s, %(esc_educ)s, %(problemas_juridicos)s, %(problemas_laborales)s, %(suicidio_amigo)s, "
        "%(pct_enfermedades_dolorosas)s, %(pct_maltrato_sexual)s, %(pct_muerte_familiar)s, %(pct_conflicto_pareja)s, %(pct_problemas_economicos)s, %(pct_esc_educ)s, %(pct_problemas_juridicos)s, %(pct_problemas_laborales)s, %(pct_suicidio_amigo)s)"
    )

    for record in filtered_data:
        try:
            cursor.execute(insert_query, record)
            logger.info("Inserted record into database: %s", record)  # Added logging indicator
        except mysql.connector.Error as err:
            logger.error("Error inserting record: %s", err)

    connection.commit()
    cursor.close()
    connection.close()

def main():
    create_database_and_tables()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for message in consumer:
        record = message.value
        if 'ciclo_vida' not in record:
            logger.warning("Missing 'ciclo_vida' key in record: %s", record)
            continue

        if record['ciclo_vida'] in ['adolescencia', 'juventud']:
            save_to_db([record])

if __name__ == "__main__":
    main()