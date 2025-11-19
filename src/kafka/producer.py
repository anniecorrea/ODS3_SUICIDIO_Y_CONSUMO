import mysql.connector
from kafka import KafkaProducer
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
    'port': 3307,
    'user': 'root',
    'password': 'airflow',
    'database': 'ODS3_SPA_SUICIDAS'
}

def fetch_data_from_db():
    """Fetch data from all dimensions and the fact table."""
    connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor(dictionary=True)

    # Fetch data from dimensions
    cursor.execute("SELECT * FROM dim_tiempo")
    dim_tiempo = {row['id_tiempo']: row for row in cursor.fetchall()}

    cursor.execute("SELECT * FROM dim_ubicacion")
    dim_ubicacion = {row['id_ubicacion']: row for row in cursor.fetchall()}

    cursor.execute("SELECT * FROM dim_perfil_demografico")
    dim_perfil = {row['id_perfil']: row for row in cursor.fetchall()}

    cursor.execute("SELECT * FROM dim_clasificacion")
    dim_clasificacion = {row['id_clasificacion']: row for row in cursor.fetchall()}

    # Fetch data from fact table
    cursor.execute("SELECT * FROM fact_casos")
    fact_casos = cursor.fetchall()

    # Combine data into a flat structure
    combined_data = []
    for fact in fact_casos:
        combined_record = {
            **fact,
            **dim_tiempo.get(fact['id_tiempo'], {}),
            **dim_ubicacion.get(fact['id_ubicacion'], {}),
            **dim_perfil.get(fact['id_perfil'], {}),
            **dim_clasificacion.get(fact['id_clasificacion'], {})
        }
        combined_data.append(combined_record)

    cursor.close()
    connection.close()
    return combined_data

def send_to_kafka(data):
    """Send data to Kafka topic."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    for record in data:
        producer.send(KAFKA_TOPIC, record)
        logger.info("Sent record to Kafka: %s", record)  # Added logging indicator
    producer.flush()

def main():
    data = fetch_data_from_db()
    send_to_kafka(data)

if __name__ == "__main__":
    main()