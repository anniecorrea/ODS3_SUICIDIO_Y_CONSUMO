import mysql.connector
from kafka import KafkaProducer
import json

# Configuración de MySQL
MYSQL_HOST = 'localhost'
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'airflow'
MYSQL_DATABASE = 'ODS3_SPA_SUICIDAS'
MYSQL_PORT = 3307

# Configuración de Kafka
KAFKA_TOPIC = 'ods3_kpis'
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']

def extraer_todas_tablas():
	tablas = [
		'dim_tiempo',
		'dim_ubicacion',
		'dim_perfil_demografico',
		'dim_clasificacion',
		'fact_casos'
	]
	connection = mysql.connector.connect(
		host=MYSQL_HOST,
		user=MYSQL_USER,
		password=MYSQL_PASSWORD,
		database=MYSQL_DATABASE,
		port=MYSQL_PORT
	)
	cursor = connection.cursor(dictionary=True)
	datos_por_tabla = {}
	for tabla in tablas:
		cursor.execute(f'SELECT * FROM {tabla}')
		datos_por_tabla[tabla] = cursor.fetchall()
	cursor.close()
	connection.close()
	return datos_por_tabla

def enviar_a_kafka(datos_por_tabla):
	producer = KafkaProducer(
		bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
		value_serializer=lambda v: json.dumps(v).encode('utf-8')
	)
	# Enviar dimensiones completas primero
	for tabla in ['dim_tiempo', 'dim_ubicacion', 'dim_perfil_demografico', 'dim_clasificacion']:
		for registro in datos_por_tabla[tabla]:
			mensaje = {
				'tabla': tabla,
				'registro': registro
			}
			print(f"Enviando a Kafka | Tabla: {tabla} | Registro: {registro}")
			producer.send(KAFKA_TOPIC, mensaje)
	# Enviar fact_casos en lotes de 10,000
	fact_casos = datos_por_tabla['fact_casos']
	batch_size = 10000
	for i in range(0, len(fact_casos), batch_size):
		batch = fact_casos[i:i+batch_size]
		for registro in batch:
			mensaje = {
				'tabla': 'fact_casos',
				'registro': registro
			}
			print(f"Enviando a Kafka | Tabla: fact_casos | Registro: {registro}")
			producer.send(KAFKA_TOPIC, mensaje)
	producer.flush()

if __name__ == '__main__':
	datos_por_tabla = extraer_todas_tablas()
	enviar_a_kafka(datos_por_tabla)
	print('Datos de todas las tablas enviados a Kafka')