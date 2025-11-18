from kafka import KafkaConsumer
import json
import pandas as pd

# Configuración de Kafka
KAFKA_TOPIC = 'ods3_kpis'
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']

def calcular_kpis(df):
		import unicodedata
		BAD_SET = {"", "nan", "sin dato", "upz sin asignar", "s/d", "n.a.", "n.a", "na"}
		# Normalización y limpieza siguiendo las transformaciones ETL
		def _normalize_text(v):
			if v is None or (isinstance(v, float) and pd.isna(v)):
				return None
			s = str(v).strip().lower()
			s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("utf-8")
			return s

		def unify_missing(series, bad=BAD_SET):
			s = series.map(_normalize_text, na_action="ignore")
			mask = s.isna() | s.isin(set(bad))
			return s.where(~mask, "sin dato")

		def standardize_sex(series):
			s = series.map(_normalize_text, na_action="ignore")
			s = s.replace({
				"h": "hombre", "m": "mujer",
				"masculino": "hombre", "femenino": "mujer",
				"male": "hombre", "female": "mujer",
			})
			s = s.where(s.isin({"hombre", "mujer", "sin dato"}), "otro").fillna("sin dato")
			return s

		def normalize_ciclo_vida(series):
			mapping = {
				"0  5 primera infancia": "primera infancia",
				"6  11 infancia": "infancia",
				"12  17 adolescencia": "adolescencia",
				"18  28 juventud": "juventud",
				"29  59 adultez": "adultez",
				">60 vejez": "vejez",
				"primera infancia": "primera infancia",
				"infancia": "infancia",
				"adolescencia": "adolescencia",
				"juventud": "juventud",
				"adultez": "adultez",
				"vejez": "vejez",
			}
			s = series.map(_normalize_text, na_action="ignore")
			return s.replace(mapping).fillna("sin dato")

		def normalize_nivel_educativo(series):
			s = series.map(_normalize_text, na_action="ignore")
			s = s.str.replace("tecnico pos secundaria", "tecnico post-secundaria", regex=False)
			s = s.str.replace("tecnico post secundaria", "tecnico post-secundaria", regex=False)
			s = s.str.replace(" completo", " completa", regex=False)
			s = s.str.replace(" incompleto", " incompleta", regex=False)
			return s.fillna("sin dato")

		# Aplicar transformaciones
		for col in ['upz', 'sexo', 'ciclo_vida', 'nivel_educativo']:
			if col in df.columns:
				if col == 'upz':
					df[col] = unify_missing(df[col])
				elif col == 'sexo':
					df[col] = standardize_sex(df[col])
				elif col == 'ciclo_vida':
					df[col] = normalize_ciclo_vida(df[col])
				elif col == 'nivel_educativo':
					df[col] = normalize_nivel_educativo(df[col])

		# Limpieza estricta de la clave
		key = ['anio', 'upz', 'sexo', 'ciclo_vida', 'nivel_educativo']
		df = df.dropna(subset=key)
		# Acumular condiciones de limpieza para todas las columnas clave
		mask = pd.Series([True] * len(df), index=df.index)
		for c in ['upz', 'sexo', 'ciclo_vida', 'nivel_educativo']:
			s = df[c].astype("string").str.strip().str.lower()
			mask &= ~s.isin(BAD_SET)
		df = df.loc[mask]

		# Filtrar solo jóvenes
		df_jovenes = df[df['ciclo_vida'].isin(['adolescencia', 'juventud'])].copy()
		print(f"Total registros jóvenes: {len(df_jovenes)}")

		# Ejemplo de KPI: total de casos de SPA y suicidas por año y sexo
		if 'casos_spa' in df_jovenes.columns:
			kpi_spa = df_jovenes.groupby(['anio', 'sexo'])['casos_spa'].sum()
			print("\nKPI: Total casos SPA por año y sexo:")
			print(kpi_spa)
		if 'casos_sui' in df_jovenes.columns:
			kpi_sui = df_jovenes.groupby(['anio', 'sexo'])['casos_sui'].sum()
			print("\nKPI: Total casos suicidas por año y sexo:")
			print(kpi_sui)
		# Puedes agregar más KPIs siguiendo la lógica de los scripts ETL

if __name__ == '__main__':
	consumer = KafkaConsumer(
		KAFKA_TOPIC,
		bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
		value_deserializer=lambda m: json.loads(m.decode('utf-8')),
		auto_offset_reset='earliest',
		enable_auto_commit=True
	)
	print('Esperando datos de Kafka...')

	tablas = [
		'dim_tiempo',
		'dim_ubicacion',
		'dim_perfil_demografico',
		'dim_clasificacion',
		'fact_casos'
	]
	datos_acumulados = {tabla: [] for tabla in tablas}

	for message in consumer:
		msg = message.value
		tabla = msg.get('tabla')
		registro = msg.get('registro')
		if tabla in datos_acumulados:
			datos_acumulados[tabla].append(registro)

		# Procesar KPIs cada vez que se acumulen 5000 registros en fact_casos y haya datos en todas las tablas
		if len(datos_acumulados['fact_casos']) >= 5000 and all(len(datos_acumulados[t]) > 0 for t in tablas):
			df_fact = pd.DataFrame(datos_acumulados['fact_casos'])
			df_tiempo = pd.DataFrame(datos_acumulados['dim_tiempo'])
			df_ubicacion = pd.DataFrame(datos_acumulados['dim_ubicacion'])
			df_perfil = pd.DataFrame(datos_acumulados['dim_perfil_demografico'])
			df_clasificacion = pd.DataFrame(datos_acumulados['dim_clasificacion'])

			df = df_fact.merge(df_tiempo, left_on='id_tiempo', right_on='id_tiempo', how='left')
			df = df.merge(df_ubicacion, left_on='id_ubicacion', right_on='id_ubicacion', how='left')
			df = df.merge(df_perfil, left_on='id_perfil', right_on='id_perfil', how='left')
			df = df.merge(df_clasificacion, left_on='id_clasificacion', right_on='id_clasificacion', how='left')

			df = df.rename(columns={
				'anio': 'anio',
				'upz': 'upz',
				'sexo': 'sexo',
				'ciclo_vida': 'ciclo_vida',
				'nivel_educativo': 'nivel_educativo',
				'clasificacion': 'clasificacion'
			})

			calcular_kpis(df)
			# Limpiar solo los registros de fact_casos, mantener las dimensiones
			datos_acumulados['fact_casos'] = []
