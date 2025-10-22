CREATE DATABASE IF NOT EXISTS ODS3_SPA_SUICIDAS;
USE ODS3_SPA_SUICIDAS;

-- ======================================
-- DIMENSION: TIEMPO
-- ======================================
CREATE TABLE IF NOT EXISTS dim_tiempo (
    id_tiempo INT AUTO_INCREMENT PRIMARY KEY,
    anio INT NOT NULL,
    UNIQUE (anio)
);

-- ======================================
-- DIMENSION: UBICACIÓN
-- ======================================
CREATE TABLE IF NOT EXISTS dim_ubicacion (
    id_ubicacion INT AUTO_INCREMENT PRIMARY KEY,
    upz VARCHAR(100) NOT NULL,
    UNIQUE (upz)
);

-- ======================================
-- DIMENSION: PERFIL DEMOGRÁFICO
-- ======================================
CREATE TABLE IF NOT EXISTS dim_perfil_demografico (
    id_perfil INT AUTO_INCREMENT PRIMARY KEY,
    sexo VARCHAR(20),
    ciclo_vida VARCHAR(50),
    nivel_educativo VARCHAR(100),
    UNIQUE (sexo, ciclo_vida, nivel_educativo)
);

-- ======================================
-- DIMENSION: CLASIFICACIÓN
-- ======================================
CREATE TABLE IF NOT EXISTS dim_clasificacion (
    id_clasificacion INT AUTO_INCREMENT PRIMARY KEY,
    clasificacion VARCHAR(100) NOT NULL,
    UNIQUE (clasificacion)
);

-- ======================================
-- TABLA DE HECHOS
-- ======================================
CREATE TABLE IF NOT EXISTS fact_casos (
    id_fact INT AUTO_INCREMENT PRIMARY KEY,
    
    -- Llaves foráneas
    id_tiempo INT NOT NULL,
    id_ubicacion INT NOT NULL,
    id_perfil INT NOT NULL,
    id_clasificacion INT NOT NULL,
    
    -- Medidas
    casos_spa INT,
    casos_sui INT,

    -- Sitios habituales
    sitio_vivienda INT,
    sitio_parque INT,
    sitio_est_educativo INT,
    sitio_bares_tabernas INT,
    sitio_via_publica INT,
    sitio_casa_amigos INT,

    -- Porcentajes sitios
    pct_sitio_vivienda FLOAT,
    pct_sitio_parque FLOAT,
    pct_sitio_est_educativo FLOAT,
    pct_sitio_bares_tabernas FLOAT,
    pct_sitio_via_publica FLOAT,
    pct_sitio_casa_amigos FLOAT,

    -- Causas
    enfermedades_dolorosas INT,
    maltrato_sexual INT,
    muerte_familiar INT,
    conflicto_pareja INT,
    problemas_economicos INT,
    esc_educ INT,
    problemas_juridicos INT,
    problemas_laborales INT,
    suicidio_amigo INT,

    -- Porcentajes causas
    pct_enfermedades_dolorosas FLOAT,
    pct_maltrato_sexual FLOAT,
    pct_muerte_familiar FLOAT,
    pct_conflicto_pareja FLOAT,
    pct_problemas_economicos FLOAT,
    pct_esc_educ FLOAT,
    pct_problemas_juridicos FLOAT,
    pct_problemas_laborales FLOAT,
    pct_suicidio_amigo FLOAT,

    -- Relaciones
    FOREIGN KEY (id_tiempo) REFERENCES dim_tiempo(id_tiempo),
    FOREIGN KEY (id_ubicacion) REFERENCES dim_ubicacion(id_ubicacion),
    FOREIGN KEY (id_perfil) REFERENCES dim_perfil_demografico(id_perfil),
    FOREIGN KEY (id_clasificacion) REFERENCES dim_clasificacion(id_clasificacion)
);
