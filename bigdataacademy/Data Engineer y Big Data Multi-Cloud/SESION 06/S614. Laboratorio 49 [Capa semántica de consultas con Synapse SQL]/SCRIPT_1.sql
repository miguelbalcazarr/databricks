-- Por defecto, Azure Synapse trabaja sobre la base de datos "master"
-- Esta base de datos sólo la deberemos usar para crear otras bases de datos

-- Creamos la BASE DE DATOS del delta lake
-- Tendrá soporte para caracteres especiales (Latin1_General_100_BIN2_UTF8)
CREATE DATABASE DELTALAKE COLLATE Latin1_General_100_BIN2_UTF8;

-- Cambiamos a esta base de datos para trabajar sobre ella
USE DELTALAKE;

-- Creamos el DATA SOURCE que apunte al blob storage del delta lake
-- IMPORTANTE: CAMBIAR "XXX" por nuestras iniciales
CREATE EXTERNAL DATA SOURCE DELTALAKE_STORAGE
WITH (LOCATION = 'abfss://deltalake@storagebdadataXXX.dfs.core.windows.net/');

-- Creamos el FILE FORMAT para leer archivos de TEXTO PLANO desde la capa "BRONZE"
CREATE EXTERNAL FILE FORMAT DELTALAKE_FORMAT_TXT WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS(
        FIELD_TERMINATOR = '|',
        ENCODING = 'UTF8',
        FIRST_ROW = 2
    )
);

-- Creamos el FILE FORMAT para leer archivos PARQUET desde la capa "SILVER"
CREATE EXTERNAL FILE FORMAT DELTALAKE_FORMAT_PARQUET WITH (
    FORMAT_TYPE = PARQUET
);

-- Dentro de la base de datos "DELTALAKE" creamos el esquema "BRONZE"
CREATE SCHEMA BRONZE;

-- Creamos la tabla de texto plano "PERSONA" en la capa "BRONZE"
CREATE EXTERNAL TABLE BRONZE.PERSONA(
	ID VARCHAR(8),
	NOMBRE VARCHAR(128),
	TELEFONO VARCHAR(128),
	CORREO VARCHAR(128),
	FECHA_INGRESO VARCHAR(128),
	EDAD INT
) WITH (
	LOCATION = 'bronze/persona/*.data',
	DATA_SOURCE = DELTALAKE_STORAGE,
	FILE_FORMAT = DELTALAKE_FORMAT_TXT
);

-- Vemos los 10 primeros registros que tengan más de 30 años
SELECT TOP 10
    *
FROM 
    BRONZE.PERSONA
WHERE
	EDAD > 30;

-- Dentro de la base de datos "DELTALAKE" creamos el esquema "SILVER"
CREATE SCHEMA SILVER;

-- Creamos la tabla de texto plano "PERSONA" en la capa "BRONZE"
CREATE EXTERNAL TABLE SILVER.PERSONA(
	ID VARCHAR(8),
	NOMBRE VARCHAR(128),
	TELEFONO VARCHAR(128),
	CORREO VARCHAR(128),
	FECHA_INGRESO VARCHAR(128),
	EDAD INT
) WITH (
	LOCATION = 'silver/persona/*.parquet',
	DATA_SOURCE = DELTALAKE_STORAGE,
	FILE_FORMAT = DELTALAKE_FORMAT_PARQUET
);

-- Vemos los 10 primeros registros que tengan más de 30 años
SELECT TOP 10
    *
FROM 
    SILVER.PERSONA
WHERE
	EDAD > 30;

-- Eliminamos las tablas
DROP EXTERNAL TABLE BRONZE.PERSONA;
DROP EXTERNAL TABLE SILVER.PERSONA;

-- Eliminamos los formatos de archivos
DROP EXTERNAL FILE FORMAT DELTALAKE_FORMAT_TXT;
DROP EXTERNAL FILE FORMAT DELTALAKE_FORMAT_PARQUET;

-- Eliminamos el data source
DROP EXTERNAL DATA SOURCE DELTALAKE_STORAGE;

-- Eliminamos la base de datos
-- Primero cambiamos a la base de datos "master"
USE master;

-- Eliminamos la base de datos
DROP DATABASE DELTALAKE;




