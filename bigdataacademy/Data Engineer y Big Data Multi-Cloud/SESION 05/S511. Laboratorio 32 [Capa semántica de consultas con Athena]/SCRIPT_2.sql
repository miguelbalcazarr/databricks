-- Borramos la base de datos si previamente existe
DROP DATABASE IF EXISTS SILVER CASCADE;

-- Creamos la base de datos
-- IMPORTANTE: Cambiar "XXX" por las iniciales de tu nombre
CREATE DATABASE SILVER LOCATION 's3://deltalakebdaXXX/silver';

-- Creamos la tabla para la entidad
-- IMPORTANTE: Cambiar "XXX" por las iniciales de tu nombre
CREATE EXTERNAL TABLE SILVER.PERSONA(
	ID STRING,
	NOMBRE STRING,
	TELEFONO STRING,
	CORREO STRING,
	FECHA_INGRESO STRING,
	EDAD INT,
	SALARIO DOUBLE,
	ID_EMPRESA STRING
)
STORED AS PARQUET
LOCATION 's3://deltalakebdaXXX/silver/persona'
TBLPROPERTIES(
    'store.charset' = 'ISO-8859-1', 
    'retrieve.charset' = 'ISO-8859-1'
);

-- Imprimimos los datos para verificar
SELECT * FROM SILVER.PERSONA;