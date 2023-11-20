-- Borramos la base de datos si previamente existe
DROP DATABASE IF EXISTS BRONZE CASCADE;

-- Creamos la base de datos
-- IMPORTANTE: Cambiar "XXX" por las iniciales de tu nombre
CREATE DATABASE BRONZE LOCATION 's3://deltalakebdaXXX/bronze';

-- Creamos la tabla para la entidad
-- IMPORTANTE: Cambiar "XXX" por las iniciales de tu nombre
CREATE EXTERNAL TABLE BRONZE.PERSONA(
	ID STRING,
	NOMBRE STRING,
	TELEFONO STRING,
	CORREO STRING,
	FECHA_INGRESO STRING,
	EDAD STRING,
	SALARIO STRING,
	ID_EMPRESA STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 's3://deltalakebdaXXX/bronze/persona'
TBLPROPERTIES(
    'skip.header.line.count' = '1',
    'store.charset' = 'ISO-8859-1', 
    'retrieve.charset' = 'ISO-8859-1'
);

-- Imprimimos los datos para verificar
SELECT * FROM BRONZE.PERSONA;