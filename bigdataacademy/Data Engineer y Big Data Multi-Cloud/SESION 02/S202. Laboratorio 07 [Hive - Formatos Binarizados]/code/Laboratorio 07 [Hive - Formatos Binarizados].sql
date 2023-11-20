-- Databricks notebook source
-- -------------------------------------------------------------------------------------------------------
-- 
-- @copyright Big Data Academy [info@bigdataacademy.org]
-- @professor Alonso Melgarejo [alonsoraulmgs@gmail.com]
-- 
-- -------------------------------------------------------------------------------------------------------

-- COMMAND ----------

-- DBTITLE 1,1. Formato TEXTFILE
-- Creamos una tabla
CREATE TABLE TEST.PERSONA_TEXTFILE(
    ID STRING,
    NOMBRE STRING,
    TELEFONO STRING,
    CORREO STRING,
    FECHA_INGRESO DATE,
    EDAD INT,
    SALARIO DOUBLE,
    ID_EMPRESA STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'dbfs:///databases/TEST/PERSONA_TEXTFILE'
TBLPROPERTIES(
    'store.charset'='ISO-8859-1', 
    'retrieve.charset'='ISO-8859-1'
);

-- COMMAND ----------

-- Colocamos el archivo de datos

-- COMMAND ----------

-- MAGIC %fs cp dbfs:///FileStore/_bigdata/persona.data dbfs:///databases/TEST/PERSONA_TEXTFILE

-- COMMAND ----------

-- Verificamos
SELECT P.* FROM TEST.PERSONA_TEXTFILE P LIMIT 10;

-- COMMAND ----------

-- Abrimos las primeras líneas del archivo de datos

-- COMMAND ----------

-- MAGIC %fs head dbfs:///databases/TEST/PERSONA_TEXTFILE/persona.data

-- COMMAND ----------

-- DBTITLE 1,2. Formato PARQUET + compresión SNAPPY
-- Creamos una tabla que soporte el formato PARQUET y la compresión SNAPPY
CREATE TABLE TEST.PERSONA_PARQUET(
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
LOCATION 'dbfs:///databases/TEST/PERSONA_PARQUET'
TBLPROPERTIES (
	'parquet.compression'='SNAPPY',
	'store.charset'='ISO-8859-1', 
    'retrieve.charset'='ISO-8859-1'
);

-- COMMAND ----------

-- Insertamos los datos en la tabla PARQUET desde la tabla TEXTFILE
INSERT INTO TABLE TEST.PERSONA_PARQUET
	SELECT 
		P.* 
	FROM 
		TEST.PERSONA_TEXTFILE P;

-- COMMAND ----------

-- Verificamos la tabla PARQUET
SELECT P.* FROM TEST.PERSONA_PARQUET P LIMIT 10;

-- COMMAND ----------

-- Verificamos el directorio de la tabla

-- COMMAND ----------

-- MAGIC %fs ls dbfs:///databases/TEST/PERSONA_PARQUET

-- COMMAND ----------

-- Abrimos el archivo binarizado

-- COMMAND ----------

-- MAGIC %fs head dbfs:///databases/TEST/PERSONA_PARQUET/part-00000-tid-5006775414878884224-b4b1a0b0-50b5-4717-bcd7-9ee48e679493-1-1-c000.snappy.parquet

-- COMMAND ----------

-- DBTITLE 1,4. Formato DELTA + compresión SNAPPY
-- Creamos una tabla que soporte el formato DELTA y la compresión SNAPPY
-- Por defecto la compresión SNAPPY ya se encuentra activada en los formatos DELTA
CREATE TABLE TEST.PERSONA_DELTA(
	ID STRING,
	NOMBRE STRING,
	TELEFONO STRING,
	CORREO STRING,
	FECHA_INGRESO DATE,
	EDAD INT,
	SALARIO DOUBLE,
	ID_EMPRESA STRING
)
USING DELTA
LOCATION 'dbfs:///databases/TEST/PERSONA_DELTA'
TBLPROPERTIES (
	'store.charset'='ISO-8859-1', 
    'retrieve.charset'='ISO-8859-1'
);

-- COMMAND ----------

-- Insertamos los datos en la tabla DELTA desde la tabla TEXTFILE
INSERT INTO TABLE TEST.PERSONA_DELTA
	SELECT 
		P.* 
	FROM 
		TEST.PERSONA_TEXTFILE P;

-- COMMAND ----------

-- Verificamos
SELECT P.* FROM TEST.PERSONA_DELTA P LIMIT 10;

-- COMMAND ----------

-- Verificamos el directorio de la tabla

-- COMMAND ----------

-- MAGIC %fs ls dbfs:///databases/TEST/PERSONA_DELTA

-- COMMAND ----------

-- Vemos que el archivo se ha binarizado en PARQUET con COMPRESIÓN SNAPPY
-- Tratamos de abrir el archivo

-- COMMAND ----------

-- MAGIC %fs head dbfs:///databases/TEST/PERSONA_DELTA/part-00000-759dd8b4-bc0f-4293-b06c-73a6940707fc-c000.snappy.parquet

-- COMMAND ----------

-- DBTITLE 1,4. Actualización y eliminación de registros en la tabla
-- Las actualizaciones y eliminaciones de registros en una tabla sólo pueden hacerse sobre tablas DELTA

-- COMMAND ----------

-- Actualizamos los registros que cumplan la siguiente condición
UPDATE 
  TEST.PERSONA_DELTA
SET
  TELEFONO = '999999999',
  CORREO = 'DUMMY@GMAIL.COM'
WHERE
  EDAD > 30 AND
  SALARIO > 1000;

-- COMMAND ----------

-- Verificamos
SELECT P.* FROM TEST.PERSONA_DELTA P LIMIT 10;

-- COMMAND ----------

-- Eliminemos los registros que cumplan la siguiente condición
DELETE FROM
  TEST.PERSONA_DELTA
WHERE
  EDAD > 30 AND
  SALARIO > 1000;

-- COMMAND ----------

-- Verificamos
SELECT P.* FROM TEST.PERSONA_DELTA P LIMIT 10;
