-- Databricks notebook source
-- -------------------------------------------------------------------------------------------------------
-- 
-- @copyright Big Data Academy [info@bigdataacademy.org]
-- @professor Alonso Melgarejo [alonsoraulmgs@gmail.com]
-- 
-- -------------------------------------------------------------------------------------------------------

-- COMMAND ----------

-- DBTITLE 1,1. Creación de tabla en texto plano
-- Creación de tabla
CREATE TABLE TEST.TRANSACCION_TXT(
	ID_PERSONA STRING,
	ID_EMPRESA STRING,
	MONTO DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'dbfs:///databases/TEST/TRANSACCION_TXT'
TBLPROPERTIES(
    'store.charset'='ISO-8859-1', 
    'retrieve.charset'='ISO-8859-1'
);

-- COMMAND ----------

-- Colocamos el archivo de datos

-- COMMAND ----------

-- MAGIC %fs cp dbfs:///FileStore/_bigdata/transacciones_2018_01_21.data dbfs:///databases/TEST/TRANSACCION_TXT

-- COMMAND ----------

-- Verificamos
SELECT T.* FROM TEST.TRANSACCION_TXT T LIMIT 10;

-- COMMAND ----------

-- DBTITLE 1,2. Creación de tabla particionada
-- Creación de tabla particionada por FECHA
CREATE TABLE TEST.TRANSACCION_PARTICIONADA(
	ID_PERSONA STRING,
	ID_EMPRESA STRING,
	MONTO DOUBLE
)
USING DELTA
PARTITIONED BY (FECHA STRING)
LOCATION 'dbfs:///databases/TEST/TRANSACCION_PARTICIONADA'
TBLPROPERTIES(
    'store.charset'='ISO-8859-1', 
    'retrieve.charset'='ISO-8859-1'
);

-- COMMAND ----------

-- Crearemos una partición y le agregaremos información binarizada
INSERT INTO TEST.TRANSACCION_PARTICIONADA PARTITION (FECHA='2018-01-21')
	SELECT
		T.*
	FROM
		TEST.TRANSACCION_TXT T;

-- COMMAND ----------

-- Verificamos que haya datos
SELECT T.* FROM TEST.TRANSACCION_PARTICIONADA T LIMIT 10;

-- COMMAND ----------

-- Contamos el número de registros
SELECT COUNT(*) FROM TEST.TRANSACCION_PARTICIONADA;

-- COMMAND ----------

-- Mostramos la particiones existentes
SHOW PARTITIONS TEST.TRANSACCION_PARTICIONADA;

-- COMMAND ----------

-- Listemos el contenido de la carpeta de nuestra tabla

-- COMMAND ----------

-- MAGIC %fs ls dbfs:///databases/TEST/TRANSACCION_PARTICIONADA

-- COMMAND ----------

-- Notamos que se ha creado una carpeta llamada "FECHA=2018-01-21"
-- Listemos el contenido de esta carpeta

-- COMMAND ----------

-- MAGIC %fs ls dbfs:///databases/TEST/TRANSACCION_PARTICIONADA/FECHA=2018-01-21

-- COMMAND ----------

-- DBTITLE 1,3. Creación de una segunda partición
-- Truncaremos la tabla TXT
-- eliminamos el directorio de la tabla TXT

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:///databases/TEST/TRANSACCION_TXT

-- COMMAND ----------

-- Volvemos a crear el directorio

-- COMMAND ----------

-- MAGIC %fs mkdirs dbfs:///databases/TEST/TRANSACCION_TXT

-- COMMAND ----------

-- Colocamos el archivo de datos

-- COMMAND ----------

-- MAGIC %fs cp dbfs:///FileStore/_bigdata/transacciones_2018_01_22.data dbfs:///databases/TEST/TRANSACCION_TXT

-- COMMAND ----------

-- Crearemos una partición y le agregaremos información binarizada
INSERT INTO TEST.TRANSACCION_PARTICIONADA PARTITION (FECHA='2018-01-22')
	SELECT
		T.*
	FROM
		TEST.TRANSACCION_TXT T;

-- COMMAND ----------

-- Mostramos la particiones existentes
SHOW PARTITIONS TEST.TRANSACCION_PARTICIONADA;

-- COMMAND ----------

-- Contamos el número de registros
SELECT COUNT(*) FROM TEST.TRANSACCION_PARTICIONADA;

-- COMMAND ----------

-- Listemos el contenido de la carpeta de nuestra tabla

-- COMMAND ----------

-- MAGIC %fs ls dbfs:///databases/TEST/TRANSACCION_PARTICIONADA

-- COMMAND ----------

-- DBTITLE 1,4. Eliminación de particiones en tablas DELTA
-- Ejecutamos el comando DELETE con el valor de las particiones que queremos eliminar
-- Eliminaremos las dos particiones que insertamos
DELETE FROM
  TEST.TRANSACCION_PARTICIONADA
WHERE
  FECHA = '2018-01-21' OR
  FECHA = '2018-01-22';

-- COMMAND ----------

-- Verificamos
SELECT * FROM TEST.TRANSACCION_PARTICIONADA LIMIT 10;

-- COMMAND ----------

-- DBTITLE 1,4. Eliminación de particiones en tablas PARQUET (y otros formatos binarios)
-- Creamos una tabla parquet particionada
CREATE TABLE TEST.TRANSACCION_PARTICIONADA_PARQUET(
	ID_PERSONA STRING,
	ID_EMPRESA STRING,
	MONTO DOUBLE
)
STORED AS PARQUET
PARTITIONED BY (FECHA STRING)
LOCATION 'dbfs:///databases/TEST/TRANSACCION_PARTICIONADA_PARQUET'
TBLPROPERTIES(
    'parquet.compression'='SNAPPY',
    'store.charset'='ISO-8859-1', 
    'retrieve.charset'='ISO-8859-1'
);

-- COMMAND ----------

-- Insertamos una partición
INSERT INTO TEST.TRANSACCION_PARTICIONADA_PARQUET PARTITION (FECHA='2018-01-22')
	SELECT
		T.*
	FROM
		TEST.TRANSACCION_TXT T;

-- COMMAND ----------

-- Verificamos
SELECT T.* FROM TEST.TRANSACCION_PARTICIONADA_PARQUET T LIMIT 10;

-- COMMAND ----------

-- Para borrar la partición debemos de ejecutar dos comandos
-- Primero borramos manualmente la partición
ALTER TABLE TEST.TRANSACCION_PARTICIONADA_PARQUET DROP IF EXISTS PARTITION (FECHA='2018-01-22');

-- COMMAND ----------

-- Luego borramos el directorio de la partición

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:///databases/TEST/TRANSACCION_PARTICIONADA_PARQUET/FECHA=2018-01-22

-- COMMAND ----------

-- Verificamos
SELECT T.* FROM TEST.TRANSACCION_PARTICIONADA_PARQUET T LIMIT 10;
