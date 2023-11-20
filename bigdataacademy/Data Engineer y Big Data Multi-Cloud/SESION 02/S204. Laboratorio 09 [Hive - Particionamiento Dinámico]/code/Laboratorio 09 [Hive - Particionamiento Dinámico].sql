-- Databricks notebook source
-- -------------------------------------------------------------------------------------------------------
-- 
-- @copyright Big Data Academy [info@bigdataacademy.org]
-- @professor Alonso Melgarejo [alonsoraulmgs@gmail.com]
-- 
-- -------------------------------------------------------------------------------------------------------show databases;

-- COMMAND ----------

-- DBTITLE 1,1. Tabla de datos histórica
-- Ejemplos de registros de este archivo:
--
-- ID_PERSONA|ID_EMPRESA|MONTO|FECHA
-- 18|3|1383|2018-01-21
-- 30|6|2331|2018-01-21
-- 12|4|467|2018-01-23
-- 28|1|730|2018-01-21
-- 24|6|4475|2018-01-22
-- 67|9|561|2018-01-22
-- 9|4|3765|2018-01-22
-- 36|2|2659|2018-01-23
-- 91|5|3497|2018-01-22
--
-- Queremos subir este archivo a una tabla particionada, en donde la partición es la FECHA
-- Sabemos que una tabla particionada tiene subcarpetas por cada partición, dentro de las subcarpetas, van los archivos
-- ¿Cómo cortamos el archivo para que aterrice en forma de partición en cada subcarpeta?

-- COMMAND ----------

-- Creamos una tabla en donde colocaremos el archivo sin particionar
-- Notemos que esta table no está particionada
CREATE TABLE TEST.TRANSACCION_SIN_PARTICIONAR(
	ID_PERSONA STRING,
	ID_EMPRESA STRING,
	MONTO DOUBLE,
	FECHA STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'dbfs:///databases/TEST/TRANSACCION_SIN_PARTICIONAR'
TBLPROPERTIES(
    'store.charset'='ISO-8859-1', 
    'retrieve.charset'='ISO-8859-1'
);

-- COMMAND ----------

-- Colocamos el archivo de datos sin particionar

-- COMMAND ----------

-- MAGIC %fs cp dbfs:///FileStore/transacciones.data dbfs:///databases/TEST/TRANSACCION_SIN_PARTICIONAR

-- COMMAND ----------

-- Verificamos
SELECT COUNT(*) FROM TEST.TRANSACCION_SIN_PARTICIONAR;

-- COMMAND ----------

-- DBTITLE 1,2. Ejecución del particionamiento dinámico
-- Activamos el particionamiento dinámico
SET hive.exec.dynamic.partition=true; 

-- COMMAND ----------

-- Activamos el modo no estricto del particionamiento dinámico, el cuál ignora el tipo de dato del campo de partición
SET hive.exec.dynamic.partition.mode=nonstrict;

-- COMMAND ----------

-- Definimos el número máximo de particiones
SET hive.exec.max.dynamic.partitions=10000;
SET hive.exec.max.dynamic.partitions.pernode=10000;

-- COMMAND ----------

-- Ejecutamos el particionamiento dinámico
INSERT INTO TABLE TEST.TRANSACCION_PARTICIONADA PARTITION (FECHA)
	SELECT
		T.*
	FROM
		TEST.TRANSACCION_SIN_PARTICIONAR T;

-- COMMAND ----------

-- Verificamos
SHOW PARTITIONS TEST.TRANSACCION_PARTICIONADA;

-- COMMAND ----------

-- Si revisamos las rutas HDFS de las particiones podemos encontrar las subcarpetas generadas por cada partición

-- COMMAND ----------

-- MAGIC %fs ls dbfs:///databases/TEST/TRANSACCION_PARTICIONADA
