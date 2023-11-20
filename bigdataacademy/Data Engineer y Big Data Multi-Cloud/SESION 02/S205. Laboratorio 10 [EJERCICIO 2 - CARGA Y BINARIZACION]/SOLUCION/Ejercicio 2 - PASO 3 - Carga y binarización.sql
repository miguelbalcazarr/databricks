-- Databricks notebook source
-- DBTITLE 1,1. Cabeceras
-- Activación de particionamiento dinámico
SET hive.exec.dynamic.partition=true; 

-- Lo colocamos en modo no estricto para que ignore el tipo de dato del campo partición
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Definimos el número máximo de particiones
SET hive.exec.max.dynamic.partitions=10000;
SET hive.exec.max.dynamic.partitions.pernode=10000;

-- COMMAND ----------

-- DBTITLE 1,2. Carga y binarización de PERSONA
-- Cargamos el archivo de datos

-- COMMAND ----------

-- MAGIC %fs cp dbfs:///FileStore/_bigdata/DATA_PERSONA.txt dbfs:///databases/LANDING_TMP/PERSONA

-- COMMAND ----------

-- Verificamos
SELECT P.* FROM LANDING_TMP.PERSONA P LIMIT 10;

-- COMMAND ----------

-- Binarizamos
INSERT INTO TABLE LANDING.PERSONA
  SELECT
    P.*
  FROM
    LANDING_TMP.PERSONA P;

-- COMMAND ----------

-- Verificamos
SELECT P.* FROM LANDING.PERSONA P LIMIT 10;

-- COMMAND ----------

-- DBTITLE 1,3. Carga y binarización de EMPRESA
-- Cargamos el archivo de datos

-- COMMAND ----------

-- MAGIC %fs cp dbfs:///FileStore/_bigdata/DATA_EMPRESA.txt dbfs:///databases/LANDING_TMP/EMPRESA

-- COMMAND ----------

-- Verificamos
SELECT E.* FROM LANDING_TMP.EMPRESA E LIMIT 10;

-- COMMAND ----------

-- Binarizamos
INSERT INTO TABLE LANDING.EMPRESA
  SELECT
    E.*
  FROM
    LANDING_TMP.EMPRESA E;

-- COMMAND ----------

-- Verificamos
SELECT E.* FROM LANDING.EMPRESA E LIMIT 10;

-- COMMAND ----------

-- DBTITLE 1,3. Carga y binarización de TRANSACCIÓN
-- Cargamos el archivo de datos

-- COMMAND ----------

-- MAGIC %fs cp dbfs:///FileStore/_bigdata/DATA_TRANSACCION.txt dbfs:///databases/LANDING_TMP/TRANSACCION

-- COMMAND ----------

-- Verificamos
SELECT T.* FROM LANDING_TMP.TRANSACCION T LIMIT 10;

-- COMMAND ----------

-- Binarizamos con particionamiento dinámico
INSERT INTO TABLE LANDING.TRANSACCION PARTITION (FECHA)
  SELECT
    T.*
  FROM
    LANDING_TMP.TRANSACCION T;

-- COMMAND ----------

-- Verificamos
SELECT T.* FROM LANDING.TRANSACCION T LIMIT 10;

-- COMMAND ----------

-- Verificamos las particiones
SHOW PARTITIONS LANDING.TRANSACCION
