-- Databricks notebook source
-- DBTITLE 1,1. Creación de base de datos
-- Borramos la base de datos si existe
DROP DATABASE IF EXISTS LANDING_TMP CASCADE;

-- COMMAND ----------

-- Borramos la base de datos si existe

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:///databases/LANDING_TMP

-- COMMAND ----------

-- Creación de base de datos
CREATE DATABASE LANDING_TMP LOCATION 'dbfs:///databases/LANDING_TMP';

-- COMMAND ----------

-- DBTITLE 1,2. Creación de tablas en LANDING_TMP
-- Creación de tabla
CREATE TABLE LANDING_TMP.PERSONA(
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
LOCATION 'dbfs:///databases/LANDING_TMP/PERSONA'
TBLPROPERTIES(
  'store.charset'='ISO-8859-1', 
  'retrieve.charset'='ISO-8859-1'
);

-- COMMAND ----------

-- Creación de tabla
CREATE TABLE LANDING_TMP.EMPRESA(
  ID STRING,
  NOMBRE STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'dbfs:///databases/LANDING_TMP/EMPRESA'
TBLPROPERTIES(
  'store.charset'='ISO-8859-1', 
  'retrieve.charset'='ISO-8859-1'
);

-- COMMAND ----------

-- Creación de tabla
CREATE TABLE LANDING_TMP.TRANSACCION(
  ID_PERSONA STRING,
  ID_EMPRESA STRING,
  MONTO DOUBLE,
  FECHA STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'dbfs:///databases/LANDING_TMP/TRANSACCION'
TBLPROPERTIES(
  'store.charset'='ISO-8859-1', 
  'retrieve.charset'='ISO-8859-1'
);
