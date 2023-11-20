-- Databricks notebook source
-- DBTITLE 1,1. Creación de base de datos
-- Borramos la base de datos si existe
DROP DATABASE IF EXISTS LANDING CASCADE;

-- COMMAND ----------

-- Borramos la base de datos si existe

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:///databases/LANDING

-- COMMAND ----------

-- Creación de base de datos
CREATE DATABASE LANDING LOCATION 'dbfs:///databases/LANDING';

-- COMMAND ----------

-- DBTITLE 1,2. Creación de tablas en LANDING
-- Creación de tabla
CREATE TABLE LANDING.PERSONA(
  ID STRING,
  NOMBRE STRING,
  TELEFONO STRING,
  CORREO STRING,
  FECHA_INGRESO STRING,
  EDAD INT,
  SALARIO DOUBLE,
  ID_EMPRESA STRING
)
USING DELTA
LOCATION 'dbfs:///databases/LANDING/PERSONA'
TBLPROPERTIES(
  'store.charset'='ISO-8859-1', 
  'retrieve.charset'='ISO-8859-1'
);

-- COMMAND ----------

-- Creación de tabla
CREATE TABLE LANDING.EMPRESA(
  ID STRING,
  NOMBRE STRING
)
USING DELTA
LOCATION 'dbfs:///databases/LANDING/EMPRESA'
TBLPROPERTIES(
  'store.charset'='ISO-8859-1', 
  'retrieve.charset'='ISO-8859-1'
);

-- COMMAND ----------

-- Creación de tabla
CREATE TABLE LANDING.TRANSACCION(
  ID_PERSONA STRING,
  ID_EMPRESA STRING,
  MONTO DOUBLE
)
USING DELTA
PARTITIONED BY (FECHA STRING)
LOCATION 'dbfs:///databases/LANDING/TRANSACCION'
TBLPROPERTIES(
    'store.charset'='ISO-8859-1', 
    'retrieve.charset'='ISO-8859-1'
);
