-- Databricks notebook source
-- -------------------------------------------------------------------------------------------------------
-- 
-- @copyright Big Data Academy [info@bigdataacademy.org]
-- @professor Alonso Melgarejo [alonsoraulmgs@gmail.com]
-- 
-- -------------------------------------------------------------------------------------------------------

-- COMMAND ----------

-- DBTITLE 1,1. Creación de base de datos
-- Listamos las bases de datos existentes
SHOW DATABASES;

-- COMMAND ----------

-- Creación de base de datos
CREATE DATABASE TEST LOCATION 'dbfs:///databases/TEST';

-- COMMAND ----------

-- Listamos las bases de datos existentes
SHOW DATABASES;

-- COMMAND ----------

-- Borrar bases de datos
DROP DATABASE IF EXISTS TEST CASCADE;

-- COMMAND ----------

-- Listamos las bases de datos existentes
SHOW DATABASES;

-- COMMAND ----------

-- Volvemos a crearla, si no existe
CREATE DATABASE IF NOT EXISTS TEST LOCATION 'dbfs:///databases/TEST';

-- COMMAND ----------

-- Listamos las bases de datos existentes
SHOW DATABASES;

-- COMMAND ----------

-- DBTITLE 1,2. Creación de tabla
-- Creamos una tabla
CREATE TABLE TEST.PERSONA(
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
LOCATION 'dbfs:///databases/TEST/PERSONA'
TBLPROPERTIES(
    'store.charset'='ISO-8859-1', 
    'retrieve.charset'='ISO-8859-1'
);

-- COMMAND ----------

-- Verificamos
SHOW TABLES IN TEST;

-- COMMAND ----------

-- Consultamos la descripción de la tabla
DESC FORMATTED TEST.PERSONA;

-- COMMAND ----------

-- Consultamos algunos campos de la tabla y los 10 primeros registros
-- Como en este momento la tabla está vacía no vemos registros (el directorio "dbfs:///databases/TEST/PERSONA" no tiene archivos)
SELECT 
  P.ID, 
  P.NOMBRE, 
  P.CORREO 
FROM 
  TEST.PERSONA P 
LIMIT 
  10;

-- COMMAND ----------

-- Copiamos el archivo en el directorio de la tabla

-- COMMAND ----------

-- MAGIC %fs cp dbfs:///FileStore/_bigdata/persona.data dbfs:///databases/TEST/PERSONA

-- COMMAND ----------

-- Consultamos algunos campos de la tabla y los 10 primeros registros
-- Ahora sí vemos registros
SELECT 
  P.ID, 
  P.NOMBRE, 
  P.CORREO 
FROM 
  TEST.PERSONA P 
LIMIT 
  10;

-- COMMAND ----------

-- También podemos ver todos los campos de la tabla
SELECT 
  P.*
FROM 
  TEST.PERSONA P 
LIMIT 
  10;

-- COMMAND ----------

-- Revisemos el directorio de la tabla

-- COMMAND ----------

-- MAGIC %fs ls dbfs:///databases/TEST/PERSONA

-- COMMAND ----------

-- Contamos la cantidad de registros de la tabla
SELECT 
  COUNT(*) 
FROM 
  TEST.PERSONA P;

-- COMMAND ----------

-- Filtramos la tabla según algunas condiciones
SELECT 
    P.NOMBRE,
    P.EDAD,
    P.SALARIO
FROM 
    TEST.PERSONA P
WHERE
    P.EDAD > 35 AND
    P.SALARIO > 5000;

-- COMMAND ----------

-- Eliminamos la tabla
DROP TABLE TEST.PERSONA;

-- COMMAND ----------

-- ¿Qué pasa con el directorio y el archivo cuando eliminamos una tabla?
-- Revisemos la ruta donde estába la tabla

-- COMMAND ----------

-- MAGIC %fs ls dbfs:///databases/TEST/PERSONA

-- COMMAND ----------

-- DBTITLE 1,3. Creación de tabla EXTERNAL
-- Volveremos a crear la tabla, pero esta vez como "EXTERNAL"

-- COMMAND ----------

-- Creamos una tabla EXTERNAL
CREATE EXTERNAL TABLE TEST.PERSONA(
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
LOCATION 'dbfs:///databases/TEST/PERSONA'
TBLPROPERTIES(
    'store.charset'='ISO-8859-1', 
    'retrieve.charset'='ISO-8859-1'
);

-- COMMAND ----------

-- Consultamos la tabla
SELECT P.* FROM TEST.PERSONA P LIMIT 10;

-- COMMAND ----------

-- DBTITLE 1,4. Truncado de tabla
-- No es posible ejecutar el comando
TRUNCATE TABLE TEST.PERSONA

-- COMMAND ----------

-- Deberemos hacer el borrado desde el sistema de archivos

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:///databases/TEST/PERSONA

-- COMMAND ----------

-- Consultamos y obtenemos un error
SELECT P.* FROM TEST.PERSONA P LIMIT 10;

-- COMMAND ----------

-- Volvemos a crear el directorio

-- COMMAND ----------

-- MAGIC %fs mkdirs dbfs:///databases/TEST/PERSONA

-- COMMAND ----------

-- Consultamos la tabla y estará vacía
SELECT P.* FROM TEST.PERSONA P LIMIT 10;
