# Databricks notebook source
## #######################################################################################################
## 
## @copyright Big Data Academy [info@bigdataacademy.org]
## @professor Alonso Melgarejo [alonsoraulmgs@gmail.com]
## 
## #######################################################################################################

# COMMAND ----------

# DBTITLE 1,1. Lectura de archivos estructurados
#Lectura desde archivo de texto plano
dfData = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("encoding", "ISO-8859-1").load("dbfs:///FileStore/_bigdata/persona.data")

# COMMAND ----------

#Mostramos los primeros 20 registros
dfData.show()

# COMMAND ----------

#Mostramos el esquema de metadatos
#Notamos que todos los campos fueron reconocidos con el tipo de dato "string"
dfData.printSchema()

# COMMAND ----------

# DBTITLE 1,2. Lectura de archivos estructurados infiriendo el esquema de metadatos
#Lectura desde archivo de texto plano
dfData2 = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("encoding", "ISO-8859-1").option("inferSchema", True).load("dbfs:///FileStore/_bigdata/persona.data")

# COMMAND ----------

#Mostramos los primeros 20 registros
dfData2.show()

# COMMAND ----------

#Mostramos el esquema de metadatos
#Notamos que los campos "EDAD" y "SALARIO" son reconocidos como "integer"
#La inferencia no necesariamente es correcta
#Por ejemplo, el "SALARIO" debería ser un "double" (decimal)
#También, los identificadores ("ID", "ID_EMPRESA") deberían ser "string"
dfData2.printSchema()

# COMMAND ----------

# DBTITLE 1,3. Lectura de tablas HIVE
# MAGIC %sql
# MAGIC -- La cabecera "%sql" nos permite ejecutar código HIVE
# MAGIC 
# MAGIC -- Creamos una base de datos
# MAGIC CREATE DATABASE TEST_2;
# MAGIC 
# MAGIC -- Creamos una tabla
# MAGIC CREATE TABLE TEST_2.PERSONA(
# MAGIC     ID STRING,
# MAGIC     NOMBRE STRING,
# MAGIC     EDAD INT,
# MAGIC     SALARIO DOUBLE
# MAGIC )
# MAGIC ROW FORMAT DELIMITED
# MAGIC FIELDS TERMINATED BY '|'
# MAGIC LINES TERMINATED BY '\n'
# MAGIC STORED AS TEXTFILE
# MAGIC LOCATION 'dbfs:///databases/TEST_2/PERSONA'
# MAGIC TBLPROPERTIES(
# MAGIC     'skip.header.line.count'='1',
# MAGIC     'store.charset'='ISO-8859-1', 
# MAGIC     'retrieve.charset'='ISO-8859-1'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Insertamos tres registros de ejemplo en la tabla
# MAGIC INSERT INTO TEST_2.PERSONA VALUES
# MAGIC ('1', 'Carl', 32, 20095),
# MAGIC ('2', 'Priscilla', 34, 9298),
# MAGIC ('3', 'Jocelyn', 27, 10853);

# COMMAND ----------

#Lectura desde tabla HIVE
dfHive = spark.sql("SELECT * FROM TEST_2.PERSONA")

# COMMAND ----------

#Mostramos la data
dfHive.show()

# COMMAND ----------

#Mostramos el esquema de la data
#Notemos que las tablas tienen metadata
dfHive.printSchema()

# COMMAND ----------

# DBTITLE 1,4. Lectura de archivos semi-estructurados JSON
#Lectura de archivo JSON
dfJson = spark.read.format("json").option("multiLine", False).load("dbfs:///FileStore/_bigdata/transacciones_complejas.json")

# COMMAND ----------

#Mostramos la data
#No podemos ver bien los datos ya que los trunca
dfJson.show()

# COMMAND ----------

#Mostramos los datos sin truncar
dfJson.show(20, False)

# COMMAND ----------

#Mostramos el esquema de metadatos
dfJson.printSchema()
