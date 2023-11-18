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
dfData = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("encoding", "ISO-8859-1").load("dbfs:///FileStore/_pyspark/persona.data")

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
dfData2 = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("encoding", "ISO-8859-1").option("inferSchema", True).load("dbfs:///FileStore/_pyspark/persona.data")

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

# DBTITLE 1,3. Leyendo todos los archivos dentro de un directorio
#Eliminamos el directorio "transaccion"

# COMMAND ----------

# MAGIC %fs rm -r dbfs:///FileStore/_pyspark/transaccion

# COMMAND ----------

#Creamos el directorio "transaccion"

# COMMAND ----------

# MAGIC %fs mkdirs dbfs:///FileStore/_pyspark/transaccion

# COMMAND ----------

#Copiaremos los tres archivos de transacciones dentro

# COMMAND ----------

# MAGIC %fs cp dbfs:///FileStore/_pyspark/transacciones_2021_01_21.data dbfs:///FileStore/_pyspark/transaccion

# COMMAND ----------

# MAGIC %fs cp dbfs:///FileStore/_pyspark/transacciones_2021_01_22.data dbfs:///FileStore/_pyspark/transaccion

# COMMAND ----------

# MAGIC %fs cp dbfs:///FileStore/_pyspark/transacciones_2021_01_23.data dbfs:///FileStore/_pyspark/transaccion

# COMMAND ----------

#Verificamos

# COMMAND ----------

# MAGIC %fs ls dbfs:///FileStore/_pyspark/transaccion

# COMMAND ----------

#Leemos las transacciones desde el directorio
dfTransaccion = spark.read.format("csv").option("header", "true").option("delimiter", ",").option("encoding", "ISO-8859-1").option("inferSchema", True).load("dbfs:///FileStore/_pyspark/transaccion")
 
#Mostramos los datos
dfTransaccion.show()

# COMMAND ----------

#Vemos el esquema de metadatos
dfTransaccion.printSchema()

# COMMAND ----------

# DBTITLE 1,4. Lectura de archivos semi-estructurados JSON
#Lectura de archivo JSON
dfJson = spark.read.format("json").option("multiLine", False).load("dbfs:///FileStore/_pyspark/transacciones_complejas.json")

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
