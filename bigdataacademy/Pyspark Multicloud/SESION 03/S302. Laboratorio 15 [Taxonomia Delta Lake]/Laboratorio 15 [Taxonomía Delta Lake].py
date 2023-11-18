# Databricks notebook source
## #######################################################################################################
## 
## @copyright Big Data Academy [info@bigdataacademy.org]
## @professor Alonso Melgarejo [alonsoraulmgs@gmail.com]
## 
## #######################################################################################################

# COMMAND ----------

# DBTITLE 1,1. Despliegue de carpeta raíz para el "DELTA LAKE"
#Eliminamos la carpeta raíz del DELTA LAKE

# COMMAND ----------

# MAGIC %fs rm -r dbfs:///FileStore/_pyspark/deltalake

# COMMAND ----------

#Creamos la carpeta raíz del DELTA LAKE

# COMMAND ----------

# MAGIC %fs mkdirs dbfs:///FileStore/_pyspark/deltalake

# COMMAND ----------

# DBTITLE 1,2. Despliegue capa "BRONZE"
#Creamos la carpeta "bronze"

# COMMAND ----------

# MAGIC %fs mkdirs dbfs:///FileStore/_pyspark/deltalake/bronze

# COMMAND ----------

#Creamos la carpeta para el archivo "RIESGO_CREDITICIO.data"

# COMMAND ----------

# MAGIC %fs mkdirs dbfs:///FileStore/_pyspark/deltalake/bronze/riesgo_crediticio/

# COMMAND ----------

#Colocamos el archivo dentro

# COMMAND ----------

# MAGIC %fs cp dbfs:///FileStore/_pyspark/RIESGO_CREDITICIO.data dbfs:///FileStore/_pyspark/deltalake/bronze/riesgo_crediticio/

# COMMAND ----------

#Creamos la carpeta para el archivo "transacciones_bancarias.json"

# COMMAND ----------

# MAGIC %fs mkdirs dbfs:///FileStore/_pyspark/deltalake/bronze/transacciones_bancarias/

# COMMAND ----------

#Colocamos el archivo dentro

# COMMAND ----------

# MAGIC %fs cp dbfs:///FileStore/_pyspark/transacciones_bancarias.json dbfs:///FileStore/_pyspark/deltalake/bronze/transacciones_bancarias/

# COMMAND ----------

# DBTITLE 1,3. Despliegue capa "SILVER"
#Creamos la carpeta "silver"

# COMMAND ----------

# MAGIC %fs mkdirs dbfs:///FileStore/_pyspark/deltalake/silver

# COMMAND ----------

#Creamos las carpetas de cada entidad

# COMMAND ----------

# MAGIC %fs mkdirs dbfs:///FileStore/_pyspark/deltalake/silver/riesgo_crediticio

# COMMAND ----------

# MAGIC %fs mkdirs dbfs:///FileStore/_pyspark/deltalake/silver/persona

# COMMAND ----------

# MAGIC %fs mkdirs dbfs:///FileStore/_pyspark/deltalake/silver/empresa

# COMMAND ----------

# MAGIC %fs mkdirs dbfs:///FileStore/_pyspark/deltalake/silver/transaccion

# COMMAND ----------

# MAGIC %fs mkdirs dbfs:///FileStore/_pyspark/deltalake/silver/contacto

# COMMAND ----------

# DBTITLE 1,4. Despliegue capa "GOLD"
#Creamos la carpeta "gold"

# COMMAND ----------

# MAGIC %fs mkdirs dbfs:///FileStore/_pyspark/deltalake/gold

# COMMAND ----------

#Creamos las carpetas de las soluciones

# COMMAND ----------

# MAGIC %fs mkdirs dbfs:///FileStore/_pyspark/deltalake/gold/reporte_1

# COMMAND ----------

# MAGIC %fs mkdirs dbfs:///FileStore/_pyspark/deltalake/gold/reporte_2
