# Databricks notebook source
## #######################################################################################################
## 
## @copyright Big Data Academy [info@bigdataacademy.org]
## @professor Alonso Melgarejo [alonsoraulmgs@gmail.com]
## 
## #######################################################################################################

# COMMAND ----------

# DBTITLE 1,1. Lectura de archivo semi-estructurado XML
#Leemos los datos
dfXml = spark.read.format("xml").option("rootTag", "root").option("rowTag", "element").load("dbfs:///FileStore/_pyspark/transacciones.xml")
 
#Mostramos la data
dfXml.show()
