# Databricks notebook source
## #######################################################################################################
## 
## @copyright Big Data Academy [info@bigdataacademy.org]
## @professor Alonso Melgarejo [alonsoraulmgs@gmail.com]
## 
## #######################################################################################################

# COMMAND ----------

# DBTITLE 1,1. Creación de tópico
#Creamos un tópico llamado "transaccion"

# COMMAND ----------

# MAGIC %sh /kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic transaccion --partitions 1 --replication-factor 1

# COMMAND ----------

# DBTITLE 1,2. Listar tópicos
#Ejecutamos el comando "list" para listar los tópicos existentes

# COMMAND ----------

# MAGIC %sh /kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
