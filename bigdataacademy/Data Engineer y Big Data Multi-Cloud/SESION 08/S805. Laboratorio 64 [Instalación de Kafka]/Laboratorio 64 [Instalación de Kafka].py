# Databricks notebook source
## #######################################################################################################
## 
## @copyright Big Data Academy [info@bigdataacademy.org]
## @professor Alonso Melgarejo [alonsoraulmgs@gmail.com]
## 
## #######################################################################################################

# COMMAND ----------

# DBTITLE 1,1. Iniciar Kafka
#Dentro de "/kafka" en la carpeta "bin" está el script "kafka-server-start.sh" que inicia el servicio de KAFKA
#Dentro de "/kafka" en la carpeta "config" está el archivo "server.properties" el cual tiene la configuración para que el servicio de KAFKA funcione

# COMMAND ----------

#Iniciamos el servicio ejecutando el script junto con el archivo de configuración
#IMPORTANTE: NO DEBEMOS CERRAR ESTE NOTEBOOK, SI NO EL PROCESO DE KAFKA FINALIZARÍA

# COMMAND ----------

# MAGIC %sh /kafka/bin/kafka-server-start.sh /kafka/config/server.properties
