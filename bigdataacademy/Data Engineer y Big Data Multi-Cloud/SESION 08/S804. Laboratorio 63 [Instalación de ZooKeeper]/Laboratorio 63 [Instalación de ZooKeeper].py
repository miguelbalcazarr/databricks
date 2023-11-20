# Databricks notebook source
## #######################################################################################################
## 
## @copyright Big Data Academy [info@bigdataacademy.org]
## @professor Alonso Melgarejo [alonsoraulmgs@gmail.com]
## 
## #######################################################################################################

# COMMAND ----------

# DBTITLE 1,1. Descompresión
#Crearemos en la raíz un directorio llamado "/kafka", dentro descomprimiremos el instalador

# COMMAND ----------

# MAGIC %sh mkdir /kafka

# COMMAND ----------

#Descomprimimos el instalador y lo colocamos dentro del directorio "/kafka"

# COMMAND ----------

# MAGIC %sh tar -xvf kafka.tgz -C /kafka --strip-components=1

# COMMAND ----------

#Verificamos el contenido del directorio "/kafka"

# COMMAND ----------

# MAGIC %sh ls /kafka

# COMMAND ----------

# DBTITLE 1,2. Iniciar Zookeeper
#Dentro de "/kafka" en la carpeta "bin" está el script "zookeeper-server-start.sh" que inicia el servicio de ZooKeeper
#Dentro de "/kafka" en la carpeta "config" está el archivo "zookeeper.properties" el cual tiene la configuración para que el servicio de ZooKeeper funcione


# COMMAND ----------

#Iniciamos el servicio ejecutando el script junto con el archivo de configuración
#IMPORTANTE: NO DEBEMOS CERRAR ESTE NOTEBOOK, SI NO EL PROCESO DE ZOOKEEPER FINALIZARÍA

# COMMAND ----------

# MAGIC %sh /kafka/bin/zookeeper-server-start.sh /kafka/config/zookeeper.properties
