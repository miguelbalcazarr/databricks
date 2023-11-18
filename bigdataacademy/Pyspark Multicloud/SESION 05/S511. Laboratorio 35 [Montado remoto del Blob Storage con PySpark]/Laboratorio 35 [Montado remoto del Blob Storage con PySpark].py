# Databricks notebook source
## #######################################################################################################
## 
## @copyright Big Data Academy [info@bigdataacademy.org]
## @professor Alonso Melgarejo [alonsoraulmgs@gmail.com]
## 
## #######################################################################################################

# COMMAND ----------

# DBTITLE 1,1. Definición de parámetros de montado
#Cuenta de almacenamiento
#Remplazar "XXX" por tus iniciales
cuentaDeAlmacenamiento = "storagebdaXXX"

# COMMAND ----------

#Blob Storage
blobStorage = "landing"

# COMMAND ----------

#Token de acceso
tokenSas = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

# COMMAND ----------

# DBTITLE 1,2. Desmontamos si ya realizamos el montado
#Desmontamos el directorio
dbutils.fs.unmount("dbfs:///mnt/azure")

# COMMAND ----------

# DBTITLE 1,3. Realizamos el montado
#Realizamos el montado
dbutils.fs.mount(
  source = "wasbs://"+blobStorage+"@"+cuentaDeAlmacenamiento+".blob.core.windows.net",
  mount_point  = "/mnt/azure",
  extra_configs = {
    "fs.azure.sas."+blobStorage+"."+cuentaDeAlmacenamiento+".blob.core.windows.net": tokenSas
  }
)

# COMMAND ----------

# DBTITLE 1,4. Verificamos el montado
# MAGIC %fs ls dbfs:///mnt/azure

# COMMAND ----------

# DBTITLE 1,5. Leemos los datos
#Lectura de datos
df = spark.read.format("csv").option("header","true").option("delimiter","|").option("inferSchema", "true").load("dbfs:///mnt/azure/persona/")

# COMMAND ----------

#Verificamos
df.show()

# COMMAND ----------

#Mostramos el esquema de metadatos
df.printSchema()
