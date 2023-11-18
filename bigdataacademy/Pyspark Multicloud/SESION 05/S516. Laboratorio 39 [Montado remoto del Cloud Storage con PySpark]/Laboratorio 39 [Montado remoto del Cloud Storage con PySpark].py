# Databricks notebook source
## #######################################################################################################
## 
## @copyright Big Data Academy [info@bigdataacademy.org]
## @professor Alonso Melgarejo [alonsoraulmgs@gmail.com]
## 
## #######################################################################################################

# COMMAND ----------

# DBTITLE 1,1. Definición de parámetros de montado
#Bucket de GCP
#Remplazar "XXX" por tus iniciales
bucket = "storagebdaXXX"

# COMMAND ----------

# DBTITLE 1,2. Desmontamos si ya realizamos el montado
#Desmontamos el directorio
dbutils.fs.unmount("dbfs:///mnt/gcp")

# COMMAND ----------

# DBTITLE 1,3. Realizamos el montado
#Realizamos el montado
dbutils.fs.mount(
  source = "gs://"+bucket,
  mount_point  = "/mnt/gcp"
)

# COMMAND ----------

# DBTITLE 1,4. Verificamos el montado
# MAGIC %fs ls dbfs:///mnt/gcp

# COMMAND ----------

# DBTITLE 1,5. Leemos los datos
#Lectura de datos
df = spark.read.format("csv").option("header","true").option("delimiter","|").option("inferSchema", "true").load("dbfs:///mnt/gcp/persona/")

# COMMAND ----------

#Verificamos
df.show()

# COMMAND ----------

#Mostramos el esquema de metadatos
df.printSchema()
