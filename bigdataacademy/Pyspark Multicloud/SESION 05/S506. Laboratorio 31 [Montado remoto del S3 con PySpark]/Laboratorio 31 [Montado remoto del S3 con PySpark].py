# Databricks notebook source
## #######################################################################################################
## 
## @copyright Big Data Academy [info@bigdataacademy.org]
## @professor Alonso Melgarejo [alonsoraulmgs@gmail.com]
## 
## #######################################################################################################

# COMMAND ----------

# DBTITLE 1,1. Definición de parámetros de montado
#Credencial de seguridad "Access Key ID" desde el archivo de claves
accessKeyId = "XXXXXXXXXXXXXXXXXXXXXXXXX"

# COMMAND ----------

#Credencial de seguridad "Secret Access Key" desde el archivo de claves
#En ocasiones la clave se genera con un caracter "/"
#El caracter "/" es un caracter especial que debe ser reemplazado con "%2F"
secretAccessKey = "XXXXXXXXXXXXXXXXXXXXXXXXX".replace("/", "%2F")

# COMMAND ----------

#Bucket que queremos montar
#IMPORTANTE: Cambiar "XXX" por las iniciales de tu nombre
bucket = "storagebdaXXX"

# COMMAND ----------

# DBTITLE 1,2. Desmontamos si ya realizamos el montado
#Desmontamos el directorio
dbutils.fs.unmount("dbfs:///mnt/aws")

# COMMAND ----------

# DBTITLE 1,3. Realizamos el montado
#Realizamos el montado
dbutils.fs.mount(
  source = "s3a://"+accessKeyId+":"+secretAccessKey+"@"+bucket,
  mount_point = "/mnt/aws"
)

# COMMAND ----------

# DBTITLE 1,4. Verificamos el montado
# MAGIC %fs ls dbfs:///mnt/aws

# COMMAND ----------

# DBTITLE 1,5. Leemos los datos
#Lectura de datos
df = spark.read.format("csv").option("header","true").option("delimiter","|").option("inferSchema", "true").load("dbfs:///mnt/aws/landing/persona/")

# COMMAND ----------

#Verificamos
df.show()

# COMMAND ----------

#Mostramos el esquema de metadatos
df.printSchema()
