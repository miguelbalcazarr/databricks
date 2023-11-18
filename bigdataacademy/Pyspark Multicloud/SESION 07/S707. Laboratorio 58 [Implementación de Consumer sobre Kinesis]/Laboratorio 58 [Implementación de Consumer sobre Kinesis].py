# Databricks notebook source
## #######################################################################################################
## 
## @copyright Big Data Academy [info@bigdataacademy.org]
## @professor Alonso Melgarejo [alonsoraulmgs@gmail.com]
## 
## #######################################################################################################

# COMMAND ----------

# DBTITLE 1,1. Encriptación de la clave de acceso para el consumer
#Definimos el ACCESS KEY ID desde el archivo de credenciales
awsAccessKeyId = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

# COMMAND ----------

#Definimos el SECRET ACCESS KEY desde el archivo de credenciales
awsSecretAccessKey = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

# COMMAND ----------

# DBTITLE 1,2. Conexión al tópico
#Creamos la conexión en tiempo real sobre el dataframe "dfStream"
#Dentro de esta variable en tiempo real se colocarán los registros desde el tópico
dfStream = spark.readStream.format("kinesis").option("awsAccessKey", awsAccessKeyId).option("awsSecretKey", awsSecretAccessKey).option("region", "us-east-1").option("streamName", "transaccion").option("startingOffsets", "latest").load()

# COMMAND ----------

#Verificamos el esquema de metadatos
#Dentro del campo "data" se encuentran los registros binarizados
dfStream.printSchema()

# COMMAND ----------

#Iniciamos el stream para verificar con la función "display"
#Luego de iniciar el stream iniciamos el PRODUCER para verificar que los registros lleguen al CONSUMER
#Luego de verificar que los datos lleguen, detenemos el CONSUMER
display(dfStream)

# COMMAND ----------

# DBTITLE 1,3. Des-binarización de los registros
#Seleccionamos la columna "data" y des-binarizamos el contenido
df1 = dfStream.select(
    dfStream["data"].cast("string")
)

# COMMAND ----------

#Verificamos el esquema de metadatos
#Dentro del campo "data" se encontrarán los registros como "strings"
df1.printSchema()

# COMMAND ----------

#Iniciamos el stream para verificar con la función "display"
#Luego de iniciar el stream iniciamos el PRODUCER para verificar que los registros lleguen al CONSUMER
#Luego de verificar que los datos lleguen, detenemos el CONSUMER
display(df1)
