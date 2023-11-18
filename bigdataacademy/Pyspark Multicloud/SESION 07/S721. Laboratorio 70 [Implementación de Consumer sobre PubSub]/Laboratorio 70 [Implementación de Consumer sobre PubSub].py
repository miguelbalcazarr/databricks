# Databricks notebook source
## #######################################################################################################
## 
## @copyright Big Data Academy [info@bigdataacademy.org]
## @professor Alonso Melgarejo [alonsoraulmgs@gmail.com]
## 
## #######################################################################################################

# COMMAND ----------

# DBTITLE 1,1. Colocación del archivo de credenciales en el sistema de archivos local de Linux
#Creamos la variable de sistema "GOOGLE_APPLICATION_CREDENTIALS" en LINUX con la ruta del archivo de credenciales

# COMMAND ----------

# MAGIC %sh mkdir /credenciales

# COMMAND ----------

#Desde el sistema de archivos DBFS copiamos el archivo de credenciales al sistema de archivos local de LINUX

# COMMAND ----------

# MAGIC %fs cp dbfs:///FileStore/_pyspark/credenciales.json file:///credenciales

# COMMAND ----------

#Verificamos el archivo de credenciales en el sistema de archivos local de LINUX

# COMMAND ----------

# MAGIC %sh ls /credenciales

# COMMAND ----------

# DBTITLE 1,2. Configuración del archivo de credenciales en las variables de entorno
#Creamos la variable de sistema "GOOGLE_APPLICATION_CREDENTIALS" en LINUX con la ruta del archivo de credenciales

# COMMAND ----------

# MAGIC %sh export GOOGLE_APPLICATION_CREDENTIALS="/credenciales/credenciales.json"

# COMMAND ----------

#Crearemos también la variable de configuración sobre PYTHON
#Importamos la librería para manipulación del sistema operativo
import os

# COMMAND ----------

#Creamos la variable del sistema con la ruta del archivo de credenciales
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/credenciales/credenciales.json"

# COMMAND ----------

# DBTITLE 1,3. Configuración del suscriptor del tópico
#Colocamos el suscriptor del tópico desde donde leeremos los registros
#La estructura del suscriptor es como la que sigue:
#
# projects/xxxxxxxx/locations/xxxxxxxx/subscriptions/xxxxxxxx
#
suscriptor = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

# COMMAND ----------

# DBTITLE 1,4. Binarización del archivo de credenciales
#El consumer necesita leer la información del archivo de credenciales de manera binaria
#Importamos la librería de binarización
import base64

# COMMAND ----------

#Leemos el archivo de credenciales
with open("/credenciales/credenciales.json", "rb") as archivo:
    credenciales = archivo.read()

# COMMAND ----------

#Binarizamos la información
credencialesBin = base64.b64encode(credenciales).decode("utf-8")

# COMMAND ----------

# DBTITLE 1,5. Conexión al tópico
#Creamos la conexión en tiempo real sobre el dataframe "dfStream"
#Dentro de esta variable en tiempo real se colocarán los registros desde el tópico
dfStream = spark.readStream.format("pubsublite").option("gcp.credentials.key", credencialesBin).option("pubsublite.subscription", suscriptor).load()

# COMMAND ----------

#Verificamos el esquema de metadatos
#Dentro del campo "body" se encuentran los registros binarizados
dfStream.printSchema()

# COMMAND ----------

#Iniciamos el stream para verificar con la función "display"
#Luego de iniciar el stream iniciamos el PRODUCER para verificar que los registros lleguen al CONSUMER
#Luego de verificar que los datos lleguen, detenemos el CONSUMER
display(dfStream)

# COMMAND ----------

# DBTITLE 1,6. Des-binarización de los registros
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
