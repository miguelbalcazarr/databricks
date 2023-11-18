# Databricks notebook source
## #######################################################################################################
## 
## @copyright Big Data Academy [info@bigdataacademy.org]
## @professor Alonso Melgarejo [alonsoraulmgs@gmail.com]
## 
## #######################################################################################################

# COMMAND ----------

# DBTITLE 1,1. Encriptación de la clave de acceso para el consumer
#Colocamos la clave de acceso copiada desde la directiva "CONSUMER" en "AZURE"
claveDeAcceso = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

# COMMAND ----------

#El consumer necesita encriptar la clave para poder acceder al consumer
#La función de encriptación se encuentra dentro de una libría de JAVA
#Para acceder a librerías JAVA desde SPARK tenemos a la variable "sc"
#Encriptamos la clave de acceso
claveDeAccesoEncriptada = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(claveDeAcceso)

# COMMAND ----------

# DBTITLE 1,2. Conexión al tópico
#Creamos la conexión en tiempo real sobre el dataframe "dfStream"
#Dentro de esta variable en tiempo real se colocarán los registros desde el tópico
dfStream = spark.readStream.format("eventhubs").option("eventhubs.connectionString", claveDeAccesoEncriptada).option("startingOffsets", "latest").load()

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

# DBTITLE 1,3. Des-binarización de los registros
#Seleccionamos la columna "body" y des-binarizamos el contenido
df1 = dfStream.select(
    dfStream["body"].cast("string")
)

# COMMAND ----------

#Verificamos el esquema de metadatos
#Dentro del campo "body" se encontrarán los registros como "strings"
df1.printSchema()

# COMMAND ----------

#Iniciamos el stream para verificar con la función "display"
#Luego de iniciar el stream iniciamos el PRODUCER para verificar que los registros lleguen al CONSUMER
#Luego de verificar que los datos lleguen, detenemos el CONSUMER
display(df1)
