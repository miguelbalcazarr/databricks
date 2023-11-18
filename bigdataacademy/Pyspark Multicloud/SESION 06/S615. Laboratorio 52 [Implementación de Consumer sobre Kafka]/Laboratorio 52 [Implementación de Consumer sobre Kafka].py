# Databricks notebook source
## #######################################################################################################
## 
## @copyright Big Data Academy [info@bigdataacademy.org]
## @professor Alonso Melgarejo [alonsoraulmgs@gmail.com]
## 
## #######################################################################################################

# COMMAND ----------

# DBTITLE 1,2. Conexión al tópico
#Creamos la conexión en tiempo real sobre el dataframe "dfStream"
#Dentro de esta variable en tiempo real se colocarán los registros desde el tópico
dfStream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "transaccion").option("startingOffsets", "latest").load()

# COMMAND ----------

#Verificamos el esquema de metadatos
#Dentro del campo "value" se encuentran los registros binarizados
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
    dfStream["value"].cast("string")
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

# COMMAND ----------

# DBTITLE 1,4. Conversión de "strings" a "json"
#Convertiremos los registros de STRINGS a JSON para poder procesarlos
#Deberemos definir el esquema de metadatos
#Importamos el utilitario para modificar el esquema de metadatos de un dataframe
from pyspark.sql.types import StructType

# COMMAND ----------

#Un esquema de metadatos está conformado por varios campos
#Importamos el utilitario que define un campo de un dataframe
from pyspark.sql.types import StructField

# COMMAND ----------

#Cuando modifiquemos un campo con "StructField" debemos de indicar su tipo de dato
#Importamos el utilitario para definir un campo como "STRING"
from pyspark.sql.types import StringType

# COMMAND ----------

#Definimos el esquema de metadatos de los registros JSON:
#
# message: STRING
# iss_position: COMPLEJO
#   longitude: STRING
#   latitude: STRING
# timestamp: STRING
#
schema = StructType([
  StructField("message", StringType(), True),
  StructField("iss_position", StructType([
    StructField("longitude", StringType(), True),
    StructField("latitude", StringType(), True),
  ]), True),
  StructField("timestamp", StringType(), True)
])

# COMMAND ----------

#Necesitaremos el utilitario que convierte un STRING a un JSON
import pyspark.sql.functions as f

# COMMAND ----------

#Convertimos el campo DATA de STRING a JSON
#Usaremos la función "withColumn" que agrega una nueva columna al dataframe
df2 = df1.withColumn(
    "json", #Nombre de la nueva columna
    f.from_json(df1["value"], schema) #Función que convierte el campo "value" a un JSON
)

# COMMAND ----------

#Verificamos el esquema de metadatos
#Dentro del campo "json" se encontrarán los campos y sub-campos de los registros JSON
df2.printSchema()

# COMMAND ----------

#Iniciamos el stream para verificar con la función "display"
#Luego de iniciar el stream iniciamos el PRODUCER para verificar que los registros lleguen al CONSUMER
#Luego de verificar que los datos lleguen, detenemos el CONSUMER
display(df2)

# COMMAND ----------

# DBTITLE 1,5. Modelamiento semi-estructurado en tiempo real
#Desde el campo "json" navegamos por los sub-campos para estructurar el registro
#También indicamos los tipos de datos
df3 = df2.select(
    df2["json.message"].cast("string"),
    df2["json.iss_position.longitude"].cast("double"),
    df2["json.iss_position.latitude"].cast("double"),
    df2["json.timestamp"].cast("int")
)

# COMMAND ----------

#Verificamos el esquema de metadatos
#Vemos que los tipos de datos son los correctos
df3.printSchema()

# COMMAND ----------

#Iniciamos el stream para verificar con la función "display"
#Luego de iniciar el stream iniciamos el PRODUCER para verificar que los registros lleguen al CONSUMER
#Luego de verificar que los datos lleguen, detenemos el CONSUMER
display(df3)

# COMMAND ----------

# DBTITLE 1,5. Procesamiento en real-time
#Una vez modelado el stream, los data engineer comienzan a procesar
#Por ejemplo podemos quedarnos con aquellos registros que tengan el campo "message" en "success"
dfResultado = df3.filter(df3["message"] == "success")

# COMMAND ----------

#Iniciamos el stream para verificar con la función "display"
#Luego de iniciar el stream iniciamos el PRODUCER para verificar que los registros lleguen al CONSUMER
#Luego de verificar que los datos lleguen, detenemos el CONSUMER
display(dfResultado)

# COMMAND ----------

# DBTITLE 1,6. Almacenamiento en real-time
#Finalmente el dataframe resultante se almacena en el storage
#El proceso se ejecuta cada 1 segundo, extrae los registros del tópico, los procesa y los almacena
dfResultado.writeStream.format("parquet").outputMode("append").option("checkpointLocation", "dbfs:///FileStore/_pyspark/resultado_kafka/_checkpoints/resultado").trigger(processingTime = "1 second").start("dbfs:///FileStore/_pyspark/resultado_kafka").awaitTermination()
