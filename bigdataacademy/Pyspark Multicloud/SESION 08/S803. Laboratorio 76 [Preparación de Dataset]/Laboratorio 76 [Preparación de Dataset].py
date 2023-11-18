# Databricks notebook source
# DBTITLE 1,1. Librerías
#Utilitarios para modificar el esquema de metadatos
from pyspark.sql.types import StructType, StructField

#Importamos los tipos de datos que definiremos para cada campo
from pyspark.sql.types import StringType, IntegerType, DoubleType

#Importamos la librería de pandas compatible con entornos de clúster de Big Data
import pyspark.pandas as pd

#Por defecto un dataframe Pandas muestra 1000 registros
#Vamos a indicarle que sólo muestre 20 para que no se sature el notebook
pd.set_option('display.max_rows', 20)

# COMMAND ----------

# DBTITLE 1,2. Lectura de archivos [SPARK]
#Lectura desde archivo de texto plano indicando el esquema de metadatos (función "schema")
dfRaw = spark.read.format("csv").option("header", "true").option("delimiter", ",").option("encoding", "ISO-8859-1").schema(
    StructType(
        [
          StructField("Sexo", StringType(), True),
          StructField("Estado civil", StringType(), True),
          StructField("Densidad corporal", DoubleType(), True),
          StructField("Porcentaje de grasa", DoubleType(), True),
          StructField("Edad (años)", DoubleType(), True),
          StructField("Peso (libras)", DoubleType(), True),
          StructField("Altura (pulgadas)", DoubleType(), True),
        ]
    )
).load("dbfs:///FileStore/_pyspark/body.csv")

# COMMAND ----------

#Mostramos los datos
dfRaw.show()

# COMMAND ----------

#Vemos el esquema de metadatos
dfRaw.printSchema()

# COMMAND ----------

# DBTITLE 1,3. Limpieza de datos [SPARK]
#Aplicamos reglas de limpieza de datos
dfRaw1 = dfRaw.filter(
  (dfRaw["Densidad corporal"] > 0) &
  (dfRaw["Porcentaje de grasa"] > 0) &
  (dfRaw["Edad (años)"] > 0) &
  (dfRaw["Edad (años)"] < 100) &
  (dfRaw["Peso (libras)"] > 0) &
  (dfRaw["Altura (pulgadas)"] > 0)
)

#Verificamos
dfRaw1.show()

# COMMAND ----------

# DBTITLE 1,4. Creación de columnas dummy [PANDAS]
#Convertimos el dataframe SPARK a un dataframe PANDAS
dfpRaw1 = pd.from_pandas(dfRaw1.toPandas())

#Verificamos
dfpRaw1

# COMMAND ----------

#Conversión de variables categóricas a variables numéricas en columnas dummy
#Colocamos el parámetro "drop_first" en "False" para evitar que se elimine la columna original "Sexo"
dfpRaw2 = pd.get_dummies(dfpRaw1, columns = [
  "Sexo",
  "Estado civil"
])

#Verificamos
dfpRaw2

# COMMAND ----------

#Concatenamos la columna original
dfpRaw3 = pd.concat(
  [dfpRaw1[["Sexo", "Estado civil"]], dfpRaw2], 
  axis = 1
)

#Verificamos
dfpRaw3

# COMMAND ----------

# DBTITLE 1,5. Normalización de los datos
#Convertimos el dataframe PANDAS a un dataframe SPARK
dfRaw3 = dfpRaw3.to_spark()

# COMMAND ----------

#Utilitario para definir una columna que contenga a todas las columnas
from pyspark.ml.feature import VectorAssembler

# COMMAND ----------

#Colocamos todas las columnas en una nueva columna llamada "features"
dfRaw4 = VectorAssembler(
  inputCols = [
    "Densidad corporal", 
    "Porcentaje de grasa", 
    "Edad (años)", 
    "Peso (libras)", 
    "Altura (pulgadas)", 
    "Sexo_FEMENINO", 
    "Sexo_MASCULINO", 
    "Sexo_NOREGISTRADO",
    "Estado civil_CASADO",
    "Estado civil_SOLTERO"
  ], 
  outputCol = "features"
).transform(dfRaw3)

#Verificamos
dfRaw4.show()

# COMMAND ----------

#Utilitario para normalizar los datos entre cierto rango
from pyspark.ml.feature import MinMaxScaler

# COMMAND ----------

#Generamos el objeto escalador con los datos
#El objeto escalador reconoce los mínimos y máximos de cada columna
#Colocará los objetos escalados en la columna "features_normalizados"
escalador = MinMaxScaler(inputCol="features", outputCol="features_normalizados").fit(dfRaw4)

# COMMAND ----------

#Normalizamos los datos
dfRaw5 = escalador.transform(dfRaw4)

#Verificamos
dfRaw5.show()

# COMMAND ----------

#Vemos los campos que nos interesan
dfRaw5.select(
  dfRaw5["features"], 
  dfRaw5["features_normalizados"]
).show()

# COMMAND ----------

#Vemos los campos que nos interesan, no truncándolos
dfRaw5.select(
  dfRaw5["features"], 
  dfRaw5["features_normalizados"]
).show(20, False)

# COMMAND ----------

# DBTITLE 1,6. Almacenamiento del dataset preparado
#Almacenamos el dataframe listo para ser analizado
#Lo guardamos en un formato binario de rápido procesamiento (parquet)
dfRaw5.write.format("parquet").mode("overwrite").option("compression", "snappy").save("dbfs:///FileStore/_pyspark/output/dataset_body/")

# COMMAND ----------

#Para recuperarlo, lo hacemos de la siguiente manera
dfRaw4 = spark.read.format("parquet").load("dbfs:///FileStore/_pyspark/output/dataset_body/")

#Verificamos
dfRaw4.show()
