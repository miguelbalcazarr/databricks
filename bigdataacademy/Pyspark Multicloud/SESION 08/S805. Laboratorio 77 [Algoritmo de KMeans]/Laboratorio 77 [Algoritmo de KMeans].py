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

#Utilitario para definir una columna que contenga a todas las columnas
from pyspark.ml.feature import VectorAssembler

# COMMAND ----------

# DBTITLE 1,2. Lectura de dataset
#Leemos el archivo de dataset
dfDataset = spark.read.format("parquet").load("dbfs:///FileStore/_pyspark/output/dataset_body/")

#Verificamos
dfDataset.show()

# COMMAND ----------

# DBTITLE 1,3. Selección del algoritmo
#Importamos el algoritmo de "KMeans"
from pyspark.ml.clustering import KMeans

# COMMAND ----------

# DBTITLE 1,4. Definición de parámetros del algoritmo
#Configuramos el algoritmo de "KMeans" para que clusterice en tres clústers los datos de la columna "features_normalizados"
algoritmo = KMeans(
  k = 3,
  featuresCol = "features_normalizados"
)

# COMMAND ----------

# DBTITLE 1,5. Entrenamiento del modelo
#Entrenamos el modelo con el algoritmo y los datos
modelo = algoritmo.fit(dfDataset)

# COMMAND ----------

# DBTITLE 1,6. Obtención de la inercia
#Obtenemos la inercia
modelo.summary.trainingCost
