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

#Importamos el algoritmo de "KMeans"
from pyspark.ml.clustering import KMeans

#Importamos la librería de gráficos
import matplotlib.pyplot as plt

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
#Valor de k calibrado
k_calibrado = 5

# COMMAND ----------

#Configuramos el algoritmo de "KMeans" para que clusterice en tres clústers los datos de la columna "features_normalizados"
algoritmo = KMeans(
  k = k_calibrado,
  featuresCol = "features_normalizados"
)

# COMMAND ----------

# DBTITLE 1,5. Entrenamiento del modelo
#Entrenamos el modelo con el algoritmo y los datos
modelo = algoritmo.fit(dfDataset)

# COMMAND ----------

# DBTITLE 1,6. Asignación de etiqueta de clúster a cada elemento del dataset
#Asignamos las etiquetas de clúster (clúster "0", "1" y "2") a cada registro del dataset
dfResultado1 = modelo.transform(dfDataset)

#Verificamos
dfResultado1.show()

# COMMAND ----------

#Obtenemos las columnas que le interesan a negocio
#La asignación de cada registro al clúster está en la columna "prediction"
dfResultado2 = dfResultado1.select(
  dfResultado1["Sexo"],
  dfResultado1["Estado civil"],
  dfResultado1["Densidad corporal"],
  dfResultado1["Porcentaje de grasa"],
  dfResultado1["Edad (años)"],
  dfResultado1["Peso (libras)"],
  dfResultado1["Altura (pulgadas)"],
  dfResultado1["prediction"].alias("Cluster")
)

#Verificamos
dfResultado2.show()

# COMMAND ----------

#Almacenamos el resultado en 1 archivo de texto plano
dfResultado2.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save("dbfs:///FileStore/_pyspark/output/dataset_body_cluster/")
