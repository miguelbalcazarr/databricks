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

# DBTITLE 1,4. Definición de rango de parámetros
#Definimos un array de 2 a 10
array_k = list(range(2, 10))

#Verificamos
array_k

# COMMAND ----------

# DBTITLE 1,5. Calibración del valor de K
#Colocaremos las inercias de cada modelo en un array
array_inercia = []

#Iteramos cada elemento del array
for k in array_k:
  print(f"Valor de K = {k}")
  
  #Configuramos el algoritmo de "KMeans" para que clusterice en tres clústers los datos de la columna "features_normalizados"
  algoritmo = KMeans(
    k = k,
    featuresCol = "features_normalizados"
  )
  
  #Entrenamos el modelo con el algoritmo y los datos
  modelo = algoritmo.fit(dfDataset)
  
  #Colocamos la inercia
  array_inercia.append(modelo.summary.trainingCost)

# COMMAND ----------

#Verificamos el array de inercias
array_inercia

# COMMAND ----------

# DBTITLE 1,6. Gráfico de codo de inercias
#Importamos la librería de gráficos
import matplotlib.pyplot as plt

# COMMAND ----------

#Grafimos los k y sus inercias
plt.plot(
    array_k, 
    array_inercia, 
    marker = "o"
)

#Agregamos las etiquetas
plt.xlabel('K')
plt.ylabel('Inercia')

#Mostramos el grafico
plt.show()

# COMMAND ----------

#Vemos que el codo se forma en el valor:
k_calibrado = 5
