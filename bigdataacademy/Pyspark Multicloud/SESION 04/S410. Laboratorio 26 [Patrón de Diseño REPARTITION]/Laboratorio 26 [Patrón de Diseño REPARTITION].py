# Databricks notebook source
## #######################################################################################################
## 
## @copyright Big Data Academy [info@bigdataacademy.org]
## @professor Alonso Melgarejo [alonsoraulmgs@gmail.com]
## 
## #######################################################################################################

# COMMAND ----------

# DBTITLE 1,1. Librerías
#Estos objetos nos ayudarán a definir la metadata
from pyspark.sql.types import StructType, StructField
 
#Importamos los tipos de datos que usaremos
from pyspark.sql.types import StringType, IntegerType, DoubleType
 
#Para importarlos todos usamos la siguiente linea
from pyspark.sql.types import *
 
#Colocamos todos los utilitarios del paquete de librerías "pyspark.sql.functions" dentro de la variable "f"
import pyspark.sql.functions as f

# COMMAND ----------

# DBTITLE 1,2. Lectura
#Leemos el archivo indicando el esquema
dfTransaccion = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("encoding", "ISO-8859-1").schema(
    StructType(
        [
            StructField("ID_PERSONA", StringType(), True),
            StructField("ID_EMPRESA", StringType(), True),
            StructField("MONTO", DoubleType(), True),
            StructField("FECHA", StringType(), True)
        ]
    )
).load("dbfs:///FileStore/_pyspark/DATA_TRANSACCION.txt")

#Mostramos los datos
dfTransaccion.show()

# COMMAND ----------

# DBTITLE 1,3. Cálculo de particiones en un dataframe
#Averiguar el número de particiones de un dataframe
particionesActuales = dfTransaccion.rdd.getNumPartitions()
print(particionesActuales)

# COMMAND ----------

#Cada partición debe tener 100 mil registros
#En total el dataframe tiene 235040, necesitaremos 3 particiones
cantidadDeRegistros = dfTransaccion.count()
print(cantidadDeRegistros)

# COMMAND ----------

#Obtenemos el número de particiones ideal
numeroDeParticiones = (cantidadDeRegistros / 100000.0)
print(numeroDeParticiones)

# COMMAND ----------

#Deberemos redondear hacia arriba
#Importamos la librería que contiene el utilitario
import math

# COMMAND ----------

#Redondeamos hacia arriba
numeroDeParticionesRedondeado = math.ceil(numeroDeParticiones)
print(numeroDeParticionesRedondeado)

# COMMAND ----------

# DBTITLE 1,4. Reparticionamiento hacía arriba
#Llamamos a la función de reparticionamiento
dfTransaccion = dfTransaccion.repartition(numeroDeParticionesRedondeado)

# COMMAND ----------

#Verificamos
particionesActuales = dfTransaccion.rdd.getNumPartitions()
print(particionesActuales)

# COMMAND ----------

# DBTITLE 1,5. Reparticionamiento hacía abajo
#Llamamos a la función de reparticionamiento
dfTransaccion = dfTransaccion.coalesce(3)

# COMMAND ----------

#Verificamos
particionesActuales = dfTransaccion.rdd.getNumPartitions()
print(particionesActuales)

# COMMAND ----------

# DBTITLE 1,6. Función utilitaria de reparticionamiento
#Función de reparticionamiento
def reparticionar(df):
    dfReparticionado = None

    #Averiguar el número de particiones de un dataframe
    particionesActuales = dfTransaccion.rdd.getNumPartitions()

    #Contamos la cantidad de registros actuales
    cantidadDeRegistros = dfTransaccion.count()

    #Obtenemos el número de particiones ideal
    numeroDeParticiones = (cantidadDeRegistros / 100000.0)

    #Redondeamos hacia arriba
    numeroDeParticionesRedondeado = math.ceil(numeroDeParticiones)
    
    #Reparticionamos
    if (numeroDeParticionesRedondeado > particionesActuales):
        #Redondeamos hacia arriba
        dfReparticionado = df.repartition(numeroDeParticionesRedondeado)
    else:
        #Redondeamos hacia abajo
        dfReparticionado = df.coalesce(numeroDeParticionesRedondeado)
    
    return dfReparticionado

# COMMAND ----------

#Ejemplo de uso
 
#Leemos el archivo indicando el esquema
dfTransaccion = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("encoding", "ISO-8859-1").schema(
    StructType(
        [
            StructField("ID_PERSONA", StringType(), True),
            StructField("ID_EMPRESA", StringType(), True),
            StructField("MONTO", DoubleType(), True),
            StructField("FECHA", StringType(), True)
        ]
    )
).load("dbfs:///FileStore/_pyspark/DATA_TRANSACCION.txt")
 
#Reparticionamos
dfTransaccion = reparticionar(dfTransaccion)
 
#Mostramos las particiones
dfTransaccion.rdd.getNumPartitions()
