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

# DBTITLE 1,2. Lectura desde capa "BRONZE"
#Lectura desde archivo de texto plano
dfRiesgo = spark.read.format("csv").option("header","true").option("delimiter",",").schema(
    StructType(
        [
            StructField("ID_CLIENTE", StringType(), True),
            StructField("RIESGO_CENTRAL_1", DoubleType(), True),
            StructField("RIESGO_CENTRAL_2", DoubleType(), True),
            StructField("RIESGO_CENTRAL_3", DoubleType(), True)
        ]
    )
).load("dbfs:///FileStore/_pyspark/deltalake/bronze/riesgo_crediticio/")

#Vemos el esquema
dfRiesgo.printSchema()
 
#Mostramos los datos
dfRiesgo.show()

# COMMAND ----------

# DBTITLE 1,3. Reglas de calidad
#Aplicamos reglas de limpieza de datos
dfRiesgoLimpio = dfRiesgo.filter(
  (dfRiesgo["ID_CLIENTE"].isNotNull()) &
  (dfRiesgo["RIESGO_CENTRAL_1"] >= 0) &
  (dfRiesgo["RIESGO_CENTRAL_2"] >= 0) &
  (dfRiesgo["RIESGO_CENTRAL_3"] >= 0) &
  (dfRiesgo["RIESGO_CENTRAL_1"] <= 1) &
  (dfRiesgo["RIESGO_CENTRAL_2"] <= 1) &
  (dfRiesgo["RIESGO_CENTRAL_3"] <= 1)
)

#Vemos el esquema
dfRiesgoLimpio.printSchema()
 
#Mostramos los datos
dfRiesgoLimpio.show()

# COMMAND ----------

# DBTITLE 1,4. Almacenamiento en capa "SILVER"
#Almacenamiento binarizado
dfRiesgoLimpio.write.format("delta").mode("overwrite").save("dbfs:///FileStore/_pyspark/deltalake/silver/riesgo_crediticio/")
