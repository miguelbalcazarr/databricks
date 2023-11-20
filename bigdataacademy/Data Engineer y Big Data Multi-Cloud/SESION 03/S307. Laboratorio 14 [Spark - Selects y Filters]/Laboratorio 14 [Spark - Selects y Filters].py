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

# COMMAND ----------

# DBTITLE 1,2. Lectura de datos
#Lectura desde archivo de texto plano indicando el esquema de metadatos (función "schema")
dfData = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("encoding", "ISO-8859-1").schema(
    StructType(
        [
            StructField("ID", StringType(), True),
            StructField("NOMBRE", StringType(), True),
            StructField("TELEFONO", StringType(), True),
            StructField("CORREO", StringType(), True),
            StructField("FECHA_INGRESO", StringType(), True),
            StructField("EDAD", IntegerType(), True),
            StructField("SALARIO", DoubleType(), True),
            StructField("ID_EMPRESA", StringType(), True)
        ]
    )
).load("dbfs:///FileStore/_bigdata/persona.data")

# COMMAND ----------

# DBTITLE 1,3. Transformation SELECT
#OPERACION EQUIVALENTE EN SQL:
#SELECT ID, NOMBRE, EDAD FROM dfData

#Seleccionamos algunas columnas
df1 = dfData.select(
  dfData["ID"], 
  dfData["NOMBRE"], 
  dfData["EDAD"]
)

#Mostramos los datos
df1.show()

# COMMAND ----------

# DBTITLE 1,4. Transformation FILTER
#OPERACION EQUIVALENTE EN SQL:
#SELECT * FROM dfData WHERE EDAD > 60

#Hacemos un filtro
df2 = dfData.filter(dfData["EDAD"] > 60)

#Mostramos los datos
df2.show()

# COMMAND ----------

#Hacemos un filtro con un "and"
#SELECT * FROM dfData WHERE EDAD > 60 AND SALARIO > 20000
df3 = dfData.filter(
  (dfData["EDAD"] > 60) & 
  (dfData["SALARIO"] > 20000) &
  (dfData["ID"].isNotNull())
)

#Mostramos los datos
df3.show()

# COMMAND ----------

#Hacemos un filtro con un "or"
#SELECT * FROM dfData WHERE EDAD > 60 OR SALARIO > 20000
df4 = dfData.filter(
  (dfData["EDAD"] > 60) |
  (dfData["SALARIO"] > 20000)
)

#Mostramos los datos
df4.show()
