# Databricks notebook source
## #######################################################################################################
## 
## @copyright Big Data Academy [info@bigdataacademy.org]
## @professor Alonso Melgarejo [alonsoraulmgs@gmail.com]
## 
## #######################################################################################################

# COMMAND ----------

# DBTITLE 1,1. Librerías
#Desde la librería "pyspark.sql.types" importamos los utilitarios "StructType" y el "StructField"
#"StrucType" nos permite modificar el esquema de metadatos de un dataframe
#"StructField" nos permite modificar a un campo del esquema de metadatos, por ejemplo si el dataframe tiene 8 campos deberemos de usar 8 veces el utilitario "StructField"
from pyspark.sql.types import StructType, StructField

# COMMAND ----------

#Importamos los tipos de datos que definiremos para cada campo
from pyspark.sql.types import StringType, IntegerType, DoubleType

# COMMAND ----------

#Como todos los utilitarios están dentro del mismo paquete de libreías, también podemos importarlos así
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

#También, podemos importar todos los utilitarios dentro de un paquete de librerías para no escribirlos uno a uno
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,2. Lectura de archivos indicando el esquema de metadatos
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

#Mostramos la data
dfData.show()

# COMMAND ----------

#Mostramos el esquema de metadatos
dfData.printSchema()
