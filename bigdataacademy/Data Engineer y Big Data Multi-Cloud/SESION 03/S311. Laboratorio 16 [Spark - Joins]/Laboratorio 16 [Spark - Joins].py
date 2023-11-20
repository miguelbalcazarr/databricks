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

# DBTITLE 1,2. Transformation JOIN
#Leemos el archivo TRANSACCION
dfTransaccion = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("encoding", "ISO-8859-1").schema(
    StructType(
        [
            StructField("ID_PERSONA", StringType(), True),
            StructField("ID_EMPRESA", StringType(), True),
            StructField("MONTO", DoubleType(), True),
            StructField("FECHA", StringType(), True)
        ]
    )
).load("dbfs:///FileStore/_bigdata/transacciones.data")

#Mostramos los datos
dfTransaccion.show()

# COMMAND ----------

#Leemos el archivo PERSONA
dfPersona = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("encoding", "ISO-8859-1").schema(
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

#Mostramos los datos
dfPersona.show()

# COMMAND ----------

#TIPOS DE JOINS DISPONIBLES
#
# - inner (JOIN ESTÁNDAR)
# - left
# - rigth
# - outer
# - full
# - cross

# COMMAND ----------

#Ejecución del JOIN
dfJoin = dfTransaccion.join(
    dfPersona,
    dfTransaccion["ID_PERSONA"] == dfPersona["ID"],
    "inner"
).select(
    dfTransaccion["ID_PERSONA"],
    dfPersona["NOMBRE"],
    dfPersona["EDAD"],
    dfPersona["SALARIO"],
    dfPersona["ID_EMPRESA"].alias("ID_EMPRESA_TRABAJO"),
    dfTransaccion["ID_EMPRESA"].alias("ID_EMPRESA_TRANSACCION"),
    dfTransaccion["MONTO"],
    dfTransaccion["FECHA"]
)

#Mostramos los datos
dfJoin.show()

# COMMAND ----------

# DBTITLE 1,3. Transformation JOIN (usando alias)
#Ejecución del JOIN [CON ALIAS]
dfJoin = dfTransaccion.alias("T").join(
    dfPersona.alias("P"),
    f.col("T.ID_PERSONA") == f.col("P.ID"),
    "inner"
).select(
    f.col("T.ID_PERSONA"),
    f.col("P.NOMBRE"),
    f.col("P.EDAD"),
    f.col("P.SALARIO"),
    f.col("P.ID_EMPRESA").alias("ID_EMPRESA_TRABAJO"),
    f.col("T.ID_EMPRESA").alias("ID_EMPRESA_TRANSACCION"),
    f.col("T.MONTO"),
    f.col("T.FECHA")
)

#Mostramos los datos
dfJoin.show()
