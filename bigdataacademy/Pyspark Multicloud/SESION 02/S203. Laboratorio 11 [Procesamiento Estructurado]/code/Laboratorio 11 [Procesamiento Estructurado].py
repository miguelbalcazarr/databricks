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
).load("dbfs:///FileStore/_pyspark/persona.data")

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

# COMMAND ----------

# DBTITLE 1,5. Transformation GROUP BY
#OPERACION EQUIVALENTE EN SQL:
#SELECT 
#	EDAD,
#	COUNT(EDAD),
#	MIN(FECHA_INGRESO),
#	SUM(SALARIO),
#	MAX(SALARIO),
#	AVG(SALARIO)
#FROM
#	dfData
#GROUP BY
#	EDAD

# COMMAND ----------

#Colocamos todos los utilitarios del paquete de librerías "pyspark.sql.functions" dentro de la variable "f"
import pyspark.sql.functions as f

# COMMAND ----------

#Hacemos un GROUP BY
df5 = dfData.groupBy(dfData["EDAD"]).agg(
	f.count(dfData["EDAD"]).alias("CANTIDAD"), 
	f.min(dfData["FECHA_INGRESO"]).alias("FECHA_INGRESO_MAS_ANTIGUA"), 
	f.sum(dfData["SALARIO"]).alias("SUMA_SALARIOS"), 
	f.max(dfData["SALARIO"]).alias("SALARIO_MAYOR"),
	f.avg(dfData["SALARIO"]).alias("PROMEDIO_SALARIO")
)

#Mostramos los datos
df5.show()

# COMMAND ----------

# DBTITLE 1,6. Transformation JOIN
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
).load("dbfs:///FileStore/_pyspark/transacciones.data")

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
).load("dbfs:///FileStore/_pyspark/persona.data")

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
