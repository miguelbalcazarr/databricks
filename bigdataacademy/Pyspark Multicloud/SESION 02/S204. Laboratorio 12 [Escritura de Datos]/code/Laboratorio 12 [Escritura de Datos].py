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

# DBTITLE 1,3. Procesamiento
#Hacemos un GROUP BY
dfResultado = dfData.groupBy(dfData["EDAD"]).agg(
	f.count(dfData["EDAD"]).alias("CANTIDAD"), 
	f.min(dfData["FECHA_INGRESO"]).alias("FECHA_INGRESO_MAS_ANTIGUA"), 
	f.sum(dfData["SALARIO"]).alias("SUMA_SALARIOS"), 
	f.max(dfData["SALARIO"]).alias("SALARIO_MAYOR"),
    f.avg(dfData["SALARIO"]).alias("PROMEDIO_SALARIO")
)

#Mostramos los datos
dfResultado.show()

# COMMAND ----------

# DBTITLE 1,4. Almacenamiento [TEXTO PLANO]
#Almacenamiento en texto plano
dfResultado.write.format("csv").option("header", "true").option("delimiter", "|").option("encoding", "ISO-8859-1").mode("overwrite").save("dbfs:///FileStore/_pyspark/dfResultadoText")

# COMMAND ----------

# MAGIC %fs ls dbfs:///FileStore/_pyspark/dfResultadoText

# COMMAND ----------

#Leemos
dfResultadoLeido = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("encoding", "ISO-8859-1").load("dbfs:///FileStore/_pyspark/dfResultadoText")

#Verificamos
dfResultadoLeido.show()

# COMMAND ----------

# DBTITLE 1,5. Almacenamiento [Binario - Parquet]
# MAGIC %fs rm -r dbfs:///FileStore/_pyspark/dfResultadoParquet

# COMMAND ----------

#Almacenamiento en formato binario
dfResultado.write.format("parquet").option("compression", "snappy").mode("overwrite").save("dbfs:///FileStore/_pyspark/dfResultadoParquet")

# COMMAND ----------

# MAGIC %fs ls dbfs:///FileStore/_pyspark/dfResultadoParquet

# COMMAND ----------

#Leemos
dfResultadoLeido = spark.read.format("parquet").load("dbfs:///FileStore/_pyspark/dfResultadoParquet")
 
#Verificamos
dfResultadoLeido.show()

# COMMAND ----------

# DBTITLE 1,6. Almacenamiento [Binario - Delta]
# MAGIC %fs rm -r dbfs:///FileStore/_pyspark/dfResultadoDelta

# COMMAND ----------

#Almacenamiento en formato binario
dfResultado.write.format("delta").option("compression", "snappy").mode("overwrite").save("dbfs:///FileStore/_pyspark/dfResultadoDelta")

# COMMAND ----------

# MAGIC %fs ls dbfs:///FileStore/_pyspark/dfResultadoDelta

# COMMAND ----------

#Leemos
dfResultadoLeido = spark.read.format("delta").load("dbfs:///FileStore/_pyspark/dfResultadoDelta")

#Verificamos
dfResultadoLeido.show()
