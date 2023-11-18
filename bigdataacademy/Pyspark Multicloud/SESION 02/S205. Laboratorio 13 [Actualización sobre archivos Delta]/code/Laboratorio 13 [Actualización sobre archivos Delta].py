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
 
#Mostramos los datos
dfData.show()

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

# DBTITLE 1,4 Almacenamiento [Delta]
# MAGIC %fs rm -r dbfs:///FileStore/_pyspark/dfResultadoDelta

# COMMAND ----------

#Almacenamiento en formato binario
dfResultado.write.format("delta").option("compression", "snappy").mode("overwrite").save("dbfs:///FileStore/_pyspark/dfResultadoDelta")

# COMMAND ----------

#Leemos
dfResultadoLeido = spark.read.format("delta").load("dbfs:///FileStore/_pyspark/dfResultadoDelta")
 
#Verificamos
dfResultadoLeido.show()

# COMMAND ----------

#Contamos cuántos registros hay
dfResultadoLeido.count()

# COMMAND ----------

# DBTITLE 1,5. Delete de registros en archivos DELTA
#Importamos la librería que nos permite modificar archivos DELTA
from delta.tables import DeltaTable

# COMMAND ----------

#Nos conectamos a los archivos en modo "TABLA DELTA" para hacer modificaciones
deltaTable = DeltaTable.forPath(spark, "dbfs:///FileStore/_pyspark/dfResultadoDelta")

# COMMAND ----------

#Ejecutamos una consulta de eliminación de registros
#Colocamos las condiciones de eliminación
deltaTable.delete(
  (f.col("EDAD") < 50) &
  (f.col("SUMA_SALARIOS") < 40000)
)

# COMMAND ----------

#Leemos
dfResultadoLeido = spark.read.format("delta").load("dbfs:///FileStore/_pyspark/dfResultadoDelta")
 
#Verificamos
dfResultadoLeido.show()

# COMMAND ----------

#Contamos cuántos registros hay
dfResultadoLeido.count()

# COMMAND ----------

# DBTITLE 1,6. Update de registros en archivo DELTA
#Para actualizar:
# 1. Colocamos las condiciones de búsqueda de registros
# 2. Colocamos las actualizaciones para los registros que coincidan
deltaTable.update(
  (
    (f.col("EDAD") <= 40) &
    (f.col("CANTIDAD") == 5)
  ),
  {
    "FECHA_INGRESO_MAS_ANTIGUA": f.lit("2022-01-01"),
    "SALARIO_MAYOR": f.lit("9999")
  }
)

# COMMAND ----------

#Leemos
dfResultadoLeido = spark.read.format("delta").load("dbfs:///FileStore/_pyspark/dfResultadoDelta")
 
#Verificamos
dfResultadoLeido.show()
