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

# DBTITLE 1,2. Lectura de archivo semi-estructurado JSON
#Lectura desde archivo JSON
dfTransaccionesBancarias = spark.read.format("json").schema(
    StructType(
        [
            StructField("PERSONA", StructType([
                StructField("ID_PERSONA", StringType(), True),
                StructField("NOMBRE_PERSONA", StringType(), True),
                StructField("EDAD", IntegerType(), True),
                StructField("SALARIO", DoubleType(), True)
            ]), True),
            StructField("EMPRESA", StructType([
                StructField("ID_EMPRESA", StringType(), True),
                StructField("NOMBRE_EMPRESA", StringType(), True)
            ]), True),
            StructField("TRANSACCION", StructType([
                StructField("MONTO", DoubleType(), True),
                StructField("FECHA", StringType(), True)
            ]), True)
        ]
    )
).load("dbfs:///FileStore/_bigdata/transacciones_complejas.json")

#Vemos el esquema
dfTransaccionesBancarias.printSchema()
 
#Mostramos los datos
dfTransaccionesBancarias.show()

# COMMAND ----------

# DBTITLE 1,3. Modelamiento entidad "TRANSACCION"
#Selección de campos que definen a la entidad
dfTransaccion = dfTransaccionesBancarias.select(
  dfTransaccionesBancarias["PERSONA.ID_PERSONA"],
  dfTransaccionesBancarias["EMPRESA.ID_EMPRESA"],
  dfTransaccionesBancarias["TRANSACCION.MONTO"],
  dfTransaccionesBancarias["TRANSACCION.FECHA"]
)
 
#Mostramos los datos
dfTransaccion.show()

# COMMAND ----------

#Implementación de reglas de calidad
dfTransaccionLimpio = dfTransaccion.filter(
  (dfTransaccion["ID_PERSONA"].isNotNull()) &
  (dfTransaccion["ID_EMPRESA"].isNotNull()) &
  (dfTransaccion["MONTO"] >= 0) &
  (dfTransaccion["MONTO"] < 10000)
)
 
#Mostramos
dfTransaccionLimpio.show()

# COMMAND ----------

# DBTITLE 1,4. Modelamiento entidad "EMPRESA"
#Selección de campos que definen a la entidad
#Usamos la función distinct para obtener registros únicos de las empresas
dfEmpresa = dfTransaccionesBancarias.select(
  dfTransaccionesBancarias["EMPRESA.ID_EMPRESA"],
  dfTransaccionesBancarias["EMPRESA.NOMBRE_EMPRESA"]
).distinct()
 
#Mostramos los datos
dfEmpresa.show()

# COMMAND ----------

#Implementación de reglas de calidad
dfEmpresaLimpio = dfEmpresa.filter(
  (dfEmpresa["ID_EMPRESA"].isNotNull())
)
 
#Mostramos
dfEmpresaLimpio.show()

# COMMAND ----------

# DBTITLE 1,5. Modelamiento entidad "PERSONA"
#Selección de campos que definen a la entidad
#Usamos la función distinct para obtener registros únicos de las persona
dfPersona = dfTransaccionesBancarias.select(
    dfTransaccionesBancarias["PERSONA.ID_PERSONA"],
    dfTransaccionesBancarias["PERSONA.NOMBRE_PERSONA"],
    dfTransaccionesBancarias["PERSONA.EDAD"],
    dfTransaccionesBancarias["PERSONA.SALARIO"]
).distinct()
 
#Mostramos los datos
dfPersona.show()

# COMMAND ----------

#Implementación de reglas de calidad
dfPersonaLimpio = dfPersona.filter(
  (dfPersona["ID_PERSONA"].isNotNull()) &
  (dfPersona["NOMBRE_PERSONA"].isNotNull()) &
  (dfPersona["SALARIO"] >= 0) &
  (dfPersona["SALARIO"] <= 10000) &
  (dfPersona["EDAD"] > 0) &
  (dfPersona["EDAD"] < 60)
)
 
#Mostramos
dfPersonaLimpio.show()
