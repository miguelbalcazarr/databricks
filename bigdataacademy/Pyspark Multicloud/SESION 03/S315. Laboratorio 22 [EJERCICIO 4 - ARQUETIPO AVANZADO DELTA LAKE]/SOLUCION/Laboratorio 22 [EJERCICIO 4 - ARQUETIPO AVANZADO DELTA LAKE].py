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

#Utilitario para crear un UDF
from pyspark.sql.functions import udf

#Utilitario para enviarle varios parámetros a un UDF
from pyspark.sql.functions import struct

# COMMAND ----------

# DBTITLE 1,2. Taxonomía Delta Lake
#Eliminamos la carpeta raíz del DELTA LAKE

# COMMAND ----------

# MAGIC %fs rm -r dbfs:///FileStore/_pyspark/EJERCICIO_4

# COMMAND ----------

#Creamos la carpeta raíz del DELTA LAKE

# COMMAND ----------

# MAGIC %fs mkdirs dbfs:///FileStore/_pyspark/EJERCICIO_4

# COMMAND ----------

#Copiamos el archivo "RIESGO_CREDITICIO.data" en la capa "BRONZE"

# COMMAND ----------

# MAGIC %fs cp dbfs:///FileStore/_pyspark/RIESGO_CREDITICIO.data dbfs:///FileStore/_pyspark/EJERCICIO_4/bronze/riesgo_crediticio/

# COMMAND ----------

#Copiamos el archivo "transacciones_bancarias.json" en la capa "BRONZE"

# COMMAND ----------

# MAGIC %fs cp dbfs:///FileStore/_pyspark/transacciones_bancarias.json dbfs:///FileStore/_pyspark/EJERCICIO_4/bronze/transacciones_bancarias/

# COMMAND ----------

# DBTITLE 1,3. Lectura de datos
#Lectura de datos
dfRiesgo = spark.read.format("csv").option("header","true").option("delimiter",",").schema(
    StructType(
        [
            StructField("ID_CLIENTE", StringType(), True),
            StructField("RIESGO_CENTRAL_1", DoubleType(), True),
            StructField("RIESGO_CENTRAL_2", DoubleType(), True),
            StructField("RIESGO_CENTRAL_3", DoubleType(), True)
        ]
    )
).load("dbfs:///FileStore/_pyspark/EJERCICIO_4/bronze/riesgo_crediticio/")

#Vemos el esquema
dfRiesgo.printSchema()
 
#Mostramos los datos
dfRiesgo.show()

# COMMAND ----------

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
).load("dbfs:///FileStore/_pyspark/EJERCICIO_4/bronze/transacciones_bancarias/")

#Vemos el esquema
dfTransaccionesBancarias.printSchema()
 
#Mostramos los datos
dfTransaccionesBancarias.show()

# COMMAND ----------

# DBTITLE 1,4. Modelamiento entidad "RIESGO"
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

#Almacenamiento binarizado en capa "SILVER"
dfRiesgoLimpio.write.format("delta").mode("overwrite").save("dbfs:///FileStore/_pyspark/EJERCICIO_4/silver/riesgo_crediticio/")

# COMMAND ----------

# DBTITLE 1,5. Modelamiento entidad "TRANSACCION"
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

# DBTITLE 1,6. Modelamiento entidad "EMPRESA"
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

#Almacenamiento binarizado en capa "SILVER"
dfEmpresaLimpio.write.format("delta").mode("overwrite").save("dbfs:///FileStore/_pyspark/EJERCICIO_4/silver/empresa/")

# COMMAND ----------

# DBTITLE 1,7. Modelamiento entidad "PERSONA"
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

# COMMAND ----------

#Almacenamiento binarizado en capa "SILVER"
dfPersonaLimpio.write.format("delta").mode("overwrite").save("dbfs:///FileStore/_pyspark/EJERCICIO_4/silver/persona/")

# COMMAND ----------

# DBTITLE 1,8. Creación de UDF
#Implementamos la función
def calcularRiesgoPonderado(riesgo1, riesgo2, riesgo3):
  resultado = 0
 
  resultado = (2*riesgo1 + 3*riesgo2 + 2*riesgo3) / 7
 
  return resultado

# COMMAND ----------

#Creamos el UDF
udfCalcularRiesgoPonderado = udf(
    (
      lambda parametros : calcularRiesgoPonderado(
        parametros[0], 
        parametros[1],
        parametros[2]
      )
    ),
    DoubleType()
)

# COMMAND ----------

#Aplicamos la función
dfRiesgoPonderado = dfRiesgoLimpio.select(
    dfRiesgoLimpio["ID_CLIENTE"].alias("ID_CLIENTE"),
    udfCalcularRiesgoPonderado(
        struct(
          dfRiesgoLimpio["RIESGO_CENTRAL_1"],
          dfRiesgoLimpio["RIESGO_CENTRAL_2"],
          dfRiesgoLimpio["RIESGO_CENTRAL_3"],
        )
    ).alias("RIESGO_PONDERADO")
)
 
#Mostramos los datos
dfRiesgoPonderado.show()

# COMMAND ----------

# DBTITLE 1,9. Preparación de tablones
#PASO 1 (<<T_P>>): AGREGAR LOS DATOS DE LAS PERSONAS QUE REALIZARON LAS TRANSACCIONES
df1 = dfTransaccionLimpio.join(
  dfPersonaLimpio,
  dfTransaccionLimpio["ID_PERSONA"] == dfPersonaLimpio["ID_PERSONA"],
  "inner"
).select(
  dfTransaccionLimpio["ID_PERSONA"],
  dfPersonaLimpio["NOMBRE_PERSONA"].alias("NOMBRE_PERSONA"),
  dfPersonaLimpio["EDAD"].alias("EDAD_PERSONA"),
  dfPersonaLimpio["SALARIO"].alias("SALARIO_PERSONA"),
  dfTransaccionLimpio["ID_EMPRESA"].alias("ID_EMPRESA_TRANSACCION"),
  dfTransaccionLimpio["MONTO"].alias("MONTO_TRANSACCION"),
  dfTransaccionLimpio["FECHA"].alias("FECHA_TRANSACCION")
)
 
#Mostramos los datos
df1.show()

# COMMAND ----------

#PASO 2 (<<T_P_E>>): AGREGAR LOS DATOS DE LAS EMPRESAS EN DONDE SE REALIZARON LAS TRANSACCIONES
dfTablon = df1.join(
  dfEmpresaLimpio,
  df1["ID_EMPRESA_TRANSACCION"] == dfEmpresaLimpio["ID_EMPRESA"],
  "inner"
).select(
  df1["ID_PERSONA"],
  df1["NOMBRE_PERSONA"],
  df1["EDAD_PERSONA"],
  df1["SALARIO_PERSONA"],
  df1["ID_EMPRESA_TRANSACCION"],
  dfEmpresaLimpio["NOMBRE_EMPRESA"].alias("NOMBRE_EMPRESA"),
  df1["MONTO_TRANSACCION"],
  df1["FECHA_TRANSACCION"]
)
 
#Mostramos los datos
dfTablon.show()

# COMMAND ----------

#PASO 4 (<<T_P_E_R>>): Agregamos el riesgo crediticio ponderado a cada persona que realizo la transacción
df2 = dfTablon.join(
  dfRiesgoPonderado,
  dfTablon["ID_PERSONA"] == dfRiesgoPonderado["ID_CLIENTE"],
  "inner"
).select(
  dfTablon["ID_PERSONA"],
  dfTablon["NOMBRE_PERSONA"],
  dfTablon["EDAD_PERSONA"],
  dfTablon["SALARIO_PERSONA"],
  dfRiesgoPonderado["RIESGO_PONDERADO"],
  dfTablon["ID_EMPRESA_TRANSACCION"],
  dfTablon["NOMBRE_EMPRESA"],
  dfTablon["MONTO_TRANSACCION"],
  dfTablon["FECHA_TRANSACCION"]
)
 
#Mostramos los datos
df2.show()

# COMMAND ----------

# DBTITLE 1,10. Pre-Procesamiento
#Aplicamos las reglas de filtrado comúnes a los tres reportes
# - TRANSACCIONES MAYORES A 500 DÓLARES
# - REALIZADAS EN AMAZON
dfTablon1 = df2.filter(
  (df2["MONTO_TRANSACCION"] > 500) &
  (df2["NOMBRE_EMPRESA"] == "Amazon")
)
 
#Mostramos los datos
dfTablon1.show()

# COMMAND ----------

# DBTITLE 1,11. Procesamiento
#REPORTE 1:
# - POR PERSONAS ENTRE 30 A 39 AÑOS
# - CON UN SALARIO DE 1000 A 5000 DOLARES
dfReporte1 = dfTablon1.filter(
  (dfTablon1["EDAD_PERSONA"] >= 30) &
  (dfTablon1["EDAD_PERSONA"] <= 39) &
  (dfTablon1["SALARIO_PERSONA"] >= 1000) &
  (dfTablon1["SALARIO_PERSONA"] <= 5000)
)
 
#Mostramos los datos
dfReporte1.show()

# COMMAND ----------

#REPORTE 2:
# - POR PERSONAS ENTRE 40 A 49 AÑOS
# - CON UN SALARIO DE 2500 A 7000 DOLARES
dfReporte2 = dfTablon1.filter(
  (dfTablon1["EDAD_PERSONA"] >= 40) &
  (dfTablon1["EDAD_PERSONA"] <= 49) &
  (dfTablon1["SALARIO_PERSONA"] >= 2500) &
  (dfTablon1["SALARIO_PERSONA"] <= 7000)
)
 
#Mostramos los datos
dfReporte2.show()

# COMMAND ----------

#REPORTE 3:
# - POR PERSONAS ENTRE 50 A 60 AÑOS
# - CON UN SALARIO DE 3500 A 10000 DOLARES
dfReporte3 = dfTablon1.filter(
  (dfTablon1["EDAD_PERSONA"] >= 50) &
  (dfTablon1["EDAD_PERSONA"] <= 60) &
  (dfTablon1["SALARIO_PERSONA"] >= 3500) &
  (dfTablon1["SALARIO_PERSONA"] <= 10000)
)
 
#Mostramos los datos
dfReporte3.show()

# COMMAND ----------

# DBTITLE 1,12. Almacenamiento
#Almacenamos el REPORTE 1 en la capa "GOLD"
dfReporte1.write.format("delta").mode("overwrite").save("dbfs:///FileStore/_pyspark/EJERCICIO_4/gold/REPORTE_1/")

# COMMAND ----------

#Almacenamos el REPORTE 2 en la capa "GOLD"
dfReporte2.write.format("delta").mode("overwrite").save("dbfs:///FileStore/_pyspark/EJERCICIO_4/gold/REPORTE_2/")

# COMMAND ----------

#Almacenamos el REPORTE 3 en la capa "GOLD"
dfReporte3.write.format("delta").mode("overwrite").save("dbfs:///FileStore/_pyspark/EJERCICIO_4/gold/REPORTE_3/")
