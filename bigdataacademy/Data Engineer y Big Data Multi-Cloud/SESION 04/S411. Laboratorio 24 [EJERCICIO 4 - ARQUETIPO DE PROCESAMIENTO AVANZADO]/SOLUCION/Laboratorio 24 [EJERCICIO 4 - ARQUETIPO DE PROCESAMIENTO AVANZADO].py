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

#Utilitario para crear un UDF
from pyspark.sql.functions import udf

#Utilitario para enviarle varios parámetros a un UDF
from pyspark.sql.functions import struct

#Colocamos todos los utilitarios del paquete de librerías "pyspark.sql.functions" dentro de la variable "f"
import pyspark.sql.functions as f

# COMMAND ----------

# DBTITLE 1,2. Lectura de datos
#Leemos los datos de transacciones
dfTransacciones = spark.read.format("json").load("dbfs:///FileStore/_bigdata/transacciones_bancarias.json")

#Vemos el esquema
dfTransacciones.printSchema()

#Mostramos los datos
dfTransacciones.show()

# COMMAND ----------

#Leemos el archivo de riesgo crediticio
dfRiesgo = spark.read.format("csv").option("header", "true").option("delimiter", ",").schema(
    StructType(
        [
          StructField("ID_CLIENTE", StringType(), True),
          StructField("RIESGO_CENTRAL_1", DoubleType(), True),
          StructField("RIESGO_CENTRAL_2", DoubleType(), True),
          StructField("RIESGO_CENTRAL_3", DoubleType(), True)
        ]
    )
).load("dbfs:///FileStore/_bigdata/RIESGO_CREDITICIO.csv")

#Vemos el esquema
dfRiesgo.printSchema()

#Mostramos los datos
dfRiesgo.show()

# COMMAND ----------

# DBTITLE 1,3. Modelamiento
#Estructuramos dfTransaccion

#Seleccionamos los campos
dfTransaccion = dfTransacciones.select(
  dfTransacciones["PERSONA.ID_PERSONA"].alias("ID_PERSONA"),
  dfTransacciones["EMPRESA.ID_EMPRESA"].alias("ID_EMPRESA"),
  dfTransacciones["TRANSACCION.MONTO"].alias("MONTO"),
  dfTransacciones["TRANSACCION.FECHA"].alias("FECHA")
)

#Vemos el esquema
dfTransaccion.printSchema()

#Contamos los registros
print(dfTransaccion.count())

#Mostramos los datos
dfTransaccion.show()

# COMMAND ----------

#Estructuramos dfPersona

#Seleccionamos los campos
dfPersona = dfTransacciones.select(
  dfTransacciones["PERSONA.ID_PERSONA"].alias("ID_PERSONA"),
  dfTransacciones["PERSONA.NOMBRE_PERSONA"].alias("NOMBRE_PERSONA"),
  dfTransacciones["PERSONA.EDAD"].alias("EDAD"),
  dfTransacciones["PERSONA.SALARIO"].alias("SALARIO")
).distinct()

#Vemos el esquema
dfPersona.printSchema()

#Contamos los registros
print(dfPersona.count())

#Mostramos los datos
dfPersona.show()

# COMMAND ----------

#Estructuramos dfEmpresa

#Seleccionamos los campos
dfEmpresa = dfTransacciones.select(
  dfTransacciones["EMPRESA.ID_EMPRESA"].alias("ID_EMPRESA"),
  dfTransacciones["EMPRESA.NOMBRE_EMPRESA"].alias("NOMBRE_EMPRESA")
).distinct()

#Vemos el esquema
dfEmpresa.printSchema()

#Contamos los registros
print(dfEmpresa.count())

#Mostramos los datos
dfEmpresa.show()

# COMMAND ----------

# DBTITLE 1,4. Reglas de calidad
#Aplicamos las reglas de calidad a dfPersona
dfPersonaLimpio = dfPersona.filter(
  (dfPersona["ID_PERSONA"].isNotNull()) &
  (dfPersona["SALARIO"] >= 0) &
  (dfPersona["SALARIO"] < 100000) &
  (dfPersona["EDAD"] > 0) &
  (dfPersona["EDAD"] < 60)
)

#Vemos el esquema
dfPersonaLimpio.printSchema()

#Mostramos los datos
dfPersonaLimpio.show()

# COMMAND ----------

#Aplicamos las reglas de calidad a dfEmpresa
dfEmpresaLimpio = dfEmpresa.filter(
  (dfEmpresa["ID_EMPRESA"].isNotNull())
)

#Vemos el esquema
dfEmpresaLimpio.printSchema()

#Mostramos los datos
dfEmpresaLimpio.show()

# COMMAND ----------

#Aplicamos las reglas de calidad a dfTransaccion
dfTransaccionLimpio = dfTransaccion.filter(
  (dfTransaccion["ID_PERSONA"].isNotNull()) &
  (dfTransaccion["ID_EMPRESA"].isNotNull()) &
  (dfTransaccion["MONTO"] > 0) &
  (dfTransaccion["MONTO"] < 100000)
)

#Vemos el esquema
dfTransaccionLimpio.printSchema()

#Mostramos los datos
dfTransaccionLimpio.show()

# COMMAND ----------

#Aplicamos las reglas de calidad a dfRiesgo
dfRiesgoLimpio = dfRiesgo.filter(
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

# DBTITLE 1,5. Creación de UDF
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

# DBTITLE 1,6. Preparación de tablones
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

# DBTITLE 1,7. Pre-Procesamiento
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

# DBTITLE 1,8. Procesamiento
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

# DBTITLE 1,9. Almacenamiento
#Almacenamos el REPORTE 1
dfReporte1.write.format("csv").option("header", "true").option("delimiter", "|").option("encoding", "ISO-8859-1").mode("overwrite").save("dbfs:///FileStore/_bigdata/output/EJERCICIO_3/REPORTE_1")

# COMMAND ----------

#Leemos
dfReporteLeido1 = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("encoding", "ISO-8859-1").load("dbfs:///FileStore/_bigdata/output/EJERCICIO_3/REPORTE_1")

#Verificamos
dfReporteLeido1.show()

# COMMAND ----------

#Almacenamos el REPORTE 2
dfReporte2.write.format("csv").option("header", "true").option("delimiter", "|").option("encoding", "ISO-8859-1").mode("overwrite").save("dbfs:///FileStore/_bigdata/output/EJERCICIO_3/REPORTE_2")

# COMMAND ----------

#Leemos
dfReporteLeido2 = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("encoding", "ISO-8859-1").load("dbfs:///FileStore/_bigdata/output/EJERCICIO_3/REPORTE_2")

#Verificamos
dfReporteLeido2.show()

# COMMAND ----------

#Almacenamos el REPORTE 3
dfReporte3.write.format("csv").option("header", "true").option("delimiter", "|").option("encoding", "ISO-8859-1").mode("overwrite").save("dbfs:///FileStore/_bigdata/output/EJERCICIO_3/REPORTE_3")

# COMMAND ----------

#Leemos
dfReporteLeido3 = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("encoding", "ISO-8859-1").load("dbfs:///FileStore/_bigdata/output/EJERCICIO_3/REPORTE_3")

#Verificamos
dfReporteLeido3.show()
