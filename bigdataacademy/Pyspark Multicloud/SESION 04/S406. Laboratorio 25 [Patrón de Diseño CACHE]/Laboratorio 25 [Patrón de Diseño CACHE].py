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
).load("dbfs:///FileStore/_pyspark/DATA_PERSONA.txt")
 
#Leemos el archivo indicando el esquema
dfEmpresa = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("encoding", "ISO-8859-1").schema(
    StructType(
        [
            StructField("ID", StringType(), True),
            StructField("NOMBRE", StringType(), True)
        ]
    )
).load("dbfs:///FileStore/_pyspark/DATA_EMPRESA.txt")

#Leemos el archivo indicando el esquema
dfTransaccion = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("encoding", "ISO-8859-1").schema(
    StructType(
        [
            StructField("ID_PERSONA", StringType(), True),
            StructField("ID_EMPRESA", StringType(), True),
            StructField("MONTO", DoubleType(), True),
            StructField("FECHA", StringType(), True)
        ]
    )
).load("dbfs:///FileStore/_pyspark/DATA_TRANSACCION.txt")

# COMMAND ----------

# DBTITLE 1,3. Reglas de calidad
#Aplicamos las reglas de calidad a dfPersona
dfPersonaLimpio = dfPersona.filter(
  (dfPersona["ID"].isNotNull()) &
  (dfPersona["ID_EMPRESA"].isNotNull()) &
  (dfPersona["SALARIO"] >= 0) &
  (dfPersona["SALARIO"] < 100000) &
  (dfPersona["EDAD"] > 0) &
  (dfPersona["EDAD"] < 60)
)

#Aplicamos las reglas de calidad a dfEmpresa
dfEmpresaLimpio = dfEmpresa.filter(
  (dfEmpresa["ID"].isNotNull())
)

#Aplicamos las reglas de calidad a dfTransaccion
dfTransaccionLimpio = dfTransaccion.filter(
  (dfTransaccion["ID_PERSONA"].isNotNull()) &
  (dfTransaccion["ID_EMPRESA"].isNotNull()) &
  (dfTransaccion["MONTO"] > 0) &
  (dfTransaccion["MONTO"] < 100000)
)

# COMMAND ----------

# DBTITLE 1,4. Preparación de tablones
#PASO 1 (<<T_P>>): AGREGAR LOS DATOS DE LAS PERSONAS QUE REALIZARON LAS TRANSACCIONES
df1 = dfTransaccionLimpio.join(
  dfPersonaLimpio,
  dfTransaccionLimpio["ID_PERSONA"] == dfPersonaLimpio["ID"],
  "inner"
).select(
  dfTransaccionLimpio["ID_PERSONA"],
  dfPersonaLimpio["NOMBRE"].alias("NOMBRE_PERSONA"),
  dfPersonaLimpio["EDAD"].alias("EDAD_PERSONA"),
  dfPersonaLimpio["SALARIO"].alias("SALARIO_PERSONA"),
  dfPersonaLimpio["ID_EMPRESA"].alias("ID_EMPRESA_PERSONA"),
  dfTransaccionLimpio["ID_EMPRESA"].alias("ID_EMPRESA_TRANSACCION"),
  dfTransaccionLimpio["MONTO"].alias("MONTO_TRANSACCION"),
  dfTransaccionLimpio["FECHA"].alias("FECHA_TRANSACCION")
)

#PASO 2 (<<T_P_E>>): AGREGAR LOS DATOS DE LAS EMPRESAS EN DONDE SE REALIZARON LAS TRANSACCIONES
dfTablon = df1.join(
  dfEmpresaLimpio,
  df1["ID_EMPRESA_TRANSACCION"] == dfEmpresaLimpio["ID"],
  "inner"
).select(
  df1["ID_PERSONA"],
  df1["NOMBRE_PERSONA"],
  df1["EDAD_PERSONA"],
  df1["SALARIO_PERSONA"],
  df1["ID_EMPRESA_PERSONA"],
  df1["ID_EMPRESA_TRANSACCION"],
  dfEmpresaLimpio["NOMBRE"].alias("NOMBRE_EMPRESA"),
  df1["MONTO_TRANSACCION"],
  df1["FECHA_TRANSACCION"]
)

# COMMAND ----------

# DBTITLE 1,5. Pre-Procesamiento
#Aplicamos las reglas de filtrado comúnes a los tres reportes
# - TRANSACCIONES MAYORES A 500 DÓLARES
# - REALIZADAS EN AMAZON
dfTablon1 = dfTablon.filter(
  (dfTablon["MONTO_TRANSACCION"] > 500) &
  (dfTablon["NOMBRE_EMPRESA"] == "Amazon")
)

# COMMAND ----------

# DBTITLE 1,6. ALMACENAMIENTO EN LA CACHÉ
#EL DATAFRAME "dfTablon1" SERÁ USADO PARA CALCULAR 3 DATAFRAMES
#LO MARCAMOS EN LA CACHÉ
dfTablon1.cache()

# COMMAND ----------

# DBTITLE 1,7. Procesamiento
#REPORTE 1:
# - POR PERSONAS ENTRE 30 A 39 AÑOS
# - CON UN SALARIO DE 1000 A 5000 DOLARES
dfReporte1 = dfTablon1.filter(
  (dfTablon1["EDAD_PERSONA"] >= 30) &
  (dfTablon1["EDAD_PERSONA"] <= 39) &
  (dfTablon1["SALARIO_PERSONA"] >= 1000) &
  (dfTablon1["SALARIO_PERSONA"] <= 5000)
)

#Almacenamos el REPORTE 1
dfReporte1.write.format("csv").option("header", "true").option("delimiter", "|").option("encoding", "ISO-8859-1").mode("overwrite").save("dbfs:///FileStore/_pyspark/output/EJERCICIO_3/REPORTE_1")

#REPORTE 2:
# - POR PERSONAS ENTRE 40 A 49 AÑOS
# - CON UN SALARIO DE 2500 A 7000 DOLARES
dfReporte2 = dfTablon1.filter(
  (dfTablon1["EDAD_PERSONA"] >= 40) &
  (dfTablon1["EDAD_PERSONA"] <= 49) &
  (dfTablon1["SALARIO_PERSONA"] >= 2500) &
  (dfTablon1["SALARIO_PERSONA"] <= 7000)
)

#Almacenamos el REPORTE 2
dfReporte2.write.format("csv").option("header", "true").option("delimiter", "|").option("encoding", "ISO-8859-1").mode("overwrite").save("dbfs:///FileStore/_pyspark/output/EJERCICIO_3/REPORTE_2")

#REPORTE 3:
# - POR PERSONAS ENTRE 50 A 60 AÑOS
# - CON UN SALARIO DE 3500 A 10000 DOLARES
dfReporte3 = dfTablon1.filter(
  (dfTablon1["EDAD_PERSONA"] >= 50) &
  (dfTablon1["EDAD_PERSONA"] <= 60) &
  (dfTablon1["SALARIO_PERSONA"] >= 3500) &
  (dfTablon1["SALARIO_PERSONA"] <= 10000)
)

#Almacenamos el REPORTE 3
dfReporte3.write.format("csv").option("header", "true").option("delimiter", "|").option("encoding", "ISO-8859-1").mode("overwrite").save("dbfs:///FileStore/_pyspark/output/EJERCICIO_3/REPORTE_3")
