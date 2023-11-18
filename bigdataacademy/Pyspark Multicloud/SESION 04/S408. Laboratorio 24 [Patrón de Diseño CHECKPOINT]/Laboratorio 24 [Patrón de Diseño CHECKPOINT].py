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

# COMMAND ----------

# DBTITLE 1,5. Desbordamiento de memoria RAM
#SUPONGAMOS QUE AL EJECUTAR ESTE PASO OBTENEMOS EL ERROR DE DESBORDAMIENTO
#APLICAREMOS UN CHECKPOINT

#PASO 2 (<<T_P_E>>): AGREGAR LOS DATOS DE LAS EMPRESAS EN DONDE SE REALIZARON LAS TRANSACCIONES
#dfTablon = df1.join(
#  dfEmpresaLimpio,
#  df1["ID_EMPRESA_TRANSACCION"] == dfEmpresaLimpio["ID"],
#  "inner"
#).select(
#  df1["ID_PERSONA"],
#  df1["NOMBRE_PERSONA"],
#  df1["EDAD_PERSONA"],
#  df1["SALARIO_PERSONA"],
#  df1["ID_EMPRESA_PERSONA"],
#  df1["ID_EMPRESA_TRANSACCION"],
#  dfEmpresaLimpio["NOMBRE"].alias("NOMBRE_EMPRESA"),
#  df1["MONTO_TRANSACCION"],
#  df1["FECHA_TRANSACCION"]
#)

# COMMAND ----------

#Definiremos de manera aleatoria el nombre de la carpeta en donde se hará el checkpoint
#Importamos el utilitario que genera números aleatorios
import random

# COMMAND ----------

#Generamos el nombre de la carpeta
directorio = random.random()

#Lo imprimimos
print(directorio)

# COMMAND ----------

#Definimos la ruta en donde almacenaremos el dataframe en disco duro
ruta = "dbfs:///FileStore/tmp/"+str(directorio)

#Lo imprimimos
print(ruta)

# COMMAND ----------

#Almacenamos el dataframe en disco duro para forzar la ejecución de la cadena de procesos que crea el df1
df1.write.mode("overwrite").format("parquet").save(ruta)

# COMMAND ----------

#Forzamos el llamado del Garbage Collector para eliminar toda la cadena de procesos asociado al "df1"
#La memoria RAM se liberó
df1.unpersist(blocking = True)

# COMMAND ----------

#Leemos el archivo desde el directorio de checkpoint
df1 = spark.read.format("parquet").load(ruta)

# COMMAND ----------

#LA MEMORIA RAM ESTÁ VACÍA, PODEMOS SEGUIR CON LOS DEMÁS PASOS DE PROCESAMIENTO

# COMMAND ----------

# DBTITLE 1,5. Función utilitaria para aplicar un CHECKPOINT
#Función que encapsula la lógica de creación de un CHECKPOINT
def checkpoint(df):
    #Generamos el nombre de la carpeta
    directorio = random.random()

    #Definimos la ruta en donde almacenaremos el dataframe en disco duro
    ruta = "dbfs:///FileStore/tmp/"+str(directorio)

    #Almacenamos el dataframe en disco duro para forzar la ejecución de la cadena de procesos que crea el df1
    df.write.mode("overwrite").format("parquet").save(ruta)

    #Forzamos el llamado del Garbage Collector para eliminar toda la cadena de procesos asociado al "df1"
    #La memoria RAM se liberó
    df.unpersist(blocking = True)

    #Leemos el archivo desde el directorio de checkpoint
    dfCheckpoint = spark.read.format("parquet").load(ruta)

    #Retornamos el dataframe leído
    return dfCheckpoint

# COMMAND ----------

#CUANDO NECESESITAMOS APLICAR UN PUNTO DE CHECKPOIN, SIMPLEMENTE LLAMAMOS A LA FUNCIÓN
df1 = checkpoint(df1)
