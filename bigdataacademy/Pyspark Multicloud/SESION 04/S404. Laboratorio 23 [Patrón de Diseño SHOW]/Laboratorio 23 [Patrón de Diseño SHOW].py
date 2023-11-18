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

# DBTITLE 1,2. Patrón de diseño SHOW
#Parámetro que activa y desactiva el "show"
PARAM_SHOW_ACTIVO = True

# COMMAND ----------

#Parámetro que activa y desactiva el "show"
PARAM_SHOW_ACTIVO = False

#FUNCIÓN
def show(df):
    if(PARAM_SHOW_ACTIVO == True):
        #Ejecutamos el action "show"
        df.show()

        #Lo marcamos en la caché
        df.cache()

# COMMAND ----------

#Recibe el dataframe sobre el cuál se ejecuta el action "show"
def show(df):
    #Si el parámetro está activado
    if(PARAM_SHOW_ACTIVO == True):
        #Ejecutamos el action "show"
        df.show()

        #Lo marcamos en la caché
        df.cache()

# COMMAND ----------

# DBTITLE 1,3. Ejemplo de uso cuando estamos desarrollando el proceso
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
).load("dbfs:///FileStore/_pyspark/persona.data")

# COMMAND ----------

#Mostramos los datos con la función "show"
show(dfPersona)

# COMMAND ----------

#Definimos un "transformation"
df1 = dfPersona.filter(dfPersona["EDAD"] > 20)

# COMMAND ----------

#Ejecutamos el action "show"
show(df1)

# COMMAND ----------

#Almacenamos el resultado
df1.write.format("parquet").option("compression", "snappy").mode("overwrite").save("dbfs:///FileStore/_pyspark/df1")

# COMMAND ----------

# DBTITLE 1,4. Ejemplo de uso cuando el script ya está en producción
#Desactivamos el "show"
PARAM_SHOW_ACTIVO = False

# COMMAND ----------

#SI EJECUTAMOS EL MISMO PROCESO, NINGÚN "SHOW" SE EJECUTARÁ
#SOLO DEFINIMOS LA CADENA DE PROCESOS

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
).load("dbfs:///FileStore/_pyspark/persona.data")

#Mostramos los datos con la función "show"
show(dfPersona)

#Definimos un "transformation"
df1 = dfPersona.filter(dfPersona["EDAD"] > 20)

#Ejecutamos el action "show"
show(df1)

# COMMAND ----------

#SÓLO SE EJECUTA EL ACTION "write" Y EVITAMOS EL PROBLEMA DEL GARBAGE COLLECTOR

#Almacenamos el resultado
df1.write.format("parquet").option("compression", "snappy").mode("overwrite").save("dbfs:///FileStore/_pyspark/df1")
