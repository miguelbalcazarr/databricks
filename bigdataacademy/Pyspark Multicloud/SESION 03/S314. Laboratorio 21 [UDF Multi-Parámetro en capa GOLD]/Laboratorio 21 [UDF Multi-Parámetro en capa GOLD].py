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

# COMMAND ----------

#Utilitario para enviarle varios parámetros a un UDF
from pyspark.sql.functions import struct

# COMMAND ----------

# DBTITLE 1,2. Definición de función
#Función para calcular el nuevo salario
def calcularNuevoSalario(salario, edad):
  resultado = 0
  
  #Tiene mas de 30 años
  #Tiene menos de 50 años
  if (edad > 30) & (edad < 50):
    resultado = salario * 2
  else:
    resultado = salario
  
  return resultado

# COMMAND ----------

# DBTITLE 1,3. Creación de UDF
#Creamos la función personalizada
#Primer parámetro la función
#Segundo parámetro el tipo de dato que devuelve la función
udfCalcularNuevoSalario = udf(
    (
        lambda parametros : calcularNuevoSalario(
            parametros[0], 
            parametros[1]
        )
    ),
    DoubleType()
)

# COMMAND ----------

# DBTITLE 1,4. Lectura de datos
#Lectura desde archivo de texto plano indicando el esquema de metadatos (función "schema")
dfPersona = spark.read.format("delta").load("dbfs:///FileStore/_pyspark/deltalake/silver/persona/")
 
#Mostramos los datos
dfPersona.show()

# COMMAND ----------

# DBTITLE 1,6. Aplicación del UDF [WITHCOLUMN]
#Aplicamos la función
dfReporte2 = dfPersona.select(
    dfPersona["NOMBRE_PERSONA"],
    dfPersona["SALARIO"].alias("SALARIO_MENSUAL"),
    udfCalcularNuevoSalario(
      struct(
        dfPersona["SALARIO"],
        dfPersona["EDAD"]
      )
    ).alias("NUEVO_SALARIO")
)
 
#Mostramos los datos
dfReporte2.show()

# COMMAND ----------

# DBTITLE 1,7. Aplicación del UDF [SQL]
#Para usar un UDF dentro de SPARK SQL, primero debemos registrarlo
spark.udf.register("udfCalcularNuevoSalario", udfCalcularNuevoSalario)

# COMMAND ----------

#Registramos el dataframe como vista temporal
dfPersona.createOrReplaceTempView("dfPersona")

# COMMAND ----------

#Procesémoslo con SPARK SQL haciendo uso de nuestro UDF
dfReporte2 = spark.sql("""
SELECT
  ID_PERSONA,
  NOMBRE_PERSONA,
  udfCalcularNuevoSalario(STRUCT(SALARIO, EDAD)) NUEVO_SALARIO
FROM
  dfPersona
""")
 
#Mostramos los datos
dfReporte2.show()

# COMMAND ----------

# DBTITLE 1,8. Almacenamiento en capa "GOLD"
#Almacenamiento binarizado
dfReporte2.write.format("delta").mode("overwrite").save("dbfs:///FileStore/_pyspark/deltalake/gold/reporte_2/")
