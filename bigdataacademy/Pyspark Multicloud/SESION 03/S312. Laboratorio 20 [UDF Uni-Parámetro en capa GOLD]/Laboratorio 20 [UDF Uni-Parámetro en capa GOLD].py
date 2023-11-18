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

#Utilitario para crear un UDF
from pyspark.sql.functions import udf

# COMMAND ----------

# DBTITLE 1,2. Definición de función
#Funcion para calcular el salario anual
def calcularSalarioAnual(salarioMensual):
    #Calculo del salario anual
    salarioAnual = salarioMensual*12
    
    #Devolvemos el valor
    return salarioAnual

# COMMAND ----------

# DBTITLE 1,3. Creación de UDF
#Creamos la función personalizada
#Primer parámetro la función
#Segundo parámetro el tipo de dato que devuelve la función
udfCalcularSalarioAnual = udf(
    calcularSalarioAnual, 
    DoubleType()
)

# COMMAND ----------

# DBTITLE 1,4. Lectura de datos
#Lectura desde archivo de texto plano indicando el esquema de metadatos (función "schema")
dfPersona = spark.read.format("delta").load("dbfs:///FileStore/_pyspark/deltalake/silver/persona/")
 
#Mostramos los datos
dfPersona.show()

# COMMAND ----------

# DBTITLE 1,5. Aplicación del UDF [SELECT]
#Aplicamos la función
dfReporte1 = dfPersona.select(
    dfPersona["NOMBRE_PERSONA"],
    dfPersona["SALARIO"].alias("SALARIO_MENSUAL"),
    udfCalcularSalarioAnual(dfPersona["SALARIO"]).alias("SALARIO_ANUAL")
)
 
#Mostramos los datos
dfReporte1.show()

# COMMAND ----------

# DBTITLE 1,6. Aplicación del UDF [WITHCOLUMN]
#Otra forma de aplicar la funcion
#Esta forma potencialmente puede gastar memoria RAM
dfReporte1 = dfPersona.withColumn("SALARIO_ANUAL", udfCalcularSalarioAnual(dfPersona["SALARIO"]))
 
#Mostramos los datos
dfReporte1.show()

# COMMAND ----------

# DBTITLE 1,7. Almacenamiento en capa "GOLD"
#Almacenamiento binarizado
dfReporte1.write.format("delta").mode("overwrite").save("dbfs:///FileStore/_pyspark/deltalake/gold/reporte_1/")
