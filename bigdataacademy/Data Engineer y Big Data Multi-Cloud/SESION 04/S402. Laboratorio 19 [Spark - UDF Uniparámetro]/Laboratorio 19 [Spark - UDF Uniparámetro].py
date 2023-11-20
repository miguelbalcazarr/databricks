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
).load("dbfs:///FileStore/_bigdata/persona.data")

#Mostramos los datos
dfPersona.show()

# COMMAND ----------

# DBTITLE 1,5. Aplicación del UDF [SELECT]
#Aplicamos la función
df1 = dfPersona.select(
	dfPersona["NOMBRE"],
	dfPersona["SALARIO"].alias("SALARIO_MENSUAL"),
	udfCalcularSalarioAnual(dfPersona["SALARIO"]).alias("SALARIO_ANUAL")
)

#Mostramos los datos
df1.show()

# COMMAND ----------

# DBTITLE 1,6. Aplicación del UDF [WITHCOLUMN]
#Otra forma de aplicar la funcion
#Esta forma potencialmente puede gastar memoria RAM
df2 = dfPersona.withColumn("SALARIO_ANUAL", udfCalcularSalarioAnual(dfPersona["SALARIO"]))

#Mostramos los datos
df2.show()
