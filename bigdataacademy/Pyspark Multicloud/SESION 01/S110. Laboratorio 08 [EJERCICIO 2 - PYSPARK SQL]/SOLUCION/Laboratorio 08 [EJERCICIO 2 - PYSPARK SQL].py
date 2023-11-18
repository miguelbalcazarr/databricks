# Databricks notebook source
## #######################################################################################################
## 
## @copyright Big Data Academy [info@bigdataacademy.org]
## @professor Alonso Melgarejo [alonsoraulmgs@gmail.com]
## 
## #######################################################################################################

# COMMAND ----------

# DBTITLE 1,1. Librerías
#Objetos para definir la metadata
from pyspark.sql.types import StructType, StructField

#Importamos los tipos de datos que usaremos
from pyspark.sql.types import StringType, IntegerType, DoubleType

#Podemos importar todos los utilitarios con la siguiente sentencia
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,2. Lectura de dataframes
#Vamos a colocar el contenido de todos los archivos de la carpeta en un dataframe
#Solo deberemos indicar la ruta asociada
#Para este ejemplo, el archivo tiene un delimitador de coma "|"
dfTransaccion = spark.read.format("csv").option("header", "true").option("delimiter", "|").schema(
    StructType(
        [
            StructField("ID_PERSONA", StringType(), True),
            StructField("ID_EMPRESA", StringType(), True),
            StructField("MONTO", DoubleType(), True),
            StructField("FECHA", StringType(), True)
        ]
    )
).load("dbfs:///FileStore/_pyspark/transacciones.data")
 
#Vemos el contenido
dfTransaccion.show()

# COMMAND ----------

# DBTITLE 1,3. Registro de dataframes como TempViews
#Registramos los dataframes como TempViews
dfTransaccion.createOrReplaceTempView("dfTransaccion")

# COMMAND ----------

#Verificamos que las TempViews se hayan creado
spark.sql("SHOW VIEWS").show()

# COMMAND ----------

# DBTITLE 1,4. Procesamos con SQL con PySpark
#Creamos las variables que almacenan los parámetros
PARAM_MONTO = 1500
PARAM_ID_EMPRESA = 3

# COMMAND ----------

#Nos quedamos con las transaccion mayores a 1500 dólares realizadas en la empresa 3
dfResultado = spark.sql(f"""
SELECT
  T.*
FROM
  dftransaccion T
WHERE
  T.MONTO > {PARAM_MONTO} AND
  T.ID_EMPRESA > {PARAM_ID_EMPRESA}
""")
 
#Vemos el resultado
dfResultado.show()

# COMMAND ----------

#Guardamos el dataframe como TempView
dfResultado.createOrReplaceTempView("dfResultado")

# COMMAND ----------

# DBTITLE 1,5. Escritura de resultante final
#Escribimos el dataframe de la resultante final en disco duro
dfResultado.write.format("csv").mode("overwrite").option("header", "true").option("delimiter", "|").save("dbfs:///FileStore/_pyspark/output/ejercicio1")

# COMMAND ----------

#Verificamos la ruta para ver si el archivo se escribió

# COMMAND ----------

# MAGIC %fs ls dbfs:///FileStore/_pyspark/output/ejercicio1

# COMMAND ----------

# DBTITLE 1,6. Verificamos
#Para verificar, leemos el directorio del dataframe en una variable
#Como solo quiero verificar que esta escrito, puedo omitir la definición del esquema
dfResultadoLeido = spark.read.format("csv").option("header", "true").option("delimiter", "|").load("dbfs:///FileStore/_pyspark/output/ejercicio1")
 
#Mostramos los datos
dfResultadoLeido.show()
