# Databricks notebook source
# DBTITLE 1,1. Librerías
#Utilitarios para modificar el esquema de metadatos
from pyspark.sql.types import StructType, StructField

#Importamos los tipos de datos que definiremos para cada campo
from pyspark.sql.types import StringType, IntegerType, DoubleType

# COMMAND ----------

# DBTITLE 1,2. Lectura de archivos
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

# DBTITLE 1,3. Librería pandas clásica
#Importamos la librería pandas clásica que no es compatible con entornos de clúster de Big Data
import pandas as pd

# COMMAND ----------

# DBTITLE 1,4. Librería pandas compatible con clústers de Big Data
#Importamos la librería de pandas compatible con entornos de clúster de Big Data
import pyspark.pandas as pd

# COMMAND ----------

#Por defecto un dataframe Pandas muestra 1000 registros
#Vamos a indicarle que sólo muestre 20 para que no se sature el notebook
pd.set_option('display.max_rows', 20)

# COMMAND ----------

# DBTITLE 1,5. Conversión de un Dataframe SPARK a un Dataframe PANDAS
#Convertimos el dataframe SPARK a un dataframe PANDAS
dfpPersona = pd.from_pandas(dfPersona.toPandas())

# COMMAND ----------

#Vemos los datos
dfpPersona

# COMMAND ----------

# DBTITLE 1,6. Conversión de un dataframe PANDAS a un dataframe SPARK
#Convertimos el dataframe PANDAS a un dataframe SPARK
dfPersona = dfpPersona.to_spark()

# COMMAND ----------

#Verificamos
dfPersona.show()
