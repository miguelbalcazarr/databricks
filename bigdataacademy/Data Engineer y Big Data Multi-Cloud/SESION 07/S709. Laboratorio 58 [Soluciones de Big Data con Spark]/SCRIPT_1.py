#Importamos el objeto para instanciar una sesion
from pyspark.sql import SparkSession

#Importamos la librería de funciones clasicas
import pyspark.sql.functions as f

#Instanciamos una sesion
#Dependiendo de la potencia del cluster, tomara hasta 3 minutos en asignarle recursos
spark = SparkSession.builder.getOrCreate()

#Leemos los datos que procesaremos
#Consultamos al directorio y subdirectorios
dfPersona = spark.read.format("parquet").load("gs://storagebdaXXX/silver/persona/*/*.parquet")

#Procesamos
dfReporte = dfPersona.filter(dfPersona["EDAD"] > 60)

#Guardamos el resultado en el directorio final
dfReporte.write.mode("overwrite").format("parquet").save("gs://storagebdaXXX/gold/reporte")
