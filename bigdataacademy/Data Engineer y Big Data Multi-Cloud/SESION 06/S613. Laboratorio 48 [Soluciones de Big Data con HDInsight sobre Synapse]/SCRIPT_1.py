#IMPORTANTE, EN EL BLOB STORAGE CAMBIAR "XXX" POR LAS INICIALES DE TU NOMBRE

#Leemos el archivo
dfData = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferSchema", "true").load("abfss://deltalake@storagebdadataXXX.dfs.core.windows.net/bronze/persona/persona.data")