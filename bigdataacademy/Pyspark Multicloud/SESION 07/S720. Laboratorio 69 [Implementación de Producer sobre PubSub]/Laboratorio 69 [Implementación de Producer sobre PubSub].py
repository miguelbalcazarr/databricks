# Databricks notebook source
## #######################################################################################################
## 
## @copyright Big Data Academy [info@bigdataacademy.org]
## @professor Alonso Melgarejo [alonsoraulmgs@gmail.com]
## 
## #######################################################################################################

# COMMAND ----------

# DBTITLE 1,1. Función "sourceClient" para lectura de datos en real-time
#Librería para consultar APIs web
import requests
 
#Librería de manipulación de JSON
import json
 
#Creamos la función que automatiza la lectura desde la fuente de datos
def sourceClient():
    #Nos conectamos a un API y obtenemos la respuesta
    response = requests.get("http://api.open-notify.org/iss-now.json")
 
    #Colocamos la respuesta en un objeto JSON
    registro = json.loads(response.content)
 
    #Retornamos el registro leído
    return registro

# COMMAND ----------

#Ejecutamos la función
registro = sourceClient()

#Verificamos
print(registro)

# COMMAND ----------

# DBTITLE 1,2. Función "binary" para binarización
#Creamos la función que deja el registro listo para la binarización
def binary(registro):
    #Convertimos la variable a "str"
    registroBin = json.dumps(registro)

    #Retornamos el registro listo para la binarización
    return registroBin

# COMMAND ----------

#Ejecutamos la función
registroBin = binary(registro)

#Verificamos
type(registroBin)

# COMMAND ----------

# DBTITLE 1,3. Colocación del archivo de credenciales en el sistema de archivos local de Linux
#En el sistema de archivos local de Linux, creamos el directorio en donde colocaremos el archivo de credenciales

# COMMAND ----------

# MAGIC %sh mkdir /credenciales

# COMMAND ----------

#Desde el sistema de archivos DBFS copiamos el archivo de credenciales al sistema de archivos local de LINUX

# COMMAND ----------

# MAGIC %fs cp dbfs:///FileStore/_pyspark/credenciales.json file:///credenciales

# COMMAND ----------

#Verificamos el archivo de credenciales en el sistema de archivos local de LINUX

# COMMAND ----------

# MAGIC %sh ls /credenciales

# COMMAND ----------

# DBTITLE 1,4. Configuración del archivo de credenciales en las variables de entorno
#Creamos la variable de sistema "GOOGLE_APPLICATION_CREDENTIALS" en LINUX con la ruta del archivo de credenciales

# COMMAND ----------

# MAGIC %sh export GOOGLE_APPLICATION_CREDENTIALS="/credenciales/credenciales.json"

# COMMAND ----------

#Crearemos también la variable de configuración sobre PYTHON
#Importamos la librería para manipulación del sistema operativo
import os

# COMMAND ----------

#Creamos la variable del sistema con la ruta del archivo de credenciales
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/credenciales/credenciales.json"

# COMMAND ----------

# DBTITLE 1,5. Configuración del número de proyecto GCP
#Colocamos el número del proyecto
numeroDeProyecto = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

# COMMAND ----------

# DBTITLE 1,6. Escritura del registro en un tópico de "EventHub"
#Importamos el utilitario para enviar datos a tópicos
from google.cloud.pubsublite.cloudpubsub import PublisherClient

# COMMAND ----------

#Utilitario para conectarnos a la REGIÓN de GCP en donde creamos el tópico
from google.cloud.pubsublite.types import CloudRegion

# COMMAND ----------

#Utilitario para conectarnos a la ZONA de GCP en donde creamos el tópico
from google.cloud.pubsublite.types import CloudZone

# COMMAND ----------

#Utilitario para seleccionar el tópico en donde se escribiran los registros
from google.cloud.pubsublite.types import TopicPath

# COMMAND ----------

#Realizamos la conexión a la REGIÓN y la ZONA
conexion = CloudZone(CloudRegion("us-central1"), "a")

# COMMAND ----------

#Realizamos la conexión al TÓPICO
topico = TopicPath(numeroDeProyecto, conexion, "transaccion")

# COMMAND ----------

#Con el utilitario "PublisherClient" realizamos la escritura
with PublisherClient() as publicador:
    
    #Preparamos la escritura
    mensaje = publicador.publish(topico, registroBin.encode("utf-8"))

    #Realizamos la escritura
    mensaje.result()

# COMMAND ----------

# DBTITLE 1,7. Función "writer" para envío de registros al tópico
#Creamos la función que envía con el producer el registro al tópico
def writer(topico, registroBin):
    #Con el utilitario "PublisherClient" realizamos la escritura
    with PublisherClient() as publicador:
        
        #Preparamos la escritura
        mensaje = publicador.publish(topico, registroBin.encode("utf-8"))

        #Realizamos la escritura
        mensaje.result()

# COMMAND ----------

#Ejecutamos la función
writer(topico, registroBin)

# COMMAND ----------

# DBTITLE 1,8. Bucle de ejecución de ingesta hacía el tópico
#Haremos un bucle de ingesta
#Necesitamos pausar la ingesta cada 1 segundo para no saturar el servidor de la fuente de datos
#Importamos el utilitario que nos permite pausar la ejecución del bucle
import time

# COMMAND ----------

#Haremos 10 inserciones de prueba hacía el tópico
for i in range(1, 20):
    #Mostramos el número de la iteración
    print("Iteración: "+str(i))

    #Leemos los datos desde la fuente de datos
    registro = sourceClient()

    #Dejamos el registro listo para la binarización
    registroBin = binary(registro)

    #Enviamos el registro al tópico
    writer(topico, registroBin)

    #Dormimos el bucle de ingesta por 1 segundo
    time.sleep(1)
