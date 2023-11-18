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

# DBTITLE 1,3. Escritura del registro en un tópico de "EventHub"
#Importamos el utilitario para enviar datos a tópicos
from azure.eventhub import EventHubProducerClient

# COMMAND ----------

#Importamos el utilitario que encapsula el registro que queremos enviar al tópico
from azure.eventhub import EventData

# COMMAND ----------

#Colocamos la clave de acceso copiada desde la directiva "PRODUCER" en "AZURE"
claveDeAcceso = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

# COMMAND ----------

#Instanciamos la conexión al tópico "transaccion" con la clave de acceso
producer = EventHubProducerClient.from_connection_string(
    conn_str = claveDeAcceso, 
    eventhub_name = "transaccion"
)

# COMMAND ----------

#Usamos el producer para enviarle el registro al tópico
with producer:
    #Instanciamos un escritor
    writer = producer.create_batch()

    #Colocamos el registro listo para ser binarizado con el utilitario "EventData"
    writer.add(EventData(registroBin))

    #Enviamos el registro
    producer.send_batch(writer)

# COMMAND ----------

# DBTITLE 1,4. Función "writer" para envío de registros al tópico
#Creamos la función que envía con el producer el registro al tópico
def writer(producer, registroBin):
    #Usamos el producer para enviarle el registro al tópico
    with producer:
        #Instanciamos un escritor
        writer = producer.create_batch()

        #Colocamos el registro listo para ser binarizado con el utilitario "EventData"
        writer.add(EventData(registroBin))

        #Enviamos el registro
        producer.send_batch(writer)

# COMMAND ----------

#Ejecutamos la función
writer(producer, registroBin)

# COMMAND ----------

# DBTITLE 1,5. Bucle de ejecución de ingesta hacía el tópico
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
    writer(producer, registroBin)

    #Dormimos el bucle de ingesta por 1 segundo
    time.sleep(1)
