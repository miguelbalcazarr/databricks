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

# DBTITLE 1,3. Escritura del registro en un tópico de "Kinesis"
#Utilitario para manipulación de servicios de AWS
import boto3

# COMMAND ----------

#Definimos el ACCESS KEY ID desde el archivo de credenciales
awsAccessKeyId = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

# COMMAND ----------

#Definimos el SECRET ACCESS KEY desde el archivo de credenciales
awsSecretAccessKey = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

# COMMAND ----------

#Iniciamos sesión en la región en donde desplegamos el tópico
sesion = boto3.Session(
    aws_access_key_id = awsAccessKeyId,
    aws_secret_access_key = awsSecretAccessKey,
    region_name = "us-east-1"
)

# COMMAND ----------

#Obtenemos el cliente de manipulación de kinesis
kinesis = sesion.client("kinesis")

# COMMAND ----------

#Verificamos listando todos los "streams" disponibles
kinesis.list_streams()

# COMMAND ----------

#Desde el sub-campo "StreamNames" visualizamos solo los nombres de los streams
kinesis.list_streams()["StreamNames"]

# COMMAND ----------

#Enviamos los datos
#Por ahora le pondremos cualquier valor a "PartitionKey"
kinesis.put_record(
    StreamName = "transaccion",
    Data = registroBin,
    PartitionKey = "1"
)

# COMMAND ----------

# DBTITLE 1,6. Función "writer" para envío de registros al tópico
#Creamos la función que envía con el producer el registro al tópico
def writer(registroBin, patitionKey):

    #Enviamos los datos
    kinesis.put_record(
        StreamName = "transaccion",
        Data = registroBin,
        PartitionKey = patitionKey
    )

# COMMAND ----------

#Ejecutamos la función
writer(registroBin, "1")

# COMMAND ----------

# DBTITLE 1,4. Función para generar la "PartitionKey"
#Importamos la librería que manipula la fecha y hora
from datetime import datetime

# COMMAND ----------

#Obtenemos la fecha y hora actual
fecha = datetime.today()

#Verificamos
fecha

# COMMAND ----------

# Formateamos la fecha y hora
# Extraemos:
#
# - El año: %Y
# - El mes: %m
# - El día: %d
# - La hora: %H
# - El minuto: %M
# - El segundo: %S
# - El microsegundo: %f
fechaStr = fecha.strftime("%Y%m%d%H%M%S%f")

#Verificamos
fechaStr

# COMMAND ----------

#Importamos la librería que genera números aleatorios
import random

# COMMAND ----------

#Generamos un número aleatorio de 8 dígitos (entre 10000000 y 99999999)
aleatorio = random.randint(10000000, 99999999)

#Verificamos
aleatorio

# COMMAND ----------

#Concatenamos ambas variables para tener el id
id = fechaStr + str(aleatorio)

#Verificamos
id

# COMMAND ----------

#Encapsulamos la lógica en una función
def generarPartitionKey():
  #Obtenemos la fecha y hora actual
  fecha = datetime.today()

  #Formateamos la fecha y hora
  fechaStr = fecha.strftime("%Y%m%d%H%M%S%f")

  #Generamos un número aleatorio de 8 dígitos (entre 10000000 y 99999999)
  aleatorio = random.randint(10000000, 99999999)

  #Concatenamos ambas variables para tener el id
  partitionKey = fechaStr + str(aleatorio)

  #Devolvemos el valor
  return partitionKey

# COMMAND ----------

#Ejemplo de uso de la función
generarPartitionKey()

# COMMAND ----------

#Escribimos el registro en el tópico distribuyendo los registros
writer(registroBin, generarPartitionKey())

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
    writer(registroBin, generarPartitionKey())

    #Dormimos el bucle de ingesta por 1 segundo
    time.sleep(1)
