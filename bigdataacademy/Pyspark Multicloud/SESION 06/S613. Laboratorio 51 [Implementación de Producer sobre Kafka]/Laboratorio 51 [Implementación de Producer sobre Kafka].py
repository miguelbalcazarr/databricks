# Databricks notebook source
## #######################################################################################################
## 
## @copyright Big Data Academy [info@bigdataacademy.org]
## @professor Alonso Melgarejo [alonsoraulmgs@gmail.com]
## 
## #######################################################################################################

# COMMAND ----------

# DBTITLE 1,1. Lectura de API Real-Time
#Librería para consultar APIs web
import requests

# COMMAND ----------

#Librería de manipulación de JSON
import json

# COMMAND ----------

#Nos conectamos a un API y obtenemos la respuesta
response = requests.get("http://api.open-notify.org/iss-now.json")

#Verificamos
response

# COMMAND ----------

#Dentro de la respuesta, el contenido de la respuesta está en la variable "content"
response.content

# COMMAND ----------

#Colocamos la respuesta en un objeto JSON
registro = json.loads(response.content)

# COMMAND ----------

#Verificamos
registro

# COMMAND ----------

# DBTITLE 1,2. Función "sourceClient" para lectura de datos en real-time
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

# DBTITLE 1,3. Binarización del registro
#En Kinesis no es necesario hacer la binarización
#Esto se debe a que el propio tópico de Kinesis binariza el registro una vez recibido
#Para que la binarización pueda hacerse de manera automático el registro tiene que ser del tipo de dato "str"
#Si verificamos el tipo de dato del registro es un "dict" (json)
#Deberemos convertirlo a "str"
type(registro)

# COMMAND ----------

#Convertimos la variable a "str"
registroBin = json.dumps(registro)

#Verificamos, vemos que el contenido es el mismo
print(registroBin)

# COMMAND ----------

#Si verificamos el tipo de dato, ahora es un "str"
type(registroBin)

# COMMAND ----------

# DBTITLE 1,4. Función "binary" para binarización
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

# DBTITLE 1,5. Escritura del registro en un tópico de "Kafka"
#Utilitario para enviar datos a Kafka
from kafka import KafkaProducer

# COMMAND ----------

#Instanciamos la conexión al servidor KAFKA
producer = KafkaProducer(bootstrap_servers="localhost:9092")

# COMMAND ----------

#Preparamos el envío de los datos al tópico "transaccion"
#Con la función "encode" binarizamos el registro
producer.send("transaccion", registroBin.encode())

# COMMAND ----------

#Envíamos los datos al tópico
producer.flush()

# COMMAND ----------

# DBTITLE 1,6. Función "writer" para envío de registros al tópico
#Creamos la función que envía con el producer el registro al tópico
def writer(registroBin):
    #Instanciamos la conexión al servidor KAFKA
    producer = KafkaProducer(bootstrap_servers="localhost:9092")

    #Preparamos el envío de los datos al tópico "transaccion"
    #Con la función "encode" binarizamos el registro
    producer.send("transaccion", registroBin.encode())

    #Envíamos los datos al tópico
    producer.flush()

# COMMAND ----------

#Ejecutamos la función
writer(registroBin)

# COMMAND ----------

# DBTITLE 1,7. Bucle de ejecución de ingesta hacía el tópico
#Haremos un bucle de ingesta
#Necesitamos pausar la ingesta cada 1 segundo para no saturar el servidor de la fuente de datos
#Importamos el utilitario que nos permite pausar la ejecución del bucle
import time

# COMMAND ----------

#Haremos 20 inserciones de prueba hacía el tópico
for i in range(1, 20):
    #Mostramos el número de la iteración
    print("Iteración: "+str(i))

    #Leemos los datos desde la fuente de datos
    registro = sourceClient()

    #Dejamos el registro listo para la binarización
    registroBin = binary(registro)

    #Enviamos el registro al tópico
    writer(registroBin)

    #Dormimos el bucle de ingesta por 1 segundo
    time.sleep(1)
