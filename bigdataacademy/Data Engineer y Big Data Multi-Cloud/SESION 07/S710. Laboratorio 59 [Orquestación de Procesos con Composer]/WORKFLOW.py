# Importamos las bibliotecas necesarias para el código

#Utilitario para trabajar con fechas
from datetime import datetime

#Utilitario para trabajr con la programación de horarios
from datetime import timedelta

#Utilitario para definir el proceso que se ejecuta
#Un DAG es un conjunto de pasos orquestados
from airflow import DAG

#Utilitario para ejecutar procesos de Data Fusion
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator

#Creamos el DAG
with DAG(
    #Nombre del proceso
    "SOLUTION_REPORTE",
    
    #Configuración básica
    default_args = {
        #Fecha de inicio del proceso
        #El proceso comenzará a ejecutarse desde el 19 de Abril del 2025
        "start_date": datetime(2025, 4, 19),
        
        #Número de reintentos del flujo si es que el proceso falla
        "retries": 1
    },
    
    #Expresión CRON de ejecución
    #Se ejecutará los lunes, martes y miércoles a las 2:30PM
    schedule_interval='30 14 * * 2,3,4'
) as dag:
    #Ejecución del preoceso TO_SILVER
    TO_SILVER = CloudDataFusionStartPipelineOperator(
        location = "us-west1", #Región donde vive la instancia de Data Fusion
        instance_name = "serveretl", #Nombre de la instancia
        pipeline_name = "TO_SILVER", #Nombre del JOB de DATA FUSION
        task_id = "TO_SILVER" #Identificador del paso, generalmente se coloca el nombre del JOB
    )
    
    # Segundo paso
    TO_GOLD = CloudDataFusionStartPipelineOperator(
        location = "us-west1", #Región donde vive la instancia de Data Fusion
        instance_name = "serveretl", #Nombre de la instancia
        pipeline_name = "TO_GOLD", #Nombre del JOB de DATA FUSION
        task_id = "TO_GOLD" #Identificador del paso, generalmente se coloca el nombre del JOB
    )
    
    #Definimos el orden de ejecución
    TO_SILVER >> TO_GOLD