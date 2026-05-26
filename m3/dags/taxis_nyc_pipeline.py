from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
import json

# Definición del DAG
with DAG(
    dag_id='ingesta_taxis_nyc_2025',
    start_date=datetime(2026, 5, 16), # Fecha de inicio del diseño del pipeline
    schedule=None,          # Ejecución manual por ahora
    catchup=False,
    tags=['data_engineering', 'aws', 'tlc'],
) as dag:

    # 1. Definimos la lista de meses que queremos procesar (01 a 12)
    # Reemplaza el ciclo FOR interno por una lista controlada por Airflow
    lista_meses = [f"{i:02d}" for i in range(1, 13)]

    # 2. Tarea para YELLOW TRIPS (Mapeo Dinámico)
    # Airflow va a crear 12 sub-tareas en paralelo, una para cada mes
    ingestar_yellow = LambdaInvokeFunctionOperator.partial(
        task_id='ingestar_yellow_trips',
        function_name='lambda-practica1-javier', # <--- CAMBIA ESTO
        aws_conn_id='aws_default',
        region_name='eu-north-1',
        invocation_type='RequestResponse'
    ).expand(
        # Convertimos dinámicamente cada mes en el JSON que espera la Lambda
        payload=[
            json.dumps({"year": "2025", "mes": mes, "tipo_taxi": "yellow"}) 
            for mes in lista_meses
        ]
    )

    # 3. Tarea para GREEN TRIPS (Cumpliendo el Outcome del proyecto)
    ingestar_green = LambdaInvokeFunctionOperator.partial(
        task_id='ingestar_green_trips',
        function_name='lambda-practica1-javier', # <--- CAMBIA ESTO
        aws_conn_id='aws_default',
        region_name='eu-north-1',
        invocation_type='RequestResponse'
    ).expand(
        payload=[
            json.dumps({"year": "2025", "mes": mes, "tipo_taxi": "green"}) 
            for mes in lista_meses
        ]
    )

    # Flujo de trabajo: Primero Yellow (prioridad), luego Green
    ingestar_yellow >> ingestar_green