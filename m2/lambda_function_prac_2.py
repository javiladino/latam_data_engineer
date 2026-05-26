import urllib.request
import boto3
import os

def lambda_handler(event, context):
    # 1. Conexión a S3 mediante variable de entorno
    s3 = boto3.client('s3')
    nombre_bucket = os.environ['BUCKET_NAME']
    
    # 2. Parametrización: Leemos los datos que nos envía Airflow
    # Si Airflow no envía nada, usamos valores por defecto seguros
    year = event.get('year', '2025')
    mes = event.get('mes', '01')
    tipo_taxi = event.get('tipo_taxi', 'yellow') # Puede ser 'yellow' o 'green'
    
    # 3. Construcción dinámica de URLs y nombres de archivos
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{tipo_taxi}_tripdata_{year}-{mes}.parquet"
    nombre_archivo = f"{tipo_taxi}_tripdata_{year}_{mes}.parquet"
    ruta_temporal = f"/tmp/{nombre_archivo}"
    
    print(f"Iniciando descarga: {tipo_taxi} | {mes}/{year}")
    print(f"URL Origen: {url}")
    
    try:
        # Descarga el mes específico
        urllib.request.urlretrieve(url, ruta_temporal)
        
        # Sube a S3 organizando por carpetas (Buena práctica de Data Lake)
        # Ejemplo de ruta en S3: yellow/2025/yellow_tripdata_2025_01.parquet
        ruta_s3 = f"{tipo_taxi}/{year}/{nombre_archivo}"
        s3.upload_file(ruta_temporal, nombre_bucket, ruta_s3)
        
        mensaje_exito = f"✅ Éxito: {nombre_archivo} guardado en S3 en la ruta: {ruta_s3}"
        print(mensaje_exito)
        
        return {
            'statusCode': 200,
            'body': mensaje_exito
        }
        
    except Exception as e:
        print(f"❌ Error al procesar {tipo_taxi} {mes}/{year}: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }