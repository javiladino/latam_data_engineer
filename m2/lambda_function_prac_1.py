import urllib.request
import boto3
import os

def lambda_handler(event, context):
    # 1. Configuración de S3 (Donde guardaremos los archivos)
    s3 = boto3.client('s3')
    nombre_bucket = os.environ['BUCKET_NAME']# <--- CAMBIA ESTO
    
    # 2. El año que queremos procesar
    year = "2025"
    
    # 3. Bucle para recorrer los meses del 1 al 12
    for mes_num in range(1, 2):
        # Formateamos el mes para que sea 01, 02... 10, 11, 12
        mes = f"{mes_num:02d}"
        
        # Construimos la URL completa
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{mes}.parquet"
        nombre_archivo = f"taxi_data_{year}_{mes}.parquet"
        
        print(f"Descargando datos de {mes}/{year}...")
        
        try:
            # Descargamos el archivo temporalmente en la Lambda
            # Usamos /tmp/ porque es el único lugar donde Lambda permite escribir archivos
            ruta_temporal = f"/tmp/{nombre_archivo}"
            urllib.request.urlretrieve(url, ruta_temporal)
            
            # Subimos el archivo a tu Bucket de S3
            s3.upload_file(ruta_temporal, nombre_bucket, nombre_archivo)
            
            print(f"✅ ¡Éxito! Archivo {nombre_archivo} guardado en S3.")
            
        except Exception as e:
            # Si el archivo no existe (ej. aún no es diciembre 2025), saltará este error
            print(f"❌ No se pudo procesar el mes {mes}: {e}")

    return {
        'statusCode': 200,
        'body': 'Proceso completado'
    }