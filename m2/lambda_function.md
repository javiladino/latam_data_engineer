# AWS Lambda Function — Data Ingestion Pipeline

## Objetivo

Construir una Lambda Function en Python que descargue un archivo Parquet desde una URL externa (por defecto el dataset de NYC Taxi & Limousine Commission) y lo almacene en un bucket S3, usando streaming para evitar cargar el archivo completo en memoria.

---

## Arquitectura de la solución

```
Evento (EventBridge / API GW / invocación directa)
        │
        ▼
┌───────────────────┐      HTTP GET (streaming)      ┌─────────────────────┐
│  AWS Lambda       │ ──────────────────────────────▶ │  Fuente externa     │
│  (lambda_handler) │                                 │  (CloudFront/URL)   │
│                   │ ◀─────────── stream ────────────┘
│                   │
│                   │      upload_fileobj (streaming)  ┌─────────────────────┐
│                   │ ──────────────────────────────▶ │  Amazon S3          │
└───────────────────┘                                 │  (Data Lake / Raw)  │
                                                      └─────────────────────┘
```

**Patrón clave**: el archivo nunca se carga completo en memoria. El stream HTTP se pasa directamente a `upload_fileobj`, lo que permite procesar archivos de varios GB dentro del límite de 512 MB de memoria de Lambda.

---

## Paso a paso: construcción de la Lambda

### 1. Imports y dependencias

```python
import os
import io
import json
import time
import logging
from urllib.parse import urlparse
import urllib3
from urllib3.util.retry import Retry
import boto3
from botocore.exceptions import ClientError
```

**Decisiones de diseño:**

| Librería | Por qué se usa |
|---|---|
| `urllib3` | Cliente HTTP de bajo nivel con soporte nativo de streaming y política de reintentos. `requests` no ofrece la misma integración directa con `upload_fileobj`. |
| `boto3` | SDK oficial de AWS; `s3.upload_fileobj` acepta cualquier objeto file-like, incluyendo un stream HTTP. |
| `os.getenv` | Permite inyectar configuración sin cambiar el código (12-Factor App). |
| `logging` | Preferible a `print`; CloudWatch Logs captura niveles y estructuras correctamente. |

**Dependencias externas** (incluir en el deployment package o Layer):
- `urllib3` — ya viene en el runtime de Python 3.12+ de Lambda
- `boto3` — ya viene incluido en el runtime de Lambda

> Para proyectos de producción, fijar versiones en `requirements.txt` y construir el package con Docker usando la imagen `public.ecr.aws/lambda/python:3.12`.

---

### 2. Configuración de logging

```python
logger = logging.getLogger()
logger.setLevel(logging.INFO)
```

**Buenas prácticas:**
- Obtener el logger **a nivel de módulo** (no dentro del handler) para que el objeto se reutilice entre invocaciones calientes (warm starts).
- Usar `logging.INFO` en producción. Cambiar a `logging.DEBUG` mediante variable de entorno si se necesita diagnóstico puntual.
- Nunca usar `print()` en una Lambda de producción; los logs no tendrán nivel ni estructura aprovechable en CloudWatch.

---

### 3. Variables globales y cliente HTTP

```python
DEFAULT_BUCKET  = os.getenv("BUCKET", "latam-engineer-data-lake-us-east-1")
DEFAULT_PREFIX  = os.getenv("PREFIX", "raw")
DEFAULT_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "30"))
DEFAULT_RETRIES = int(os.getenv("HTTP_RETRIES", "5"))

http = urllib3.PoolManager(
    timeout=DEFAULT_TIMEOUT,
    retries=Retry(
        total=DEFAULT_RETRIES,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    ),
    headers={"User-Agent": "latam-engineer-ingestor/1.0"},
)
s3 = boto3.client("s3")
```

**Por qué inicializar clientes a nivel de módulo (fuera del handler):**

Lambda reutiliza el mismo proceso entre invocaciones consecutivas (warm start). Si los clientes se inicializan dentro del handler, cada invocación pagaría el costo de establecer la conexión TCP/TLS con AWS y con la URL externa. Inicializarlos fuera los mantiene vivos entre invocaciones, reduciendo latencia hasta un 80%.

**Política de reintentos explicada:**

```
backoff_factor=1  →  esperas: 0s, 2s, 4s, 8s, 16s  (2^(intento-1) * backoff_factor)
status_forcelist  →  sólo reintentar en errores transitorios (rate limiting, servidor caído)
allowed_methods   →  GET y HEAD son idempotentes; es seguro reintentar
raise_on_status=False → no lanzar excepción en el último intento fallido; manejar el status manualmente
```

---

### 4. Funciones auxiliares (helpers)

Los helpers encapsulan lógica reutilizable y mantienen el handler limpio. Siguen el principio de responsabilidad única (SRP).

#### 4a. Respuestas HTTP estandarizadas

```python
def _bad_request(msg: str, extra: dict | None = None):
    logger.error("400 Bad Request: %s", msg)
    body = {"error": msg}
    if extra:
        body.update(extra)
    return {"statusCode": 400, "body": json.dumps(body)}

def _server_error(msg: str, extra: dict | None = None):
    ...
    return {"statusCode": 500, "body": json.dumps(body)}
```

Las Lambda expuestas vía API Gateway deben devolver un diccionario con `statusCode` y `body`. Centralizar estos formatos evita inconsistencias y facilita los tests unitarios.

#### 4b. Validación de parámetros

```python
def _require_str(d: dict, key: str, optional: bool = False, default: str | None = None) -> str | None:
    val = d.get(key, default)
    if val is None and not optional:
        raise ValueError(f"Missing required field '{key}'")
    if val is not None and not isinstance(val, str):
        raise ValueError(f"Field '{key}' must be a string")
    return val
```

Valida en la frontera del sistema (el evento de entrada). Falla rápido con mensajes claros antes de hacer cualquier llamada de red o I/O.

#### 4c. Normalización de rutas S3

```python
def _normalize_prefix(prefix: str) -> str:
    if prefix.startswith("s3://"):
        parts = prefix.split("/", 3)
        prefix = parts[3] if len(parts) > 3 else ""
    return prefix.strip("/")
```

Protege contra errores de usuario. Si alguien pasa `s3://mi-bucket/raw` en lugar de `raw`, la función lo corrige silenciosamente y lo registra. Nunca confíes en que el input externo tiene el formato exacto esperado.

---

### 5. El handler principal

```python
def lambda_handler(event, context):
```

Esta es la función de entrada que AWS invoca. Debe:
1. Recibir `event` (dict con el payload) y `context` (metadatos de la invocación).
2. Devolver siempre un dict con `statusCode` (no lanzar excepciones sin capturar).

#### 5a. Lectura y validación del evento

```python
bucket  = _require_str(event, "bucket",  optional=True, default=DEFAULT_BUCKET)
prefix  = _require_str(event, "prefix",  optional=True, default=DEFAULT_PREFIX)
url     = _require_str(event, "url",     optional=True, default=None)
dataset = _require_str(event, "dataset", optional=(url is not None), default=None)
date    = _require_str(event, "date",    optional=(url is not None), default=None)
```

**Dos modos de invocación:**

| Modo | Campos requeridos | Cuándo usar |
|---|---|---|
| URL explícita | `url` | Descargar cualquier archivo externo |
| Dataset + fecha | `dataset` + `date` | Automatizar pipelines de NYC TLC |

#### 5b. Construcción de la clave S3

```python
s3_key = f"{prefix}/{dataset_dir}/{year}/{filename}"
# Ejemplo: raw/yellow_tripdata/2025/yellow_tripdata_2025-01.parquet
```

**Particionamiento por jerarquía**: esta estructura permite consultas eficientes con Athena/Glue, aplicar políticas de lifecycle por año, y facilita la organización por dataset en el Data Lake.

```
s3://bucket/
└── raw/
    └── yellow_tripdata/
        ├── 2024/
        │   └── yellow_tripdata_2024-12.parquet
        └── 2025/
            └── yellow_tripdata_2025-01.parquet
```

#### 5c. Descarga y subida en streaming

```python
resp = http.request("GET", url, preload_content=False)  # <-- streaming, no carga en memoria

s3.upload_fileobj(resp, bucket, s3_key)  # <-- lee del stream y sube a S3 en chunks
```

`preload_content=False` es el parámetro crítico: le indica a urllib3 que **no** descargue el cuerpo completo de la respuesta. En su lugar, el objeto `resp` actúa como un file-like object que `upload_fileobj` consume en chunks de 8 MB por defecto (configurable con `multipart_chunksize`).

#### 5d. Manejo de errores

```python
try:
    s3.upload_fileobj(resp, bucket, s3_key)
except ClientError as e:
    logger.exception("S3 upload failed")
    resp.release_conn()
    raise RuntimeError(...)
finally:
    resp.release_conn()  # siempre liberar la conexión HTTP
```

- `logger.exception` registra el stack trace completo en CloudWatch, esencial para diagnóstico.
- `resp.release_conn()` en el bloque `finally` garantiza que la conexión HTTP se devuelve al pool aunque ocurra un error.
- Se captura `ClientError` de botocore para errores de AWS (permisos, bucket inexistente) por separado de excepciones genéricas.

#### 5e. Respuesta exitosa

```python
return {
    "statusCode": 200,
    "body": json.dumps({
        "message": "Uploaded successfully",
        "bucket": bucket,
        "key": s3_key,
        "bytes": int(content_len) if content_len and content_len.isdigit() else None,
        "etag": head.get("ETag"),
        "version_id": head.get("VersionId"),
        "source_url": url,
        "dataset": dataset,
        "date": date,
        "elapsed_sec": elapsed,
    })
}
```

La respuesta incluye metadatos de observabilidad: ETag, VersionId, tiempo de ejecución y tamaño. Estos campos son útiles para sistemas downstream que consuman el evento de retorno (Step Functions, otro Lambda, etc.).

---

## Configuración: Variables de entorno

| Variable | Default | Descripción |
|---|---|---|
| `BUCKET` | `latam-engineer-data-lake-us-east-1` | Bucket S3 destino |
| `PREFIX` | `raw` | Prefijo/carpeta dentro del bucket |
| `HTTP_TIMEOUT` | `30` | Timeout en segundos para las peticiones HTTP |
| `HTTP_RETRIES` | `5` | Número máximo de reintentos HTTP |

Configurarlas en la consola de Lambda o vía IaC (SAM / CDK / Terraform):

```yaml
# SAM template.yaml
Environment:
  Variables:
    BUCKET: mi-data-lake-prod
    PREFIX: raw
    HTTP_TIMEOUT: "60"
    HTTP_RETRIES: "3"
```

---

## Permisos IAM requeridos

La ejecución role de la Lambda necesita al menos:

```json
{
  "Effect": "Allow",
  "Action": [
    "s3:PutObject",
    "s3:GetObject",
    "s3:HeadObject"
  ],
  "Resource": "arn:aws:s3:::latam-engineer-data-lake-us-east-1/*"
}
```

Principio de mínimo privilegio: otorgar solo los permisos necesarios, sobre el bucket y prefijo específico.

---

## Despliegue

### Opción A: Deployment package (zip)

```bash
# Crear entorno y empaquetar
pip install urllib3 boto3 -t ./package
cp lambda_function_antony.py ./package/lambda_function.py
cd package && zip -r ../lambda.zip . && cd ..

# Desplegar con AWS CLI
aws lambda update-function-code \
  --function-name latam-ingestor \
  --zip-file fileb://lambda.zip
```

### Opción B: SAM (recomendado para proyectos reales)

```bash
sam build && sam deploy --guided
```

---

## Eventos de prueba

### Modo dataset + fecha

```json
{
  "dataset": "yellow_tripdata",
  "date": "2025-01"
}
```

### Modo URL explícita

```json
{
  "url": "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-01.parquet",
  "dataset": "green_tripdata",
  "date": "2025-01",
  "bucket": "mi-bucket-dev",
  "prefix": "raw"
}
```

### Test local con pytest

```python
from lambda_function import lambda_handler

def test_missing_params():
    response = lambda_handler({}, None)
    assert response["statusCode"] == 400

def test_invalid_date_format():
    response = lambda_handler({"dataset": "yellow_tripdata", "date": "202501"}, None)
    assert response["statusCode"] == 400
```

---

## Checklist de buenas prácticas aplicadas

- [x] **Streaming end-to-end**: no se carga el archivo en memoria (`preload_content=False` + `upload_fileobj`)
- [x] **Clientes inicializados fuera del handler**: se reutilizan en warm starts
- [x] **Reintentos con backoff exponencial**: tolerancia a fallos transitorios de red
- [x] **Variables de entorno para configuración**: desacoplamiento entre código y entorno
- [x] **Validación en la frontera de entrada**: fail fast con mensajes claros
- [x] **Logging estructurado con niveles**: CloudWatch-friendly, sin `print()`
- [x] **Liberación de conexiones en `finally`**: evita leaks de conexiones HTTP
- [x] **Jerarquía S3 con particionamiento**: `prefix/dataset/year/file.parquet`
- [x] **Respuestas API Gateway compatibles**: `statusCode` + `body` siempre
- [x] **Separación por responsabilidad**: helpers vs. handler, lógica vs. I/O
