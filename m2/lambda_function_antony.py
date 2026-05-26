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

# -------------------- Logging --------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# -------------------- Globals --------------------
# Defaults (can be overridden by EVENT fields)
DEFAULT_BUCKET = os.getenv("BUCKET", "latam-engineer-data-lake-us-east-1")
DEFAULT_PREFIX = os.getenv("PREFIX", "raw")            # no s3://, no trailing slash
DEFAULT_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "30"))
DEFAULT_RETRIES = int(os.getenv("HTTP_RETRIES", "5"))

# HTTP client with retries (idempotent GET)
http = urllib3.PoolManager(
    timeout=DEFAULT_TIMEOUT,
    retries=Retry(
        total=DEFAULT_RETRIES,
        backoff_factor= 1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    ),
    headers={"User-Agent": "latam-engineer-ingestor/1.0"},
)
s3 = boto3.client("s3")

# -------------------- Helpers --------------------
def _bad_request(msg: str, extra: dict | None = None):
    logger.error("400 Bad Request: %s", msg)
    body = {"error": msg}
    if extra:
        body.update(extra)
    return {"statusCode": 400, "body": json.dumps(body)}

def _server_error(msg: str, extra: dict | None = None):
    logger.error("500 Server Error: %s", msg)
    body = {"error": msg}
    if extra:
        body.update(extra)
    return {"statusCode": 500, "body": json.dumps(body)}

def _require_str(d: dict, key: str, optional: bool = False, default: str | None = None) -> str | None:
    val = d.get(key, default)
    if val is None and not optional:
        raise ValueError(f"Missing required field '{key}'")
    if val is not None and not isinstance(val, str):
        raise ValueError(f"Field '{key}' must be a string")
    return val

def _normalize_prefix(prefix: str) -> str:
    # strip s3://bucket/ if someone passes a whole URI by mistake
    if prefix.startswith("s3://"):
        # s3://bucket/some/prefix  -> some/prefix
        parts = prefix.split("/", 3)
        prefix = parts[3] if len(parts) > 3 else ""
    return prefix.strip("/")

def _build_default_url(dataset: str, yyyy_mm: str) -> str:
    return f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset}_{yyyy_mm}.parquet"

def _filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    fname = os.path.basename(parsed.path)
    if not fname:
        raise ValueError(f"Cannot parse filename from URL: {url}")
    return fname

def _year_from_yyyymm(yyyy_mm: str) -> str:
    # Accepts "YYYY-MM"
    if len(yyyy_mm) != 7 or yyyy_mm[4] != "-":
        raise ValueError("date must be in format 'YYYY-MM'")
    return yyyy_mm[:4]

# -------------------- Handler --------------------
def lambda_handler(event, context):
    """
    Download a remote Parquet file (NYC TLC by default) and stream-upload it to S3.

    This Lambda supports two input modes:
    1) Provide an explicit `url` to download.
    2) Omit `url` and provide `dataset` + `date` so the function builds the NYC TLC
       CloudFront URL automatically:
       https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset}_{date}.parquet

    The destination S3 object key is constructed as:
        {prefix}/{dataset}/{year}/{filename}

    Where:
    - `prefix` is normalized (leading/trailing slashes removed; if an `s3://...` URI
      is mistakenly passed, the bucket portion is stripped).
    - `year` is derived from `date` (YYYY-MM -> YYYY). If `date` is omitted because
      you used `url`, year defaults to "unknown-year".

    Environment defaults (can be overridden by event fields):
    - BUCKET (default: "latam-engineer-data-lake-us-east-1")
    - PREFIX (default: "raw")
    - HTTP_TIMEOUT (default: 30 seconds)
    - HTTP_RETRIES (default: 3)

    Args:
        event (dict): Invocation payload. See "Expected event" below.
        context: Lambda context object (unused).

    Returns:
        dict: API Gateway-style response:
            - 200 on success, with JSON body including bucket/key/etag/version_id,
              optional `bytes` (from Content-Length), and elapsed time.
            - 400 for invalid input.
            - 500 for download/upload/internal errors.

    Expected event (EXAMPLE):
    {
      "dataset": "yellow_tripdata",          # required if 'url' omitted
      "date":    "2025-01",                  # required if 'url' omitted (format: YYYY-MM)
      "bucket":  "latam-engineer-data-lake-us-east-1",   # optional (overrides env)
      "prefix":  "raw/yellow_tripdata",      # optional (overrides env)
      "url":     "https://.../yellow_tripdata_2025-01.parquet"   # optional
    }

    Notes:
    - Download and upload are performed in a streaming manner (no full file in memory).
    - HTTP retries are handled by urllib3's Retry policy for idempotent methods (GET/HEAD),
      including exponential backoff (see explanation below).
    """
    logger.info("Received event: %s", json.dumps(event, ensure_ascii=False))

    try:
        # Read params (event overrides env defaults)
        bucket = _require_str(event, "bucket", optional=True, default=DEFAULT_BUCKET)
        prefix = _require_str(event, "prefix", optional=True, default=DEFAULT_PREFIX)
        url    = _require_str(event, "url", optional=True, default=None)

        dataset = _require_str(event, "dataset", optional=(url is not None), default=None)
        date    = _require_str(event, "date",    optional=(url is not None), default=None)

        if url is None:
            if not dataset or not date:
                return _bad_request("Either provide 'url' or both 'dataset' and 'date'")
            url = _build_default_url(dataset, date)

        # Validate/derive path elements
        filename = _filename_from_url(url)
        prefix   = _normalize_prefix(prefix)
        year     = _year_from_yyyymm(date) if date else "unknown-year"
        dataset_dir = dataset if dataset else "unknown-dataset"

        # Key pattern: {prefix}/{dataset}/{year}/{filename}
        # Example: landing/yellow_tripdata/2025/yellow_tripdata_2025-01.parquet
        s3_key = f"{prefix}/{dataset_dir}/{year}/{filename}"

        logger.info("Download URL: %s", url)
        logger.info("S3 target: s3://%s/%s", bucket, s3_key)

        # -------- Download with streaming ----------
        start = time.time()
        resp = http.request("GET", url, preload_content=False)
        if resp.status != 200:
            status = resp.status
            txt = resp.read(2048)  # read small error payload (if any)
            resp.release_conn()
            return _server_error(
                f"Failed to download: HTTP {status}",
                {"status": status, "preview": txt.decode("utf-8", errors="ignore")}
            )

        # Optional: sanity checks
        content_len = resp.headers.get("Content-Length")
        logger.info("Remote Content-Length: %s", content_len)

        # -------- Upload to S3 (streaming) ----------
        try:
            # If you want SSE-S3, add ExtraArgs={"ServerSideEncryption": "AES256"}
            s3.upload_fileobj(resp, bucket, s3_key)
        except ClientError as e:
            logger.exception("S3 upload failed")
            resp.release_conn()
            raise RuntimeError(f"Failed to download {url}: HTTP {status} :: {txt[:200].decode('utf-8', 'ignore')}")
        finally:
            resp.release_conn()

        # (Optional) HEAD to fetch ETag/VersionId for the response
        head = {}
        try:
            head = s3.head_object(Bucket=bucket, Key=s3_key)
        except ClientError:
            logger.warning("Could not HEAD the uploaded object (non-fatal).")

        elapsed = round(time.time() - start, 3)
        logger.info("Success. Uploaded in %ss → s3://%s/%s", elapsed, bucket, s3_key)

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

    except ValueError as ve:
        return _bad_request(str(ve))
    except Exception as ex:
        logger.exception("Unhandled exception")
        return _server_error("Unhandled exception", {"detail": str(ex)})