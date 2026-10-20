"""MinIO 입출력. MinIO는 S3 API와 호환되므로 AWS용 라이브러리(boto3)를 그대로 쓴다.
→ 나중에 endpoint만 AWS S3로 바꾸면 코드 수정 없이 클라우드로 이전 가능.
"""
import io
import json
from functools import lru_cache
from pathlib import Path

import boto3
import pandas as pd

from pipeline import config


@lru_cache  # 클라이언트를 한 번만 만들어 재사용
def client():
    return boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT,
        aws_access_key_id=config.MINIO_ACCESS_KEY,
        aws_secret_access_key=config.MINIO_SECRET_KEY,
        region_name="us-east-1",  # MinIO는 리전 개념이 없지만 boto3가 값을 요구함
    )


def upload_file(local: Path, bucket: str, key: str) -> None:
    client().upload_file(str(local), bucket, key)


def put_bytes(data: bytes, bucket: str, key: str) -> None:
    client().put_object(Bucket=bucket, Key=key, Body=data)


def get_bytes(bucket: str, key: str) -> bytes:
    return client().get_object(Bucket=bucket, Key=key)["Body"].read()


def list_keys(bucket: str, prefix: str) -> list[str]:
    # list_objects_v2는 한 번에 최대 1000개 → paginator로 전부 가져옴
    keys = []
    for page in client().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys += [obj["Key"] for obj in page.get("Contents", [])]
    return sorted(keys)


def exists(bucket: str, key: str) -> bool:
    try:
        client().head_object(Bucket=bucket, Key=key)  # 내용은 안 받고 메타정보만 조회
        return True
    except client().exceptions.ClientError:
        return False


def delete_prefix(bucket: str, prefix: str) -> int:
    keys = list_keys(bucket, prefix)
    for i in range(0, len(keys), 1000):  # 한 번에 최대 1000개 삭제 가능
        client().delete_objects(
            Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys[i : i + 1000]]}
        )
    return len(keys)


# --- 자주 쓰는 형식 헬퍼 ---
def read_json(bucket: str, key: str) -> dict:
    return json.loads(get_bytes(bucket, key))


def read_parquet(bucket: str, key: str) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(get_bytes(bucket, key)))


def write_parquet(df: pd.DataFrame, bucket: str, key: str) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    put_bytes(buf.getvalue(), bucket, key)
