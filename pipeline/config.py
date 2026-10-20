"""환경변수(.env)에서 접속 정보를 읽는다.

코드에 주소·비밀번호를 박지 않는 이유:
같은 코드를 맥(localhost:5433)에서도, Airflow 컨테이너(postgres:5432)에서도 돌리기 위해.
"""
import os

from dotenv import load_dotenv

load_dotenv()  # 프로젝트 루트의 .env를 찾아 os.environ에 넣음 (이미 있는 값은 덮어쓰지 않음)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ["MINIO_ROOT_USER"]
MINIO_SECRET_KEY = os.environ["MINIO_ROOT_PASSWORD"]

RAW_BUCKET = "raw"
CURATED_BUCKET = "curated"

PG_DSN = (
    f"host={os.getenv('PG_HOST', 'localhost')} port={os.getenv('PG_PORT', '5433')} "
    f"dbname={os.environ['POSTGRES_DB']} user={os.environ['POSTGRES_USER']} "
    f"password={os.environ['POSTGRES_PASSWORD']}"
)
