"""PostgreSQL 입출력 (psycopg 3)."""
from pathlib import Path

import psycopg

from pipeline import config

SCHEMA_SQL = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"


def connect() -> psycopg.Connection:
    return psycopg.connect(config.PG_DSN)


def init_schema() -> None:
    with connect() as conn:  # with 블록이 에러 없이 끝나면 commit, 에러 나면 rollback
        conn.execute(SCHEMA_SQL.read_text())


def start_run(run_id: str, dataset: str) -> None:
    with connect() as conn:
        conn.execute(
            """INSERT INTO pipeline_runs (run_id, dataset, status) VALUES (%s, %s, 'running')
               ON CONFLICT (run_id) DO UPDATE SET status = 'running', started_at = now(),
                                                 finished_at = NULL, error = NULL""",
            (run_id, dataset),
        )


def finish_run(run_id: str, status: str, revision: str | None = None, error: str | None = None) -> None:
    with connect() as conn:
        conn.execute(
            """UPDATE pipeline_runs
               SET status = %s, revision = COALESCE(%s, revision), error = %s, finished_at = now()
               WHERE run_id = %s""",
            (status, revision, error, run_id),
        )


def save_quality_results(run_id: str, dataset: str, rows: list[dict]) -> None:
    # 같은 run_id로 재시도하면 이전 결과를 지우고 다시 씀 → 재시도해도 중복 없음 (멱등)
    with connect() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM quality_results WHERE run_id = %s", (run_id,))
        cur.executemany(
            """INSERT INTO quality_results
               (run_id, dataset, episode_index, check_name, metric_value, threshold, status, passed)
               VALUES (%(run_id)s, %(dataset)s, %(episode_index)s, %(check_name)s,
                       %(metric_value)s, %(threshold)s, %(status)s, %(passed)s)""",
            [{**r, "run_id": run_id, "dataset": dataset, "passed": r["status"] != "fail"} for r in rows],
        )


def upsert_episodes(rows: list[dict]) -> None:
    # UPSERT: 없으면 INSERT, 이미 있으면(같은 PK) UPDATE
    with connect() as conn, conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO episodes
               (dataset, revision, episode_index, task, n_frames, duration_s, raw_prefix,
                curated_key, quality_passed, failed_checks, warnings, last_run_id, updated_at)
               VALUES (%(dataset)s, %(revision)s, %(episode_index)s, %(task)s, %(n_frames)s,
                       %(duration_s)s, %(raw_prefix)s, %(curated_key)s, %(quality_passed)s,
                       %(failed_checks)s, %(warnings)s, %(last_run_id)s, now())
               ON CONFLICT (dataset, revision, episode_index) DO UPDATE SET
                 task = EXCLUDED.task, n_frames = EXCLUDED.n_frames,
                 duration_s = EXCLUDED.duration_s, raw_prefix = EXCLUDED.raw_prefix,
                 curated_key = EXCLUDED.curated_key, quality_passed = EXCLUDED.quality_passed,
                 failed_checks = EXCLUDED.failed_checks, warnings = EXCLUDED.warnings,
                 last_run_id = EXCLUDED.last_run_id,
                 updated_at = now()""",
            rows,
        )
