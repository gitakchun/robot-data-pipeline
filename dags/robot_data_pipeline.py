"""로봇 데이터 파이프라인 DAG

start → ingest → quality → transform → register

- 실제 로직은 pipeline/run.py의 step_* 함수. 이 파일은 '언제, 어떤 순서로, 실패하면 어떻게'만 정의
- 태스크 사이에는 JSON(경로, 에피소드 번호 목록)만 오가고, 데이터 본체는 MinIO에서 각자 읽음
- 매일 실행되지만, 데이터셋 버전(revision)이 같으면 ingest는 건너뜀
"""
from datetime import datetime, timedelta

from airflow.sdk import Param, dag, get_current_context, task


def _record_failure(context):
    """재시도까지 모두 실패한 태스크가 있으면 pipeline_runs에 failed로 기록"""
    from pipeline import db

    ti = context["ti"]
    db.finish_run(context["run_id"], "failed", error=f"{ti.task_id}: {context.get('exception')!r}")


def _ctx():
    """현재 실행 정보: (run_id, params, 몇 번째 시도인지)"""
    ctx = get_current_context()
    return ctx["run_id"], ctx["params"], ctx["ti"].try_number


@dag(
    dag_id="robot_data_pipeline",
    schedule="@daily",                       # 매일 자정(UTC) 실행
    start_date=datetime(2026, 10, 1),
    catchup=False,                           # 과거 날짜분을 한꺼번에 몰아서 실행하지 않음
    max_active_runs=1,                       # 같은 DAG가 동시에 2개 돌지 않게 (같은 버킷 경로를 덮어쓰므로)
    default_args={
        "retries": 2,                        # 실패하면 2번 더 시도 (총 3번)
        "retry_delay": timedelta(seconds=20),
        "on_failure_callback": _record_failure,
    },
    params={                                 # 수동 실행(Trigger) 때 UI에서 바꿀 수 있는 값
        "repo_id": Param("lerobot/aloha_sim_insertion_human", type="string"),
        "inject_faults": Param(False, type="boolean", description="결함 주입 버전(-dirty)으로 처리"),
        "fail_first_try": Param(False, type="boolean", description="ingest 첫 시도를 일부러 실패시켜 재시도 확인"),
    },
    tags=["robot", "lerobot"],
)
def robot_data_pipeline():

    @task
    def start():
        from pipeline import run
        run_id, p, _ = _ctx()
        run.step_start(run_id, p["repo_id"], p["inject_faults"])

    @task
    def ingest() -> dict:
        from pipeline import run
        _, p, try_number = _ctx()
        if p["fail_first_try"] and try_number == 1:
            raise RuntimeError("재시도 데모: 첫 시도 의도적 실패 (네트워크 장애 가정)")
        return run.step_ingest(p["repo_id"], p["inject_faults"])

    @task
    def quality(meta: dict) -> list:
        from pipeline import run
        run_id, _, _ = _ctx()
        return run.step_quality(run_id, meta)

    @task
    def transform(meta: dict, failures: list) -> list:
        from pipeline import run
        return run.step_transform(meta, failures)

    @task
    def register(meta: dict, failures: list, curated: list) -> int:
        from pipeline import run
        run_id, _, _ = _ctx()
        return run.step_register(run_id, meta, failures, curated)

    # 함수 호출 결과를 다음 함수 인자로 넘기면 Airflow가 자동으로 순서(의존성)를 만듦
    meta = ingest()
    start() >> meta                          # >> : 명시적 순서 지정 (값을 주고받지 않는 경우)
    failures = quality(meta)
    curated = transform(meta, failures)
    register(meta, failures, curated)


robot_data_pipeline()
