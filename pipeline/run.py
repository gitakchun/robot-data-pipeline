"""파이프라인 단계 함수 + 로컬 실행용 CLI

각 step_* 함수는 JSON으로 바꿀 수 있는 값(dict/list/str/int)만 받고 돌려준다.
→ Airflow가 태스크 사이에 값을 넘길 때(XCom) JSON으로 저장하기 때문.
→ 무거운 데이터(DataFrame)는 넘기지 않고, 각 단계가 MinIO에서 직접 읽는다.

로컬 실행:
  uv run python -m pipeline.run                          # 원본
  uv run python -m pipeline.run --inject-faults          # 결함 주입 버전
  uv run python -m pipeline.run --repo-id lerobot/pusht  # 다른 데이터셋
"""
import argparse
from datetime import datetime

from pipeline import config, db, ingest, quality, storage, transform


def dataset_name(repo_id: str, inject_faults: bool) -> str:
    return repo_id.split("/")[1] + ("-dirty" if inject_faults else "")


def step_start(run_id: str, repo_id: str, inject_faults: bool) -> None:
    db.init_schema()
    db.start_run(run_id, dataset_name(repo_id, inject_faults))


def step_ingest(repo_id: str, inject_faults: bool) -> dict:
    """① HF → raw 버킷. 반환: {dataset, revision, raw_prefix, ...}"""
    return ingest.ingest(repo_id, inject_faults)


def step_quality(run_id: str, meta: dict) -> list[list]:
    """② 품질 검사 → quality_results. 반환: [[에피소드, [fail 검사들], [warn 검사들]], ...]
    (dict로 돌려주면 JSON 변환 때 정수 키가 문자열이 돼서 리스트로 반환)"""
    df, info, stats = quality.load_raw(meta["raw_prefix"])
    rows = quality.run_checks(df, info, stats)
    db.save_quality_results(run_id, meta["dataset"], rows)
    summary = quality.summarize(rows)
    n_fail = sum(1 for s in summary.values() if s["fail"])
    n_warn = sum(1 for s in summary.values() if s["warn"] and not s["fail"])
    print(f"[quality] 전체 {len(summary)} | 사용 가능 {len(summary) - n_fail} (그중 경고 {n_warn}) | 제외 {n_fail}")
    for ep, s in summary.items():
        if s["fail"]:
            print(f"          ep{ep} FAIL: {', '.join(s['fail'])}")
    return [[ep, s["fail"], s["warn"]] for ep, s in summary.items()]


def step_transform(meta: dict, failures: list[list]) -> list[list]:
    """③ 통과 에피소드만 curated Parquet. 반환: [[에피소드, curated key], ...]"""
    df, info, _ = quality.load_raw(meta["raw_prefix"])
    passed = [ep for ep, fail, _ in failures if not fail]
    keys = transform.build_curated(df, info, passed, transform.curated_prefix(meta["dataset"], meta["revision"]))
    return [[ep, k] for ep, k in keys.items()]


def step_register(run_id: str, meta: dict, failures: list[list], curated: list[list]) -> int:
    """④ episodes 테이블 등록 + 실행 성공 기록"""
    df, info, _ = quality.load_raw(meta["raw_prefix"])
    fps = info["fps"]
    lengths = df.groupby("episode_index").size()
    # tasks.parquet: 인덱스 = 작업 설명 문장, 컬럼 task_index = 번호 → {번호: 문장}으로 뒤집음
    tasks = storage.read_parquet(config.RAW_BUCKET, meta["raw_prefix"] + "meta/tasks.parquet")
    task_names = {int(i): name for name, i in tasks["task_index"].items()}
    ep_task = df.groupby("episode_index")["task_index"].first()
    curated_keys = dict(curated)

    rows = [
        {
            "dataset": meta["dataset"], "revision": meta["revision"], "episode_index": ep,
            "task": task_names.get(int(ep_task[ep])), "n_frames": int(lengths[ep]),
            "duration_s": round(int(lengths[ep]) / fps, 3), "raw_prefix": meta["raw_prefix"],
            "curated_key": curated_keys.get(ep), "quality_passed": not fail,
            "failed_checks": fail, "warnings": warn, "last_run_id": run_id,
        }
        for ep, fail, warn in failures
    ]
    db.upsert_episodes(rows)
    db.finish_run(run_id, "success", revision=meta["revision"])
    print(f"[register] episodes {len(rows)}행 upsert")
    return len(rows)


def main(repo_id: str, inject_faults: bool) -> None:
    run_id = f"manual__{datetime.now():%Y%m%dT%H%M%S}"
    step_start(run_id, repo_id, inject_faults)
    try:
        meta = step_ingest(repo_id, inject_faults)
        failures = step_quality(run_id, meta)
        curated = step_transform(meta, failures)
        step_register(run_id, meta, failures, curated)
        print(f"[done] run_id={run_id}")
    except Exception as e:
        db.finish_run(run_id, "failed", error=repr(e))  # 실패도 기록 → 나중에 원인 추적
        raise


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--repo-id", default="lerobot/aloha_sim_insertion_human")
    p.add_argument("--inject-faults", action="store_true")
    a = p.parse_args()
    main(a.repo_id, a.inject_faults)
