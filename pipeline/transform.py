"""③ 변환: 품질 통과 에피소드만 학습용 Parquet으로 → curated 버킷

- 배열 컬럼(observation.state, action)을 관절별 컬럼으로 펼침 → SQL·분석 도구에서 바로 쓰기 쉬움
- 에피소드 1개 = 파일 1개 → episodes 테이블에서 고른 에피소드만 골라 읽을 수 있음
- 쓰기 전에 해당 경로를 비움 → 이전 실행에서 통과했다가 이번에 불합격한 에피소드 파일이 남지 않음 (멱등)
"""
import numpy as np
import pandas as pd

from pipeline import config, storage


def curated_prefix(dataset: str, revision: str) -> str:
    return f"{dataset}/{revision[:8]}/"


def flatten(g: pd.DataFrame, joints: list[str]) -> pd.DataFrame:
    state = pd.DataFrame(np.stack(g["observation.state"].values), columns=[f"state_{j}" for j in joints], index=g.index)
    action = pd.DataFrame(np.stack(g["action"].values), columns=[f"action_{j}" for j in joints], index=g.index)
    meta = g[["episode_index", "frame_index", "timestamp", "task_index"]]
    return pd.concat([meta, state, action], axis=1).reset_index(drop=True)


def build_curated(df: pd.DataFrame, info: dict, passed_episodes: list[int], prefix: str) -> dict[int, str]:
    joints = info["features"]["observation.state"]["names"]["motors"]
    removed = storage.delete_prefix(config.CURATED_BUCKET, prefix)
    if removed:
        print(f"[transform] 이전 결과 {removed}개 삭제: curated/{prefix}")

    keys = {}
    for ep in passed_episodes:
        key = f"{prefix}episode_{ep:06d}.parquet"
        storage.write_parquet(flatten(df[df["episode_index"] == ep], joints), config.CURATED_BUCKET, key)
        keys[ep] = key
    print(f"[transform] {len(keys)}개 에피소드 → curated/{prefix}")
    return keys
