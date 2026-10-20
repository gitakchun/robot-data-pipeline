"""'현장에서 들어온 지저분한 데이터'를 흉내내기 위한 결함 주입.

공개 데이터셋은 이미 정제돼 있어 품질 검사가 아무것도 못 잡는다.
그래서 결함을 의도적으로 넣은 '-dirty' 버전을 만들어 검사 로직을 검증한다.
무작위가 아니라 고정된 위치에 넣어서 매번 같은 결과가 나오게 한다 (재현성).
"""
from pathlib import Path

import numpy as np
import pandas as pd

# (에피소드, 결함 종류, 설명) — 노트북 01과 동일
FAULTS = [
    (3, "frame_drop", "200~219번 프레임 삭제 (전송 중 유실)"),
    (7, "truncated", "100번 이후 전부 잘림 (수집 중단)"),
    (12, "nan", "left_elbow 값 5개 NaN (센서 값 유실)"),
    (20, "spike", "250번 프레임 left_elbow = 9.9 (센서 노이즈)"),
]


def _apply(df: pd.DataFrame) -> pd.DataFrame:
    ep, fi = df["episode_index"], df["frame_index"]
    df = df[~((ep == 3) & fi.between(200, 219))]
    df = df[~((df["episode_index"] == 7) & (df["frame_index"] >= 100))].copy()

    def set_joint(mask, joint_idx, value):
        for i in df.index[mask]:
            arr = np.array(df.at[i, "observation.state"], dtype="float32")  # 원본 배열을 직접 수정하지 않도록 복사
            arr[joint_idx] = value
            df.at[i, "observation.state"] = arr

    set_joint((df["episode_index"] == 12) & df["frame_index"].isin([50, 120, 200, 300, 400]), 2, np.nan)
    set_joint((df["episode_index"] == 20) & (df["frame_index"] == 250), 2, 9.9)
    return df


def inject(local_dir: Path) -> None:
    """다운로드한 data/*.parquet 파일들을 제자리에서 수정한다. (meta/는 원본 그대로 → 메타와 실제가 어긋남)"""
    for f in sorted((local_dir / "data").rglob("*.parquet")):
        _apply(pd.read_parquet(f)).to_parquet(f, index=False)
