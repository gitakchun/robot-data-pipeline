"""② 품질 검사: raw 데이터를 에피소드 단위로 검사

검사 항목 (노트북 01에서 확인한 대로, 서로의 사각지대를 보완):
  episode_length  : 길이가 중앙값 대비 정상 범위인가       (잘린 에피소드)
  missing_values  : NaN이 없는가                          (센서 유실 — 범위 검사는 NaN을 못 잡음)
  timestamp_gap   : 프레임 간격이 1/fps의 1.5배를 넘지 않나 (중간 프레임 유실)
  joint_range     : 관절값이 기준 범위 안인가              (센서 노이즈·튐)

판정은 3단계: pass / warn(표시만, 학습 데이터에 포함) / fail(학습 데이터에서 제외)
→ "이상해 보임"과 "망가졌음"은 다르다. 시연이 조금 길거나 짧은 건 정상적인 다양성일 수 있어서
  버리면 오히려 데이터 분포가 좁아진다. 확실히 망가진 것만 빼고, 애매한 건 기록해 두고 사람이 판단.

joint_range 기준은 검사 대상 데이터가 아니라 원본 메타(meta/stats.json)에서 가져온다.
→ 지저분한 데이터로 기준을 만들면 이상치가 기준 자체를 넓혀버리기 때문.
"""
import numpy as np
import pandas as pd

from pipeline import config, storage

# --- 품질 정책 (기준값을 한곳에 모아둠: 바뀌면 여기만 고치면 됨) ---
LENGTH_WARN = (0.5, 1.5)   # 중앙값의 50~150% 밖 → warn
LENGTH_FAIL = (0.25, 3.0)  # 중앙값의 25% 미만 / 300% 초과 → fail (수집 중단, 녹화 안 멈춤 등)
GAP_FACTOR = 1.5           # 프레임 간격이 1/fps의 1.5배 초과면 누락으로 봄
RANGE_MARGIN = 0.1         # 관절 기준 범위 폭의 10%만큼 여유


def load_raw(prefix: str) -> tuple[pd.DataFrame, dict, dict]:
    keys = [k for k in storage.list_keys(config.RAW_BUCKET, prefix + "data/") if k.endswith(".parquet")]
    df = pd.concat([storage.read_parquet(config.RAW_BUCKET, k) for k in keys], ignore_index=True)
    df = df.sort_values(["episode_index", "frame_index"], ignore_index=True)
    info = storage.read_json(config.RAW_BUCKET, prefix + "meta/info.json")
    stats = storage.read_json(config.RAW_BUCKET, prefix + "meta/stats.json")
    return df, info, stats


def _zero_or_fail(n: int) -> str:
    return "pass" if n == 0 else "fail"


def run_checks(df: pd.DataFrame, info: dict, stats: dict) -> list[dict]:
    fps = info["fps"]
    median_len = float(df.groupby("episode_index").size().median())
    warn_lo, warn_hi = (median_len * r for r in LENGTH_WARN)
    fail_lo, fail_hi = (median_len * r for r in LENGTH_FAIL)

    s_min, s_max = np.array(stats["observation.state"]["min"]), np.array(stats["observation.state"]["max"])
    margin = (s_max - s_min) * RANGE_MARGIN
    j_lo, j_hi = s_min - margin, s_max + margin

    rows = []
    for ep, g in df.groupby("episode_index"):
        state = np.stack(g["observation.state"].values).astype("float64")
        action = np.stack(g["action"].values).astype("float64")
        dt = g["timestamp"].diff().dropna()

        n = len(g)
        n_nan = int(np.isnan(state).sum() + np.isnan(action).sum() + g.drop(columns=["observation.state", "action"]).isna().sum().sum())
        n_gap = int(((dt > GAP_FACTOR / fps) | (dt <= 0)).sum())  # 간격이 너무 크거나, 시간이 거꾸로 가는 경우
        with np.errstate(invalid="ignore"):  # NaN 비교 경고 무시 (NaN은 missing_values에서 잡음)
            n_out = int(((state < j_lo) | (state > j_hi)).sum())

        if not fail_lo <= n <= fail_hi:
            length_status = "fail"
        elif not warn_lo <= n <= warn_hi:
            length_status = "warn"
        else:
            length_status = "pass"

        ep = int(ep)
        rows += [
            {"episode_index": ep, "check_name": "episode_length", "metric_value": n, "status": length_status,
             "threshold": f"warn∉[{warn_lo:.0f},{warn_hi:.0f}] fail∉[{fail_lo:.0f},{fail_hi:.0f}] frames"},
            {"episode_index": ep, "check_name": "missing_values", "metric_value": n_nan,
             "threshold": "== 0", "status": _zero_or_fail(n_nan)},
            {"episode_index": ep, "check_name": "timestamp_gap", "metric_value": n_gap,
             "threshold": f"dt <= {GAP_FACTOR / fps:.3f}s", "status": _zero_or_fail(n_gap)},
            {"episode_index": ep, "check_name": "joint_range", "metric_value": n_out,
             "threshold": f"stats ± {RANGE_MARGIN:.0%}", "status": _zero_or_fail(n_out)},
        ]
    return rows


def summarize(rows: list[dict]) -> dict[int, dict[str, list[str]]]:
    """{에피소드: {"fail": [검사들], "warn": [검사들]}}  — fail이 비어 있으면 학습에 사용 가능"""
    out: dict[int, dict[str, list[str]]] = {}
    for r in rows:
        s = out.setdefault(r["episode_index"], {"fail": [], "warn": []})
        if r["status"] != "pass":
            s[r["status"]].append(r["check_name"])
    return out
