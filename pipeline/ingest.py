"""① 수집: Hugging Face 데이터셋 → MinIO raw 버킷

저장 경로: raw/{dataset}/{revision 앞 8자리}/data/..., meta/..., _manifest.json
- revision(커밋 해시)으로 폴더를 나눔 → 원본이 업데이트돼도 예전 버전이 보존되고, 어떤 버전으로 학습했는지 추적 가능
- _manifest.json은 '업로드 완료 도장'. 모든 파일을 올린 뒤 **마지막에** 씀
  → 중간에 실패하면 manifest가 없으므로, 재시도 때 처음부터 다시 올림
  → manifest가 있으면 이미 수집된 것이므로 건너뜀 (불필요한 재다운로드 방지)
"""
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from huggingface_hub import HfApi, snapshot_download

from pipeline import config, faults, storage

ALLOW_PATTERNS = ["data/*", "meta/*"]  # 영상(videos/)은 제외


def raw_prefix(dataset: str, revision: str) -> str:
    return f"{dataset}/{revision[:8]}/"


def ingest(repo_id: str, inject_faults: bool = False) -> dict:
    dataset = repo_id.split("/")[1] + ("-dirty" if inject_faults else "")
    revision = HfApi().dataset_info(repo_id).sha  # 현재 최신 커밋 해시
    prefix = raw_prefix(dataset, revision)
    manifest_key = prefix + "_manifest.json"
    result = {"repo_id": repo_id, "dataset": dataset, "revision": revision, "raw_prefix": prefix}

    if storage.exists(config.RAW_BUCKET, manifest_key):
        print(f"[ingest] 이미 수집됨, 건너뜀: raw/{prefix}")
        return result | {"skipped": True}

    with tempfile.TemporaryDirectory() as tmp:  # 블록이 끝나면 임시 폴더 자동 삭제
        local = Path(tmp)
        snapshot_download(repo_id, repo_type="dataset", revision=revision,
                          allow_patterns=ALLOW_PATTERNS, local_dir=local)
        if inject_faults:
            faults.inject(local)

        files = [f for f in sorted(local.rglob("*")) if f.is_file() and ".cache" not in f.parts]
        for f in files:
            storage.upload_file(f, config.RAW_BUCKET, prefix + f.relative_to(local).as_posix())

        manifest = {
            "repo_id": repo_id,
            "revision": revision,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "files": [f.relative_to(local).as_posix() for f in files],
            "faults_injected": [f"ep{e} {kind}: {desc}" for e, kind, desc in faults.FAULTS] if inject_faults else [],
        }
        storage.put_bytes(json.dumps(manifest, ensure_ascii=False, indent=2).encode(),
                          config.RAW_BUCKET, manifest_key)

    print(f"[ingest] {len(files)}개 파일 업로드 → raw/{prefix}")
    return result | {"skipped": False}
