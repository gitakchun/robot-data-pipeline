-- 파이프라인 메타데이터 스키마
-- CREATE ... IF NOT EXISTS → 몇 번 실행해도 안전 (멱등)

-- 파이프라인 실행 이력: "언제, 무엇을, 성공했나"
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id        TEXT PRIMARY KEY,
    dataset       TEXT        NOT NULL,
    revision      TEXT,                        -- HF 데이터셋 커밋 해시 (어느 버전을 처리했는지)
    status        TEXT        NOT NULL,        -- running / success / failed
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    error         TEXT
);

-- 품질 검사 결과: 에피소드 × 검사항목 1행 (long format)
--   검사 항목이 늘어나도 테이블 구조를 바꿀 필요가 없음
CREATE TABLE IF NOT EXISTS quality_results (
    run_id        TEXT        NOT NULL REFERENCES pipeline_runs(run_id),
    dataset       TEXT        NOT NULL,
    episode_index INTEGER     NOT NULL,
    check_name    TEXT        NOT NULL,        -- episode_length / missing_values / timestamp_gap / joint_range
    metric_value  DOUBLE PRECISION,            -- 측정값 (프레임 수, 결측 개수 등)
    threshold     TEXT,                        -- 판정 기준 (사람이 읽을 수 있게 문자열)
    passed        BOOLEAN     NOT NULL,
    checked_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, episode_index, check_name)
);

-- 에피소드 카탈로그: "학습에 쓸 수 있는 에피소드가 어디 있나"
CREATE TABLE IF NOT EXISTS episodes (
    dataset        TEXT    NOT NULL,
    revision       TEXT    NOT NULL,
    episode_index  INTEGER NOT NULL,
    task           TEXT,
    n_frames       INTEGER NOT NULL,
    duration_s     DOUBLE PRECISION,
    raw_prefix     TEXT    NOT NULL,           -- 원본 위치 (raw 버킷)
    curated_key    TEXT,                       -- 학습용 파일 위치 (불합격이면 NULL)
    quality_passed BOOLEAN NOT NULL,
    failed_checks  TEXT[]  NOT NULL DEFAULT '{}',
    last_run_id    TEXT    REFERENCES pipeline_runs(run_id),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset, revision, episode_index)
);

-- ---------------------------------------------------------
-- 마이그레이션 (10/20): 품질 판정 3단계(pass/warn/fail) 도입
--   CREATE TABLE IF NOT EXISTS는 이미 있는 테이블을 건드리지 않으므로,
--   컬럼 추가는 ALTER로 따로 (IF NOT EXISTS → 여러 번 실행해도 안전)
-- ---------------------------------------------------------
ALTER TABLE quality_results ADD COLUMN IF NOT EXISTS status TEXT;     -- pass / warn / fail
COMMENT ON COLUMN quality_results.passed IS 'status <> ''fail'' (학습 사용 가능 여부)';
ALTER TABLE episodes ADD COLUMN IF NOT EXISTS warnings TEXT[] NOT NULL DEFAULT '{}';

-- "품질 통과한 에피소드만" 조회가 잦을 것이므로 인덱스
CREATE INDEX IF NOT EXISTS idx_episodes_passed ON episodes (dataset, quality_passed);
