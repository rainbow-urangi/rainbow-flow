from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

CONN_ID = "flows_ml_db"  # 당신 환경에 맞게(기존 DAG와 동일하게)
RULE_VERSION = "v0.2"         # clean은 v0.2 기준으로만 만들기

# 완전 제거할 토큰(heartbeat)
DROP_EXACT = {
    "API:POST_/Chk/sessionchk_2xx",
}

# (선택) prefix로 제거하고 싶으면 여기에 추가
DROP_PREFIX = (
    # "API:GET_/resources/",
    # "API:GET_/static/",
)

MIN_LEN_KEEP = 1  # clean 결과 길이가 너무 짧아도 일단 저장(클러스터링에서 필터링 권장)

default_args = {"retries": 1, "retry_delay": timedelta(minutes=3)}

def _rle_dedupe(tokens: list[str]) -> list[str]:
    """연속 중복 제거"""
    out = []
    prev = None
    for t in tokens:
        if t == prev:
            continue
        out.append(t)
        prev = t
    return out

def _clean_tokens(tokens: list[str]) -> list[str]:
    # 1) heartbeat 제거
    tmp = []
    for t in tokens:
        if t in DROP_EXACT:
            continue
        if DROP_PREFIX and any(t.startswith(p) for p in DROP_PREFIX):
            continue
        tmp.append(t)

    # 2) 연속 중복 제거
    tmp = _rle_dedupe(tmp)

    return tmp

def run_build_case_sequences_clean(**_context):
    hook = MySqlHook(mysql_conn_id=CONN_ID)

    with hook.get_conn() as conn:
        cur = conn.cursor()
        # v0.2 대상만 가져오기
        cur.execute(
            """
            SELECT case_id, seq_text, seq_len
            FROM case_sequences
            WHERE activity_rule_version = %s
            """,
            (RULE_VERSION,),
        )
        rows = cur.fetchall()

    if not rows:
        print("[build_case_sequences_clean] no rows found for v0.2")
        return

    updates = []
    for (case_id, seq_text, seq_len) in rows:
        if not seq_text:
            continue

        tokens = str(seq_text).split()
        cleaned = _clean_tokens(tokens)
        if len(cleaned) < MIN_LEN_KEEP:
            cleaned = []  # 너무 짧으면 빈 값으로 저장(원하면 그대로 skip로 바꿔도 됨)

        seq_text_clean = " ".join(cleaned) if cleaned else ""
        seq_len_clean = len(cleaned)

        updates.append((seq_text_clean, seq_len_clean, int(case_id)))

    with hook.get_conn() as conn:
        cur = conn.cursor()
        cur.executemany(
            """
            UPDATE case_sequences
            SET seq_text_clean = %s,
                seq_len_clean  = %s
            WHERE case_id = %s
            """,
            updates,
        )
        conn.commit()

    print(f"[build_case_sequences_clean] updated={len(updates)} cases for rule={RULE_VERSION}")

with DAG(
    dag_id="build_case_sequences_clean",
    start_date=datetime(2025, 1, 1),
    schedule=None,   # 우선은 수동 실행 권장
    catchup=False,
    default_args=default_args,
    tags=["didimdol", "ml", "clean"],
) as dag:
    PythonOperator(task_id="run", python_callable=run_build_case_sequences_clean)