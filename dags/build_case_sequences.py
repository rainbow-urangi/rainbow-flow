from __future__ import annotations

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

CONN_ID = "flows_ml_db"
PIPELINE = "build_case_sequences"
TENANT = "__ALL__"  # 단일 테넌트면 그대로 사용
BATCH_CASES = 500   # 한번에 처리할 case 수(조절 가능)

default_args = {"retries": 2, "retry_delay": timedelta(minutes=3)}


def _get_state(hook: MySqlHook) -> int:
    with hook.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT last_event_pk FROM etl_state WHERE pipeline_name=%s AND tenant_id=%s",
            (PIPELINE, TENANT),
        )
        row = cur.fetchone()
        if row:
            return int(row[0])
        cur.execute(
            "INSERT INTO etl_state(pipeline_name, tenant_id, last_event_pk) VALUES (%s,%s,0)",
            (PIPELINE, TENANT),
        )
        conn.commit()
        return 0


def _set_state(hook: MySqlHook, last_pk: int) -> None:
    with hook.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO etl_state(pipeline_name, tenant_id, last_event_pk)
               VALUES (%s,%s,%s)
               ON DUPLICATE KEY UPDATE last_event_pk=VALUES(last_event_pk)""",
            (PIPELINE, TENANT, last_pk),
        )
        conn.commit()


def run_build_case_sequences(**_context):
    hook = MySqlHook(mysql_conn_id=CONN_ID)
    last_pk = _get_state(hook)

    # 1) 이번 증분에서 영향을 받은 case_id 목록(새로 들어온 event_log 기준)
    with hook.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT task_id AS case_id
            FROM event_log
            WHERE source_event_pk > %s
            ORDER BY case_id ASC
            """,
            (last_pk,),
        )
        case_ids = [int(r[0]) for r in cur.fetchall()]

        # 새로 들어온 게 없으면 종료
        if not case_ids:
            print(f"[build_case_sequences] no new event_log after {last_pk}")
            return

        # 이번 실행에서 가장 큰 source_event_pk 업데이트
        cur.execute("SELECT MAX(source_event_pk) FROM event_log")
        max_pk_row = cur.fetchone()
        max_pk = int(max_pk_row[0] or last_pk)

    # 2) case_id들을 배치로 쪼개서 각 case의 전체 시퀀스를 재계산(upsert)
    total_upsert = 0
    for i in range(0, len(case_ids), BATCH_CASES):
        chunk = case_ids[i:i + BATCH_CASES]
        placeholders = ",".join(["%s"] * len(chunk))

        with hook.get_conn() as conn:
            cur = conn.cursor()

            # case별 event_log 전체 시퀀스 조회
            cur.execute(
                f"""
                SELECT
                  task_id AS case_id,
                  tenant_id,
                  user_id,
                  ts,
                  activity,
                  activity_rule_version
                FROM event_log
                WHERE task_id IN ({placeholders})
                ORDER BY case_id ASC, ts ASC
                """,
                chunk,
            )
            cols = [d[0] for d in cur.description]
            fetched = cur.fetchall()
            rows = [dict(zip(cols, r)) for r in fetched]

            # case별 집계
            current_case = None
            acc = []
            tenant_id = None
            user_id = None
            start_ts = None
            end_ts = None
            rule_ver = None

            upserts = []

            def flush():
                nonlocal total_upsert
                if current_case is None or not acc:
                    return
                seq_text = " ".join(acc)
                seq_json = json.dumps(acc, ensure_ascii=False)
                upserts.append((
                    current_case, tenant_id, user_id,
                    len(acc), start_ts, end_ts,
                    seq_text, seq_json, rule_ver
                ))

            for r in rows:
                cid = int(r["case_id"])
                if current_case is None:
                    current_case = cid
                    tenant_id = r.get("tenant_id")
                    user_id = r.get("user_id")
                    start_ts = r["ts"]
                    end_ts = r["ts"]
                    rule_ver = r.get("activity_rule_version") or "unknown"
                    acc = []

                if cid != current_case:
                    flush()
                    # reset
                    current_case = cid
                    tenant_id = r.get("tenant_id")
                    user_id = r.get("user_id")
                    start_ts = r["ts"]
                    end_ts = r["ts"]
                    rule_ver = r.get("activity_rule_version") or "unknown"
                    acc = []

                acc.append(r["activity"])
                end_ts = r["ts"]

            flush()

            if upserts:
                cur.executemany(
                    """
                    INSERT INTO case_sequences
                      (case_id, tenant_id, user_id, seq_len, start_ts, end_ts,
                       seq_text, seq_json, activity_rule_version)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      tenant_id=VALUES(tenant_id),
                      user_id=VALUES(user_id),
                      seq_len=VALUES(seq_len),
                      start_ts=VALUES(start_ts),
                      end_ts=VALUES(end_ts),
                      seq_text=VALUES(seq_text),
                      seq_json=VALUES(seq_json),
                      activity_rule_version=VALUES(activity_rule_version)
                    """,
                    upserts,
                )
                conn.commit()
                total_upsert += len(upserts)

    _set_state(hook, max_pk)
    print(f"[build_case_sequences] cases_upserted={total_upsert}, last_event_pk={max_pk}")


with DAG(
    dag_id="build_case_sequences",
    start_date=datetime(2025, 1, 1),
    schedule="*/30 * * * *",  # 30분마다(원하면 10분/1시간으로 조정)
    catchup=False,
    default_args=default_args,
    tags=["didimdol", "ml", "etl"],
) as dag:
    PythonOperator(task_id="run", python_callable=run_build_case_sequences)