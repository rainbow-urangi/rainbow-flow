from __future__ import annotations

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from activity_rules import build_activity, ACTIVITY_RULE_VERSION

CONN_ID = "flows_ml_db"
PIPELINE = "build_event_log"
TENANT = "__ALL__"
BATCH_SIZE = 5000

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

def run_build_event_log(**_context):
    hook = MySqlHook(mysql_conn_id=CONN_ID)
    last_pk = _get_state(hook)

    max_seen = last_pk
    total = 0

    while True:
        with hook.get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                f"""
                SELECT
                  e.id AS source_event_pk,
                  e.task_id,
                  t.session_id AS session_id,
                  e.event_time AS ts,
                  e.event_type,
                  e.interaction_type,
                  e.page_url,
                  e.page_title,
                  e.api_path,
                  e.api_method,
                  e.api_status_code,
                  e.element_tag,
                  e.data_testid,
                  e.target_selector,
                  e.associated_label,
                  e.element_text,
                  s.tenant_id,
                  s.user_id
                FROM events e
                JOIN tasks t ON t.id = e.task_id
                JOIN sessions s ON s.id = t.session_id
                WHERE e.id > %s
                ORDER BY e.id ASC
                LIMIT {BATCH_SIZE}
                """,
                (last_pk,),
            )

            # ✅ tuple rows -> dict rows
            cols = [d[0] for d in cur.description]
            fetched = cur.fetchall()
            rows = [dict(zip(cols, r)) for r in fetched]

            if not rows:
                break

            insert_vals = []
            for r in rows:
                activity, l1, l2, ver = build_activity(r)
                attrs = {
                    "page_title": r.get("page_title"),
                    "element_tag": r.get("element_tag"),
                    "data_testid": r.get("data_testid"),
                }

                if r.get("session_id") is None:
                   continue

                insert_vals.append((
                    r["source_event_pk"],
                    int(r["task_id"]),                 # case_id = task_id
                    r.get("tenant_id"),
                    r["user_id"],
                    r["session_id"],
                    int(r["task_id"]),
                    r["ts"],
                    activity,
                    l1,
                    l2,
                    ver,
                    r["event_type"],
                    r.get("interaction_type"),
                    r["page_url"],
                    r.get("page_title"),
                    r.get("api_path"),
                    r.get("api_method"),
                    r.get("api_status_code"),
                    r.get("element_tag"),
                    r.get("data_testid"),
                    r.get("target_selector"),
                    r.get("associated_label"),
                    r.get("element_text"),
                    json.dumps(attrs, ensure_ascii=False),
                ))
                max_seen = max(max_seen, int(r["source_event_pk"]))

            cur2 = conn.cursor()
            cur2.executemany(
                """
                INSERT INTO event_log
                  (source_event_pk, case_id, tenant_id, user_id, session_id, task_id, ts,
                   activity, activity_l1, activity_l2, activity_rule_version,
                   event_type, interaction_type, page_url, page_title, api_path, api_method, api_status_code,
                   element_tag, data_testid, target_selector, associated_label, element_text, attrs_json)
                VALUES
                  (%s,%s,%s,%s,%s,%s,%s,
                   %s,%s,%s,%s,
                   %s,%s,%s,%s,%s,%s,%s,
                   %s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                  activity=VALUES(activity),
                  activity_l1=VALUES(activity_l1),
                  activity_l2=VALUES(activity_l2),
                  activity_rule_version=VALUES(activity_rule_version),
                  attrs_json=VALUES(attrs_json)
                """,
                insert_vals
            )
            conn.commit()

            total += len(rows)
            last_pk = max_seen

    _set_state(hook, max_seen)
    print(f"[build_event_log] rows={total}, last_event_pk={max_seen}, rule={ACTIVITY_RULE_VERSION}")

with DAG(
    dag_id="build_event_log",
    start_date=datetime(2025, 1, 1),
    schedule="*/10 * * * *",  # 10분마다
    catchup=False,
    default_args=default_args,
    tags=["didimdol", "etl"],
) as dag:
    PythonOperator(task_id="run", python_callable=run_build_event_log)