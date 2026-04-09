from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from activity_rules import ACTIVITY_RULE_VERSION

CONN_ID = "flows_ml_db"
#LOOKBACK_HOURS = 48
LOOKBACK_HOURS = 24 * 365  # 1년

default_args = {"retries": 2, "retry_delay": timedelta(minutes=3)}

def run_build_cases(**_context):
    hook = MySqlHook(mysql_conn_id=CONN_ID)
    with hook.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            INSERT INTO cases
              (case_id, tenant_id, user_id, session_id, task_id,
               start_time, end_time, duration_ms,
               event_count, unique_activities, unique_pages, api_error_count,
               activity_rule_version)
            SELECT
              t.id AS case_id,
              s.tenant_id,
              s.user_id,
              t.session_id,
              t.id AS task_id,
              t.start_time,
              t.end_time,
              t.duration_ms,
              COUNT(el.source_event_pk) AS event_count,
              COUNT(DISTINCT el.activity) AS unique_activities,
              COUNT(DISTINCT el.page_url) AS unique_pages,
              SUM(CASE WHEN el.api_status_code >= 400 THEN 1 ELSE 0 END) AS api_error_count,
              %s AS activity_rule_version
            FROM tasks t
            JOIN sessions s ON s.id = t.session_id
            LEFT JOIN event_log el ON el.task_id = t.id
            WHERE t.start_time >= (UTC_TIMESTAMP(6) - INTERVAL {LOOKBACK_HOURS} HOUR)
            GROUP BY t.id, s.tenant_id, s.user_id, t.session_id, t.start_time, t.end_time, t.duration_ms
            ON DUPLICATE KEY UPDATE
              tenant_id=VALUES(tenant_id),
              user_id=VALUES(user_id),
              session_id=VALUES(session_id),
              start_time=VALUES(start_time),
              end_time=VALUES(end_time),
              duration_ms=VALUES(duration_ms),
              event_count=VALUES(event_count),
              unique_activities=VALUES(unique_activities),
              unique_pages=VALUES(unique_pages),
              api_error_count=VALUES(api_error_count),
              activity_rule_version=VALUES(activity_rule_version)
            """,
            (ACTIVITY_RULE_VERSION,),
        )
        conn.commit()
    print(f"[build_cases] lookback={LOOKBACK_HOURS}h rule={ACTIVITY_RULE_VERSION}")

with DAG(
    dag_id="build_cases",
    start_date=datetime(2025, 1, 1),
    schedule="0 * * * *",  # 매시간
    catchup=False,
    default_args=default_args,
    tags=["didimdol", "etl"],
) as dag:
    PythonOperator(task_id="run", python_callable=run_build_cases)