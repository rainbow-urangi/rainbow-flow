from __future__ import annotations

from datetime import datetime, timedelta
from collections import Counter, defaultdict
import json
import hashlib

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from activity_rules import ACTIVITY_RULE_VERSION

CONN_ID = "flows_ml_db"
#LOOKBACK_HOURS = 48
LOOKBACK_HOURS = 24 * 365
TOP_K_EDGES = 3000

default_args = {"retries": 2, "retry_delay": timedelta(minutes=3)}

def md5_16(s: str) -> bytes:
    return hashlib.md5(s.encode("utf-8")).digest()  # 16 bytes

def run_build_process_model(**_context):
    hook = MySqlHook(mysql_conn_id=CONN_ID)

    with hook.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT tenant_id, case_id, ts, activity
            FROM event_log
            WHERE ts >= (UTC_TIMESTAMP(6) - INTERVAL {LOOKBACK_HOURS} HOUR)
              AND activity_rule_version = %s
            ORDER BY tenant_id, case_id, ts ASC
            """,
            (ACTIVITY_RULE_VERSION,),
        )
        cols = [d[0] for d in cur.description]
        fetched = cur.fetchall()
        rows = [dict(zip(cols, r)) for r in fetched]

    seqs = defaultdict(lambda: defaultdict(list))
    for r in rows:
        tenant = r.get("tenant_id") or "__NO_TENANT__"
        seqs[tenant][int(r["case_id"])].append(r["activity"])

    for tenant, cases in seqs.items():
        node_counter = Counter()
        edge_counter = Counter()

        for _, seq in cases.items():
            if not seq:
                continue
            for a in seq:
                node_counter[a] += 1
            for i in range(len(seq) - 1):
                edge_counter[(seq[i], seq[i+1])] += 1

        most_common_edges = edge_counter.most_common(TOP_K_EDGES)

        with hook.get_conn() as conn:
            cur = conn.cursor()

            params = {"lookback_hours": LOOKBACK_HOURS, "top_k_edges": TOP_K_EDGES}
            cur.execute(
                """
                INSERT INTO process_models(tenant_id, model_name, method, params_json, activity_rule_version)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (tenant, "MVP Transition Graph", "freq_transition", json.dumps(params), ACTIVITY_RULE_VERSION),
            )
            model_id = cur.lastrowid

            # nodes (hash 기반)
            node_rows = []
            for act, freq in node_counter.items():
                node_rows.append((model_id, md5_16(act), act, int(freq)))
            if node_rows:
                cur.executemany(
                    "INSERT INTO process_nodes(model_id, activity_hash, activity, freq) VALUES (%s,%s,%s,%s)",
                    node_rows
                )

            # edges
            out_sum = defaultdict(int)
            for (a, b), f in most_common_edges:
                out_sum[a] += f

            edge_rows = []
            for (a, b), f in most_common_edges:
                prob = float(f) / float(out_sum[a]) if out_sum[a] else 0.0
                edge_rows.append((model_id, md5_16(a), md5_16(b), a, b, int(f), prob))

            if edge_rows:
                cur.executemany(
                    """
                    INSERT INTO process_edges
                      (model_id, from_hash, to_hash, from_activity, to_activity, freq, prob)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    """,
                    edge_rows
                )

            conn.commit()

    print(f"[build_process_model] tenants={len(seqs)} rule={ACTIVITY_RULE_VERSION}")

with DAG(
    dag_id="build_process_model",
    start_date=datetime(2025, 1, 1),
    schedule="15 2 * * *",  # 매일 02:15 UTC
    catchup=False,
    default_args=default_args,
    tags=["didimdol", "ml", "process"],
) as dag:
    PythonOperator(task_id="run", python_callable=run_build_process_model)