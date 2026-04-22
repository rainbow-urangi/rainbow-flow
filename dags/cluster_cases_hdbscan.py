from __future__ import annotations

from datetime import datetime, timedelta
import numpy as np
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from sklearn.feature_extraction.text import TfidfVectorizer
import hdbscan

CONN_ID = "flows_ml_db"   # build_event_log과 통일 (현재 파일 기준)
DST_DB = "flows_ml_db"    # case_sequences, case_clusters가 있는 DB
MODEL_VERSION = "baseline_hdbscan_v0.2"

MIN_LEN = 5
MAX_LEN = 200
MIN_CLUSTER_SIZE = 40
MIN_SAMPLES = 10

default_args = {"retries": 1, "retry_delay": timedelta(minutes=3)}

def _trim_seq_text(s: str) -> str:
    toks = s.split()
    if len(toks) <= MAX_LEN:
        return s
    head = toks[:120]
    tail = toks[-80:]
    return " ".join(head + ["<...>"] + tail)

def run_cluster_cases(**_context):
    hook = MySqlHook(mysql_conn_id=CONN_ID)

    # 1) case_sequences 읽기 (tuple 커서 + dict 변환)
    with hook.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT case_id, seq_len, seq_text
            FROM {DST_DB}.case_sequences
            WHERE activity_rule_version = %s
              AND seq_len >= %s
            """,
            ("v0.2", MIN_LEN),
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    if not rows:
        print("[cluster_cases] no rows. check case_sequences / rule_version / seq_len filter.")
        return

    case_ids = [int(r["case_id"]) for r in rows]
    texts = [_trim_seq_text(str(r["seq_text"])) for r in rows]

    # 2) TF-IDF
    vec = TfidfVectorizer(
        analyzer="word",
        ngram_range=(1, 2),
        min_df=2,
        max_df=0.95,
    )
    X = vec.fit_transform(texts)

    # 3) HDBSCAN
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=MIN_CLUSTER_SIZE,
        min_samples=MIN_SAMPLES,
        metric="euclidean",
        cluster_selection_method="eom",
    )
    labels = clusterer.fit_predict(X)
    probs = getattr(clusterer, "probabilities_", None)

    # 4) case_clusters upsert
    upserts = []
    for i, cid in enumerate(case_ids):
        vid = int(labels[i])
        score = float(probs[i]) if probs is not None else None
        upserts.append((cid, MODEL_VERSION, vid, score))

    with hook.get_conn() as conn:
        cur = conn.cursor()
        cur.executemany(
            f"""
            INSERT INTO {DST_DB}.case_clusters(case_id, model_version, variant_id, score)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              variant_id=VALUES(variant_id),
              score=VALUES(score)
            """,
            upserts,
        )
        conn.commit()

    n_total = len(case_ids)
    n_noise = int(np.sum(labels == -1))
    n_clusters = len(set(labels)) - (1 if -1 in set(labels) else 0)
    print(f"[cluster_cases] model={MODEL_VERSION} total={n_total} clusters={n_clusters} noise={n_noise}")

with DAG(
    dag_id="cluster_cases_hdbscan",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # 먼저는 수동 실행 권장
    catchup=False,
    default_args=default_args,
    tags=["didimdol", "ml"],
) as dag:
    PythonOperator(task_id="run", python_callable=run_cluster_cases)