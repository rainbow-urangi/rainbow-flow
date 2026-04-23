from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook


CONN_ID = "flows_ml_db"
DST_DB = "flows_ml_db"

MODEL_VERSION = "baseline_hdbscan_v0.2_clean_20260423"
RULE_VERSION = "v0.2"

# clean 기준 필터(추천)
MIN_LEN_CLEAN = 5

# HDBSCAN 파라미터(초기값)
MIN_CLUSTER_SIZE = 20
MIN_SAMPLES = 5

default_args = {"retries": 1, "retry_delay": timedelta(minutes=3)}


def run_cluster_cases(**_context):
    # ✅ import는 함수 내부로 두면, 혹시 환경 흔들릴 때 Broken DAG 방지에도 도움
    from sklearn.feature_extraction.text import TfidfVectorizer
    import hdbscan
    import numpy as np

    hook = MySqlHook(mysql_conn_id=CONN_ID)

    # 1) clean 시퀀스 읽기
    with hook.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT case_id, seq_len_clean, seq_text_clean
            FROM {DST_DB}.case_sequences
            WHERE activity_rule_version = %s
              AND seq_len_clean >= %s
              AND seq_text_clean IS NOT NULL
              AND seq_text_clean <> ''
            """,
            (RULE_VERSION, MIN_LEN_CLEAN),
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    if not rows:
        print("[cluster_cases_hdbscan] no rows. check seq_text_clean/seq_len_clean")
        return

    case_ids = [int(r["case_id"]) for r in rows]
    texts = [str(r["seq_text_clean"]) for r in rows]

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
        metric="cosine",
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
    print(f"[cluster_cases_hdbscan] model={MODEL_VERSION} total={n_total} clusters={n_clusters} noise={n_noise}")


with DAG(
    dag_id="cluster_cases_hdbscan",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # 수동 실행 권장
    catchup=False,
    default_args=default_args,
    tags=["didimdol", "ml"],
) as dag:
    PythonOperator(task_id="run", python_callable=run_cluster_cases)