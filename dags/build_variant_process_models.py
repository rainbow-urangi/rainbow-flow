from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook


CONN_ID = "flows_ml_db"
DST_DB = "flows_ml_db"

RULE_VERSION = "v0.2"
CLUSTER_MODEL_VERSION = "baseline_hdbscan_v0.2_clean_20260423"

# variant 품질을 위해 너무 작은 군집은 제외
MIN_CASES_PER_VARIANT = 20

default_args = {"retries": 1, "retry_delay": timedelta(minutes=3)}


def _md5_16(s: str) -> bytes:
    return hashlib.md5(s.encode("utf-8")).digest()


def _get_columns(hook: MySqlHook, table: str) -> list[str]:
    with hook.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
            ORDER BY ordinal_position
            """,
            (DST_DB, table),
        )
        return [r[0] for r in cur.fetchall()]


def _insert_process_model(hook: MySqlHook, model_name: str, params: dict) -> int:
    cols = _get_columns(hook, "process_models")

    # 가능한 컬럼 후보들(있는 것만 사용)
    payload = {}
    if "model_name" in cols:
        payload["model_name"] = model_name
    if "activity_rule_version" in cols:
        payload["activity_rule_version"] = RULE_VERSION
    if "model_version" in cols:
        payload["model_version"] = CLUSTER_MODEL_VERSION
    if "params_json" in cols:
        payload["params_json"] = json.dumps(params, ensure_ascii=False)
    if "created_at" in cols:
        # created_at에 default가 없을 때 대비
        payload["created_at"] = datetime.utcnow()

    if not payload:
        raise RuntimeError("process_models has no insertable columns (unexpected schema).")

    keys = list(payload.keys())
    placeholders = ", ".join(["%s"] * len(keys))
    col_sql = ", ".join(keys)
    sql = f"INSERT INTO {DST_DB}.process_models ({col_sql}) VALUES ({placeholders})"

    with hook.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, [payload[k] for k in keys])
        conn.commit()
        return int(cur.lastrowid)


def _bulk_insert_nodes(hook: MySqlHook, model_id: int, node_counts: Counter):
    cols = _get_columns(hook, "process_nodes")

    # 후보 컬럼 매핑
    # - activity: 토큰 문자열
    # - activity_hash / node_hash: BINARY(16) 해시
    # - freq / count: 빈도
    col_activity = "activity" if "activity" in cols else None
    col_hash = "activity_hash" if "activity_hash" in cols else ("node_hash" if "node_hash" in cols else None)
    col_freq = "freq" if "freq" in cols else ("count" if "count" in cols else None)

    insert_cols = ["model_id"]
    if col_activity:
        insert_cols.append(col_activity)
    if col_hash:
        insert_cols.append(col_hash)
    if col_freq:
        insert_cols.append(col_freq)

    if len(insert_cols) < 2:
        raise RuntimeError("process_nodes schema not supported: need at least activity/hash/freq columns.")

    sql = f"""
      INSERT INTO {DST_DB}.process_nodes ({", ".join(insert_cols)})
      VALUES ({", ".join(["%s"] * len(insert_cols))})
    """

    vals = []
    for act, cnt in node_counts.items():
        row = [model_id]
        if col_activity:
            row.append(act)
        if col_hash:
            row.append(_md5_16(act))
        if col_freq:
            row.append(int(cnt))
        vals.append(tuple(row))

    with hook.get_conn() as conn:
        cur = conn.cursor()
        cur.executemany(sql, vals)
        conn.commit()


def _bulk_insert_edges(hook: MySqlHook, model_id: int, edge_counts: Counter):
    cols = _get_columns(hook, "process_edges")

    col_from = "from_activity" if "from_activity" in cols else None
    col_to = "to_activity" if "to_activity" in cols else None
    col_from_hash = "from_hash" if "from_hash" in cols else None
    col_to_hash = "to_hash" if "to_hash" in cols else None
    col_freq = "freq" if "freq" in cols else ("count" if "count" in cols else None)
    col_prob = "prob" if "prob" in cols else None

    # 확률 계산용 out-degree
    out_sum = defaultdict(int)
    for (a, b), c in edge_counts.items():
        out_sum[a] += c

    insert_cols = ["model_id"]
    if col_from:
        insert_cols.append(col_from)
    if col_to:
        insert_cols.append(col_to)
    if col_from_hash:
        insert_cols.append(col_from_hash)
    if col_to_hash:
        insert_cols.append(col_to_hash)
    if col_freq:
        insert_cols.append(col_freq)
    if col_prob:
        insert_cols.append(col_prob)

    if len(insert_cols) < 3:
        raise RuntimeError("process_edges schema not supported.")

    sql = f"""
      INSERT INTO {DST_DB}.process_edges ({", ".join(insert_cols)})
      VALUES ({", ".join(["%s"] * len(insert_cols))})
    """

    vals = []
    for (a, b), c in edge_counts.items():
        row = [model_id]
        if col_from:
            row.append(a)
        if col_to:
            row.append(b)
        if col_from_hash:
            row.append(_md5_16(a))
        if col_to_hash:
            row.append(_md5_16(b))
        if col_freq:
            row.append(int(c))
        if col_prob:
            p = float(c) / float(out_sum[a]) if out_sum[a] else 0.0
            row.append(p)
        vals.append(tuple(row))

    with hook.get_conn() as conn:
        cur = conn.cursor()
        cur.executemany(sql, vals)
        conn.commit()


def run_build_variant_process_models(**_context):
    hook = MySqlHook(mysql_conn_id=CONN_ID)

    # 1) variant별 case count 확인(-1 제외)
    with hook.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT variant_id, COUNT(*)
            FROM {DST_DB}.case_clusters
            WHERE model_version=%s
              AND variant_id <> -1
            GROUP BY variant_id
            ORDER BY COUNT(*) DESC
            """,
            (CLUSTER_MODEL_VERSION,),
        )
        variants = [(int(v), int(n)) for (v, n) in cur.fetchall()]

    variants = [(v, n) for (v, n) in variants if n >= MIN_CASES_PER_VARIANT]
    if not variants:
        print("[build_variant_process_models] no variants over threshold.")
        return

    print(f"[build_variant_process_models] variants={variants}")

    # 2) variant별로 시퀀스 읽어서 node/edge 집계
    for variant_id, n_cases in variants:
        with hook.get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                f"""
                SELECT cs.case_id, cs.seq_text_clean
                FROM {DST_DB}.case_sequences cs
                JOIN {DST_DB}.case_clusters cc ON cc.case_id = cs.case_id
                WHERE cc.model_version=%s
                  AND cc.variant_id=%s
                  AND cs.activity_rule_version=%s
                  AND cs.seq_text_clean IS NOT NULL
                  AND cs.seq_text_clean <> ''
                  AND cs.seq_len_clean IS NOT NULL
                  AND cs.seq_len_clean >= 3
                """,
                (CLUSTER_MODEL_VERSION, variant_id, RULE_VERSION),
            )
            rows = cur.fetchall()

        # 집계
        node_counts = Counter()
        edge_counts = Counter()

        for case_id, seq_text_clean in rows:
            toks = str(seq_text_clean).split()
            if len(toks) < 2:
                continue
            node_counts.update(toks)
            for a, b in zip(toks[:-1], toks[1:]):
                edge_counts[(a, b)] += 1

        if not node_counts:
            print(f"[variant={variant_id}] empty after filtering. skip.")
            continue

        # 3) process_models insert
        model_name = f"VariantGraph {CLUSTER_MODEL_VERSION} (variant={variant_id}, cases={n_cases})"
        params = {
            "cluster_model_version": CLUSTER_MODEL_VERSION,
            "variant_id": variant_id,
            "n_cases": n_cases,
            "rule_version": RULE_VERSION,
            "source": "case_sequences.seq_text_clean",
        }
        model_id = _insert_process_model(hook, model_name, params)

        # 4) nodes/edges insert
        _bulk_insert_nodes(hook, model_id, node_counts)
        _bulk_insert_edges(hook, model_id, edge_counts)

        print(f"[variant={variant_id}] model_id={model_id} nodes={len(node_counts)} edges={len(edge_counts)}")


with DAG(
    dag_id="build_variant_process_models",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # 수동 실행 권장(먼저 검증)
    catchup=False,
    default_args=default_args,
    tags=["didimdol", "ml", "process"],
) as dag:
    PythonOperator(task_id="run", python_callable=run_build_variant_process_models)