from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import pymysql


def test_db_connection():
    conn_info = BaseHook.get_connection("flows_ml_db")

    conn = pymysql.connect(
        host=conn_info.host,
        port=int(conn_info.port),
        user=conn_info.login,
        password=conn_info.password,
        database=conn_info.schema,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )

    with conn.cursor() as cursor:
        cursor.execute("SELECT 1 AS test")
        result = cursor.fetchall()
        print("DB 연결 성공:", result)

    conn.close()


with DAG(
    dag_id="test_flows_db_connection",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="test_db_connection",
        python_callable=test_db_connection,
    )