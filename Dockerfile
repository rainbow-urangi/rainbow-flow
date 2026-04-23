FROM apache/airflow:2.9.3

USER airflow
COPY requirements.txt /requirements.txt
RUN python -m pip install --no-cache-dir -r /requirements.txt