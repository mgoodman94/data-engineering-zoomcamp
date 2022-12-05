FROM apache/airflow:2.4.1

ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY google_credentials.json .
ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/google_credentials.json

WORKDIR $AIRFLOW_HOME

USER root
COPY scripts scripts
RUN chmod +x scripts

USER $AIRFLOW_UID