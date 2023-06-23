FROM apache/airflow:2.4.1

ENV AIRFLOW_HOME=/opt/airflow
USER root
RUN apt-get update -qq && apt-get install vim -qqq
USER $AIRFLOW_UID
COPY requirements.txt .
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
