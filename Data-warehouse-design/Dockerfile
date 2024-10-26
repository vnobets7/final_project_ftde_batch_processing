## choose the base image and the python version
FROM apache/airflow:latest

# Install additional dependencies
USER root
COPY requirements.txt ./requirements.txt
# for regular apache-ariflow installation.
USER airflow
# Set up additional Python dependencies
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}"  \
-r /requirements.txt 