## choose the base image and the python version
FROM apache/airflow:latest

## set the user as root, helps with the installation permissions :)
USER root

## set environment varibale to avoid ui pop-ups during installations.
ENV DEBIAN_FRONTEND=noninteractive

## install JDK necessary packages in the image,
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    apt-transport-https \
    build-essential \
    gcc \
    gnupg2 \
    libssl-dev \
    libffi-dev \
    libpq-dev \
    libmariadb-dev-compat \
    libmariadb-dev \
    lsb-release \
    openjdk-17-jdk \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

## if you want to install timezone TZ library for image as well. uncomment below

## set up java home. Debian 12 bookworm comes with jdk-17 as default.
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
RUN export JAVA_HOME

## for regular apache-ariflow installation.
USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}"  \
apache-airflow-providers-apache-spark \
-r /requirements.txt 