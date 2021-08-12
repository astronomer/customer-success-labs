FROM quay.io/astronomer/ap-airflow:2.1.1-buster-onbuild

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         gcc \
         g++ \
         unixodbc-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN pip install apache-airflow-providers-odbc