FROM apache/airflow:2.3.3
USER root
RUN apt-get update 
RUN apt-get install -y libreoffice

USER airflow
RUN pip install --upgrade pip
RUN pip install boto3 pandas xlrd
