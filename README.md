# aws-anp-fuel-sales-etl


> #### :rotating_light: ALERT! :rotating_light:
> This project is still in development!


# Objective

This ETL pipeline aims to extract the internal pivot caches from consolidated reports made available by Brazilian government's regulatory agency for oil/fuels, ANP (Agência Nacional do Petróleo, Gás Natural e Biocombustíveis), and serve the data to be consumed by the users.

The source data could be found in this [link](https://github.com/raizen-analytics/data-engineering-test/raw/master/assets/vendas-combustiveis-m3.xls), and the main information to be extracted are:
- Sales of oil derivative fuels by UF and product
- Sales of diesel by UF and type

# Architecture
<img src="./git_sources/anp-architecture.excalidraw.png" alt="">

This project uses AWS Cloud applications. All the steps are orchestrated by Airflow (deployed in an EC2 machine).

## The pipeline
1. The Python Operator is called to download the Excel file and convert it to enable access to information about the internal dynamic caches. The tables are extracted, transformed in parquet files and stored at the Bronze bucket.

2. The EMR Spark application is called to process the table sales_oil_fuels. The data is transformed, the schema is applyed and the data is stored in the silver layer.

3. The EMR Spark application is called to process the table sales_diesel. The data is transformed, the schema is applyed and the data is stored in the silver layer.

TODO: To add more information about the remaining steps.

# Requisites for the deploy
- AWS CLI (in your local machine)
- Terraform (in your local machine)
- Docker (in the EC2 virtual machine)
- Docker-compose (in the EC2 virtual machine)

# Deploy
TODO: To add informations about the architecture deploy.

# Data problem

It was notice that the data was shifted after the file conversion proccessing. See bellow what's happening.

### After load the raw data, it's clear that the total doesn't match with the sum of columns.
<img src="./git_sources/data_shift_problem1.jpg" alt="">

### Here, we can see the data shifted:
<img src="./git_sources/data_shift_problem2.jpg" alt="">


### This is the result that I have at this moment. I've just fixed the first row, but the problem still happens  with the others.
<img src="./git_sources/data_shift_almost_resolved.jpg" alt="">

Now, I'm searching for a better way to fix the entire data. After that, I have plans to add a task in the Airflow to validate the data using the Great Expectations.
