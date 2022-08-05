import logging
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

# Create Spark Session object
spark = (
    SparkSession.builder.appName("ANP")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# Read bronze data
logging.info("Read bronze layer...")
oil_fuels_sales = (
    spark.read
    .format("parquet")
    .option("inferSchema", False)
    .load("s3://anp-bronze/oil_fuels_sales/")
)

# TODO: TRANSFORM DATA
# Apply some transformations
# The primary source (Excel sheet) has performed data in integer type. Because of this, we will cast that columns in integer.
oil_fuels_sales = (
    oil_fuels_sales
    .withColumn('product', F.col('COMBUSTÍVEL'))   
    .withColumn('unit', F.substring(F.col('product'), -3, 2))  # gets the unit
    .withColumn('product', F.expr("substring(product, 1, length(product)-5)"))  # removes the unit from the product name
    .withColumn('year', F.col('ANO'))
    .withColumn('uf', F.col('ESTADO'))
    .withColumn('jan', F.ceil(F.col('Jan')).cast(IntegerType()))
    .withColumn('fev', F.ceil(F.col('Fev')).cast(IntegerType()))
    .withColumn('mar', F.ceil(F.col('Mar')).cast(IntegerType()))
    .withColumn('abr', F.ceil(F.col('Abr')).cast(IntegerType()))
    .withColumn('mai', F.ceil(F.col('Mai')).cast(IntegerType()))
    .withColumn('jun', F.ceil(F.col('Jun')).cast(IntegerType()))
    .withColumn('jul', F.ceil(F.col('Jul')).cast(IntegerType()))
    .withColumn('ago', F.ceil(F.col('Ago')).cast(IntegerType()))
    .withColumn('set', F.ceil(F.col('Set')).cast(IntegerType()))
    .withColumn('out', F.ceil(F.col('Out')).cast(IntegerType()))
    .withColumn('nov', F.ceil(F.col('Nov')).cast(IntegerType()))
    .withColumn('dez', F.ceil(F.col('Dez')).cast(IntegerType()))
    .withColumn('TOTAL_tmp', F.ceil(F.col('TOTAL')).cast(IntegerType()))
    .drop('COMBUSTÍVEL', 'ANO', 'ESTADO', 'REGIÃO', 'TOTAL')
)

# I've noted that the data is shifted. The total column doesn't have the real total count. Maybe it happened due to the file conversion process.
# Trying to fix the table. It's not right yet.

oil_fuels_sales = (
    oil_fuels_sales
    .withColumn('1', F.col('fev'))
    .withColumn('2', F.col('mar'))
    .withColumn('3', F.col('abr'))
    .withColumn('4', F.col('mai'))
    .withColumn('5', F.col('jun'))
    .withColumn('6', F.col('jul'))
    .withColumn('7', F.col('ago'))
    .withColumn('8', F.col('set'))
    .withColumn('9', F.col('out'))
    .withColumn('10', F.col('nov'))
    .withColumn('11', F.col('dez'))
    .withColumn('12', F.col('TOTAL_tmp'))
    .withColumn('total', F.col('jan'))
    .drop('jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez', 'TOTAL_tmp')
)

# TODO PIVOT TABLE
# TODO: APPLY SCHEMA
#  root
#  |-- year_month: date
#  |-- uf: string
#  |-- product: string
#  |-- unit: string
#  |-- volume: double
#  |-- created_at: timestamp

# Write table in silver layer with delta format
logging.info("Writing delta table into the silver layer...")
(
    oil_fuels_sales
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("year")
    .save("s3://anp-silver/oil_fuels_sales/")
)

logging.info("Process finished.")
