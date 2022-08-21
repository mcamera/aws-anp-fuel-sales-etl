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

# Apply some transformations
oil_fuels_sales = (
    oil_fuels_sales
    .withColumn('product', F.col('COMBUSTÍVEL'))   
    .withColumn('unit', F.substring(F.col('product'), -3, 2))  # gets the unit
    .withColumn('product', F.expr("substring(product, 1, length(product)-5)"))  # removes the unit from the product name
    .withColumn('year', F.col('ANO').cast(IntegerType()))
    .withColumn('uf', F.col('ESTADO'))
    .withColumn('regiao', F.col('REGIÃO'))
    .withColumn('jan', F.col('Jan').cast(DoubleType()))
    .withColumn('fev', F.col('Fev').cast(DoubleType()))
    .withColumn('mar', F.col('Mar').cast(DoubleType()))
    .withColumn('abr', F.col('Abr').cast(DoubleType()))
    .withColumn('mai', F.col('Mai').cast(DoubleType()))
    .withColumn('jun', F.col('Jun').cast(DoubleType()))
    .withColumn('jul', F.col('Jul').cast(DoubleType()))
    .withColumn('ago', F.col('Ago').cast(DoubleType()))
    .withColumn('set', F.col('Set').cast(DoubleType()))
    .withColumn('out', F.col('Out').cast(DoubleType()))
    .withColumn('nov', F.col('Nov').cast(DoubleType()))
    .withColumn('dez', F.col('Dez').cast(DoubleType()))
    .withColumn('total', F.round(F.col('TOTAL').cast(DoubleType()),3))
    .drop('COMBUSTÍVEL', 'ANO', 'ESTADO','REGIÃO')
)

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
