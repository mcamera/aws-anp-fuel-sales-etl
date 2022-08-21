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
logging.info("Read from the bronze layer...")
diesel_sales = (
    spark.read
    .format("parquet")
    .option("inferSchema", False)
    .load("s3://anp-bronze/diesel_sales/")
)

# Apply some transformations
diesel_sales = (
    diesel_sales
    .withColumn('product', F.col('COMBUSTÍVEL'))   
    .withColumn('unit', F.substring(F.col('product'), -3, 2))  # gets the unit
    .withColumn('product', F.expr("substring(product, 1, length(product)-5)"))  # removes the unit from the product name
    .withColumn('year', F.col('ANO').cast(IntegerType()))
    .withColumn('uf', F.col('ESTADO'))
    .withColumn('1', F.col('Jan').cast(DoubleType()))
    .withColumn('2', F.col('Fev').cast(DoubleType()))
    .withColumn('3', F.col('Mar').cast(DoubleType()))
    .withColumn('4', F.col('Abr').cast(DoubleType()))
    .withColumn('5', F.col('Mai').cast(DoubleType()))
    .withColumn('6', F.col('Jun').cast(DoubleType()))
    .withColumn('7', F.col('Jul').cast(DoubleType()))
    .withColumn('8', F.col('Ago').cast(DoubleType()))
    .withColumn('9', F.col('Set').cast(DoubleType()))
    .withColumn('10', F.col('Out').cast(DoubleType()))
    .withColumn('11', F.col('Nov').cast(DoubleType()))
    .withColumn('12', F.col('Dez').cast(DoubleType()))
    .withColumn('total', F.round(F.col('TOTAL').cast(DoubleType()),3))
    .drop('COMBUSTÍVEL', 'ANO', 'ESTADO', 'REGIÃO','Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez')
)

# Write table in silver layer with delta format
logging.info("Writing delta table into the silver layer...")
(
    diesel_sales
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("year")
    .save("s3://anp-silver/diesel_sales/")
)

logging.info("Process finished.")
