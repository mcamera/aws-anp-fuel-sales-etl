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

# Read silver data
logging.info("Read from the silver layer...")
sales_oil_fuels = (
    spark.read
    .format("delta")
    .load("s3://anp-silver/sales_oil_fuels")
)

# Unpivot the table
unpivot_expr = "stack(12, 'jan', jan, 'fev', fev, 'mar', mar, 'abr', abr, 'mai', mai, 'jun', jun, 'jul', jul, 'ago', ago, 'set', set, 'out', out, 'nov', nov, 'dez', dez) as (month, volume)"
sales_oil_fuels = sales_oil_fuels.select('product','unit','year','uf','regiao',F.expr(unpivot_expr))

# Transforming the data
sales_oil_fuels = (
    sales_oil_fuels
    .withColumn('month', F.when(F.col('month') == 'jan',1)
                           .when(F.col('month') == 'fev',2)
                           .when(F.col('month') == 'mar',3)
                           .when(F.col('month') == 'abr',4)
                           .when(F.col('month') == 'mai',5)
                           .when(F.col('month') == 'jun',6)
                           .when(F.col('month') == 'jul',7)
                           .when(F.col('month') == 'ago',8)
                           .when(F.col('month') == 'set',9)
                           .when(F.col('month') == 'out',10)
                           .when(F.col('month') == 'nov',11)
                           .when(F.col('month') == 'dez',12)
               )
    .withColumn('year_month', F.to_date(F.concat(F.col('year'),F.lit('-'),F.col('month')), 'yyyy-M'))
    .withColumn('created_at', F.current_timestamp())
).drop('year','regiao','month')

# Write table in gold layer with the delta format
logging.info("Writing delta table into the gold layer...")
(
    sales_oil_fuels
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("year")
    .save("s3://anp-gold/sales_oil_fuels/")
)

# logging.info("Building symlink manifest...")
# sales_oil_fuels.generate("symlink_format_manifest")
# logging.info("Manifest built!")
# logging.info("Pipeline finished.")
