
# 📘 Notebook: create_silver_bom_tables
# Scopo: creare tabelle Delta in Lakehouse con dati simulati simili a SAP (già tipizzati, livello silver)

## 1. Inizializzazione


from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import os

# Initialize Spark session for local development
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("SAP BOM Extractor") \
    .getOrCreate()

# Ensure data directory exists
os.makedirs("data", exist_ok=True)


## 2. Tabella STKO (testata distinta base)


stko_schema = StructType([
    StructField("MATNR", StringType()),
    StructField("WERKS", StringType()),
    StructField("STLNR", StringType()),
    StructField("STLAL", StringType()),
    StructField("STLAN", StringType()),
    StructField("DATUV", DateType()),  # data validità da
    StructField("BMENG", DoubleType()) # quantità base
])

stko_data = [
    ("MAT001", "PL01", "BOM001", "01", "1", date(2020, 1, 1), 1.0),
    ("MAT002", "PL01", "BOM002", "01", "1", date(2020, 1, 1), 1.0),
]

stko_df = spark.createDataFrame(stko_data, schema=stko_schema)
# Save as Parquet in local ./data directory
stko_df.write.mode("overwrite").parquet("data/STKO")

## 3. Tabella STPO (componenti)


stpo_schema = StructType([
    StructField("STLNR", StringType()),
    StructField("POSNR", IntegerType()),
    StructField("IDNRK", StringType()),  # materiale componente
    StructField("MENGE", DoubleType()),
    StructField("DATUV", DateType())     # data validità da
])

stpo_data = [
    ("BOM001", 10, "MAT002", 2.0, date(2020, 1, 1)),
    ("BOM001", 20, "MAT003", 4.0, date(2020, 1, 1)),
    ("BOM002", 10, "MAT004", 1.0, date(2020, 1, 1)),
    ("BOM002", 20, "MAT005", 3.0, date(2020, 1, 1)),
]

stpo_df = spark.createDataFrame(stpo_data, schema=stpo_schema)
# Save as Parquet in local ./data directory
stpo_df.write.mode("overwrite").parquet("data/STPO")

## 4. Tabella MAST (link MATNR → STLNR)

mast_schema = StructType([
    StructField("MATNR", StringType()),
    StructField("WERKS", StringType()),
    StructField("STLNR", StringType()),
    StructField("STLAL", StringType())
])

mast_data = [
    ("MAT001", "PL01", "BOM001", "01"),
    ("MAT002", "PL01", "BOM002", "01"),
]

mast_df = spark.createDataFrame(mast_data, schema=mast_schema)
# Save as Parquet in local ./data directory
mast_df.write.mode("overwrite").parquet("data/MAST")


## 5. Tabelle STAS / STZU (inizialmente vuote)

# STAS: per distinte alternative
spark.createDataFrame([], StructType([
    StructField("STLNR", StringType()),
    StructField("STLAL", StringType()),
    StructField("PRIOR", IntegerType())
])).write.mode("overwrite").parquet("data/STAS")

# STZU: modifiche e varianti (engineering change)
spark.createDataFrame([], StructType([
    StructField("STLNR", StringType()),
    StructField("AENNR", StringType()),
    StructField("DATUV", DateType())
])).write.mode("overwrite").parquet("data/STZU")