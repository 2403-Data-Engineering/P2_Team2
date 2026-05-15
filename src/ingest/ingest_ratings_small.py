import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, when, count, avg, max as spark_max, sum as spark_sum, upper, lower, udf
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, FloatType, TimestampType
from ftfy import fix_text
from json_repair import repair_json

from dotenv import load_dotenv
load_dotenv()

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

bronze_path = os.getenv("ingest_filepath", "src/bronze/")


spark = SparkSession.builder.appName("Ingest").getOrCreate()



ratings_schema = StructType(fields=[
    StructField("userId", IntegerType()),
    StructField("movieId", IntegerType()),
    StructField("rating", FloatType()),
    StructField("timestamp", TimestampType(), metadata=)
])


ratings_small_df: DataFrame = spark.read.csv(path=f"{bronze_path}ratings_small.csv", header=True, schema=ratings_schema)

ratings_small_df.printSchema()
ratings_small_df.show()

