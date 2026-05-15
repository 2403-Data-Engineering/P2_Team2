import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, when, count, avg, max as spark_max, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, TimestampType

from dotenv import load_dotenv
load_dotenv()

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

silver_path = os.getenv("ingest_filepath", "src/bronze/")

spark = SparkSession.builder.appName("Ingest").getOrCreate()

# need to load parquet files from silver folder

credits = spark.read.parquet(path=f"{silver_path}credits.csv", header=True, inferSchema=True)

keywords = spark.read.parquet(path=f"{silver_path}keywords.csv", header=True, inferSchema=True)

movies = spark.read.parquet(path=f"{silver_path}movies/", header=True, inferSchema=True)

ratings = spark.read.parquet(path=f"{silver_path}ratings.csv", header=True, inferSchema=True)

# movies.join("", "Nonf", )

avg_rating = ratings.select(col("movieId, rating"), col("m")).groupBy(col("movieId")).agg(avg(col("rating").alias("Average_Rating")))

