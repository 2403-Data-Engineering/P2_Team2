import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, when, count, avg, max as spark_max, sum as spark_sum, upper, lower, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, TimestampType
from ftfy import fix_text
from json_repair import repair_json

from dotenv import load_dotenv
load_dotenv()

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

bronze_path = os.getenv("ingest_filepath", "src/bronze/")

spark = SparkSession.builder.appName("Ingest").getOrCreate()


credits_schema = StructType(fields=[
    StructField("cast", StringType(), True),   # name, type, nullable
    StructField("crew", StringType(), True),
    StructField("id", IntegerType(), False)
])

# cols: cast,crew,id
# cols: json, json, string
credits_df = spark.read.csv(path=f"{bronze_path}credits.csv", header=True, schema=credits_schema)

credits_df.printSchema()
credits_df.show()

# this is for fixing mojibake in every string
fix_text_udf = udf(lambda s: repair_json(fix_text(s)) if s is not None else None, StringType())
fix_empty_list = udf(lambda s: "[]" if (s is None or s == "") else s)

# fix_json_udf = udf(lambda s: s.apply())

# replace null values in cast and crew as []
cleaned_data = credits_df \
    .withColumn("id", when(credits_df["id"] <= 0, -1).otherwise(credits_df["id"])) \
    .replace(-1, None) \
    .withColumn("cast", fix_text_udf("cast")) \
    .withColumn("crew", fix_text_udf("crew")) \
    .fillna("[]", subset = ["cast", "crew"]) \
    .dropna(subset = ["id"]) \
    .dropDuplicates(subset = ['id']) 

cleaned_data.write.parquet("src/silver/credits/", "overwrite")

print("cleaned up nulls and strings")
    


# to clean up json, we need to use the json loads func

# replace every single quotes with double quotes


    # replace all the values within that have a negative id (for anything else, don't do anything)
    
    # need to fix all text in strings before


# getting rid of duplicates in credits only based on the id




