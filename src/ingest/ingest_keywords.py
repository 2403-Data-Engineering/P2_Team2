import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, when, count, avg, max as spark_max, sum as spark_sum, upper, lower, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from ftfy import fix_text
from json_repair import repair_json

from dotenv import load_dotenv
load_dotenv()

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

bronze_path = os.getenv("ingest_filepath", "src/bronze/")

spark = SparkSession.builder.appName("Ingest").getOrCreate()


keywords_schema = StructType(fields=[
    StructField("id", IntegerType(), False),
    StructField("keywords", StringType(), False)
])


# cols: id, keywords
# id = string, keywords = json
keywords_df = spark.read.csv(path=f"{bronze_path}keywords.csv", header=True, schema=keywords_schema)

keywords_df.printSchema()
keywords_df.show()

fix_text_udf = udf(lambda s: repair_json(fix_text(s)), StringType())

# fix the keywords json (you don't have to worry about nulls because we drop them all)
# just drop all the rows with nulls because if no keywords, then useless and if no id, then also useless
cleaned_data = keywords_df.dropna().dropDuplicates(subset=["id"]).withColumn("keywords", fix_text_udf("keywords"))



cleaned_data.write.parquet("src/silver/keywords/", "overwrite")
print("cleaned up nulls and strings in keywords.csv")