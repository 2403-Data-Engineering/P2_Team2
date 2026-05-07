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

bronze_path = os.getenv("ingest_filepath", "src/bronze/")

spark = SparkSession.builder.appName("Ingest").getOrCreate()





# cols: id, keywords
# id = string, keywords = json
keywords_df = spark.read.csv(path=f"{bronze_path}keywords.csv", header=True, inferSchema=True)

keywords_df.printSchema()
keywords_df.show()










# # cols: adult,belongs_to_collection,budget,genres,homepage,id,imdb_id,original_language,original_title,overview,popularity,poster_path,production_companies,production_countries,release_date,revenue,runtime,spoken_languages,status,tagline,title,video,vote_average,vote_count
# """ cols: 
# adult:boolean,
# belongs_to_collection:JSON object (nullable),
# budget:integer,
# genres:JSON list[{id:int, name:str}],
# homepage:string (nullable),
# id:integer,
# imdb_id:string,
# original_language:string,
# original_title:string,
# overview:string,
# popularity:float,
# poster_path:string,
# production_companies:JSON list[{id:int, name:str}],
# production_countries:JSON list[{iso_3166_1:str, name:str}],
# release_date:datetime (YYYY-MM-DD),
# revenue:float,
# runtime:float (minutes),
# spoken_languages:JSON list[{iso_639_1:str, name:str}],
# status:string,
# tagline:string (nullable),
# title:string,
# video:boolean,
# vote_average:float,
# vote_count:float
# """
# movies_metadata_df = spark.read.csv(path=f"{bronze_path}movies_metadata.csv", header=True, inferSchema=True)

# # cols: userId,movieId,rating,timestamp
# # cols: int, int, float, datetime
# ratings_small_df = spark.read.csv(path=f"{bronze_path}ratings_small.csv", header=True, inferSchema=True)


# print(movies_metadata_df.dtypes)
 



# credits_csv.select([
#     spark_sum(col(c).isNull().cast("int")).alias(c)
#     for c in credits_csv.columns
# ])

#when(col("genres").rlike(r"^\[.*\]$"), col("genres")).otherwise(lit("[]"))
