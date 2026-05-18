from pyspark.sql.types import DataType
import sys
import weaviate
import weaviate.classes.config as wc
from pyspark.sql import SparkSession
from weaviate.classes.query import MetadataQuery
import datetime

#TO RUN THIS SCRIPT YOU MUST HAVE WEAVIATE RUNNING
# specifically with docker compose up
#setting spark to ignore crc files if in folder for loading the data
spark = SparkSession.builder.config("spark.sql.files.ifngoreCorruptFiles", "true").appName("semantic_movie_search").getOrCreate()

client = weaviate.connect_to_local()
print("Connected:", client.is_ready())

if (client.collections.exists("Movie")):
    print("Movie collection already exists")
    client.collections.delete("Movie")

client.collections.create(
    name="movie",
    vectorizer_config=wc.Configure.Vectorizer.text2vec_transformers(),
    properties=[
        wc.Property(name="title", data_type=wc.DataType.TEXT),
        wc.Property(name="release_date", data_type=wc.DataType.DATE),
        wc.Property(name="overview", data_type=wc.DataType.TEXT),
        wc.Property(name="tagline", data_type=wc.DataType.TEXT, skip_vectorization=True),
        wc.Property(name="genre_list", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True),
        wc.Property(name="keyword_list", data_type=wc.DataType.TEXT),
        wc.Property(name="director", data_type=wc.DataType.TEXT),
        wc.Property(name="top_cast", data_type=wc.DataType.TEXT),
        wc.Property(name="avg_rating", data_type=wc.DataType.NUMBER, skip_vectorization=True),
        wc.Property(name="rating_count", data_type=wc.DataType.INT, skip_vectorization=True),
        wc.Property(name="combined_text", data_type=wc.DataType.TEXT)
    ]
)
print("movie collection created")
# create the movie collection in weaviate
movies = client.collections.get("Movie")

# read the parquet in the gold folder
movies_data = spark.read.parquet("src/gold/movies/part-00000-eda87c56-1d9a-4072-adcc-61181e63d814-c000.snappy.parquet")

# movies_data.printSchema()

# for every movie in the batch
with movies.batch.dynamic() as batch:
    for row in movies_data.collect():
        # combined_text = (f"{row.title}" by f"{row.director}",
        # f"{row.keyword_list}")
        batch.add_object(properties={
            "title": row.title,
            "director": row.director,
            "year": str(row.release_date),
            "genre_list": row.genre_list,
            "combined_text": row.combined_text
        })
        
        
            #         "title": row.title,
            # "director": row.director,
            # "year": int(row.release_date),
            # "genre_list": row.genre_list,
            # "combined_text": combined_text


result = movies.aggregate.over_all(total_count=True)

if result.total_count == 0:
    sys.exit()
else:
    print(f"Total movies in collection: {result.total_count}")

failed = movies.batch.failed_objects
if failed:
    print(f"Warning: {len(failed)} objects failed to import")
    for f in failed[:3]:
        print(f)
else:
    print("All movies imported successfully")





print("Enter a search query: ")
input = input()

response = movies.query.hybrid(
    query=input,
    limit=5,
    return_metadata=MetadataQuery(distance=True),
    alpha=0.75
    
)

client.close()
spark.stop()




# for every movie in the response, get its title, its director, and its distance
print("========================= Results =========================")
for obj in response.objects:
    print(f"{obj.properties['title']} - {obj.properties['director']} - ({obj.metadata.distance})")
    print('\n')
print("===========================================================")
#f"{obj.properties['title']} - {obj.properties['director']} - ({obj.metadata.distance: .3f})"

# f"{obj.properties['title']} - {obj.properties['director']} - ({obj.metadata.distance})"

# Printing a response object gives you this:
# Object(uuid=_WeaviateUUIDInt('e01fd9b0-a9de-4f02-a8f0-857d8a302dda'), metadata=MetadataReturn(creation_time=None, last_update_time=None, distance=None, certainty=None, score=None, explain_score=None, is_consistent=None, rerank_score=None), properties={'director': None, 'keyword_list': None, 'overview': None, 'genre_list': ['Horror', 'Thriller', 'Action'], 'release_date': None, 'avg_rating': None, 'top_cast': None, 'year': '2015-07-07', 'title': 'Awaken', 'combined_text': 'Awaken A Perfect Vacation A random group of people wake up on an Island where they are being hunted down in a sinister plot to harvest their organs.   Horror Thriller Action', 'tagline': None, 'rating_count': None}, references=None, vector={}, collection='Movie')


# adult,belongs_to_collection,budget,genres,homepage,id,imdb_id,original_language,original_title,overview,popularity,poster_path,production_companies,production_countries,release_date,revenue,runtime,spoken_languages,status,tagline,title,video,vote_average,vote_count