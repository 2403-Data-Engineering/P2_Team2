from pyspark.sql.types import DataType
import weaviate
import weaviate.classes.config as wc
from pyspark.sql import SparkSession
from weaviate.classes.query import MetadataQuery

spark = SparkSession.builder.appName("semantic_movie_search").getOrCreate()

client = weaviate.connect_to_local()
print("Connected:", client.is_ready())

if (client.collections.exists("Movie")):
    client.collections.delete("Movie")

client.collections.create(
    name="movie",
    vectorizer_config=wc.Configure.Vectorizer.text2vec_transformers(),
    properties=[
        wc.Property(name="id", data_type=wc.DataType.TEXT, skip_vectorization=True),
        wc.Property(name="title", data_type=wc.DataType.TEXT),
        wc.Property(name="release_date", data_type=wc.DataType.DATE),
        wc.Property(name="overview", data_type=wc.DataType.TEXT),
        wc.Property(name="tagline", data_type=wc.DataType.TEXT, skip_vectorization=True),
        wc.Property(name="genre_list", data_type=wc.DataType.TEXT, skip_vectorization=True),
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
movies_data = spark.read.parquet("src/gold/movies")


# for every movie in the batch
with movies.batch.dynamic() as batch:
    for row in movies_data.collect():
        # combined_text = (f"{row.title}" by f"{row.director}",
        # f"{row.keyword_list}")
        batch.add_object(properties={
            "title": row.title,
            "director": row.director,
            "year": int(row.release_date),
            "genre_list": row.genre_list,
            "combined_text": row.combined_text
        })
        
        
            #         "title": row.title,
            # "director": row.director,
            # "year": int(row.release_date),
            # "genre_list": row.genre_list,
            # "combined_text": combined_text


result = movies.aggregate.over_all(total_count=True)
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



print("========================= Results =========================")
for obj in response.objects:
    print(f"{obj.properties['title']} - {obj.properties['director']} - ({obj.metadata.distance: .3f})")
print("===========================================================")


# adult,belongs_to_collection,budget,genres,homepage,id,imdb_id,original_language,original_title,overview,popularity,poster_path,production_companies,production_countries,release_date,revenue,runtime,spoken_languages,status,tagline,title,video,vote_average,vote_count