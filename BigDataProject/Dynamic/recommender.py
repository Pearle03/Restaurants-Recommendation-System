import math
import sys

from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from pyspark.sql.connect.functions import udf, desc
from pyspark.sql.functions import col, lit
from pyspark.sql.types import DoubleType

# Initialize Elasticsearch
es = Elasticsearch("http://localhost:9200")


# Function to clear the Elasticsearch index
def clearElasticSearch(index):
    if es.indices.exists(index=index):
        es.indices.delete(index=index)


# Function to read data from Elasticsearch into a DataFrame
def readElasticSearch(spark, index, doc_type):
    df = spark.read \
        .format("es") \
        .option("es.resource", f"{index}/{doc_type}") \
        .load()
    return df


# Function to transform data for Elasticsearch indexing
def remap_es(df, index, doc_type):
    # Define the transformation logic here
    # For example, you might concatenate latitude and longitude into a 'location' field
    df = df.withColumn("location", col("latitude").cast("string") + "," + col("longitude").cast("string"))

    # Then write the transformed DataFrame to Elasticsearch
    df.write \
        .format("es") \
        .option("es.resource", f"{index}/{doc_type}") \
        .save()


# Function to print the results
def printResult(df):
    for row in df.collect():
        print(row)


# Function to filter unique data based on a key
def copyUniqueData(df, key_column, count):
    # Assuming 'key_column' is the column based on which uniqueness is determined
    df_distinct = df.dropDuplicates([key_column])
    df_topn = df_distinct.limit(count)
    return df_topn


# Function to calculate the distance between two points on the Earth
def distance(lat1, lon1, lat2, lon2):
    radius = 6371  # km

    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) * math.sin(dlat / 2) +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) * math.sin(dlon / 2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = radius * c

    return d


# UDF to calculate distance using the Haversine formula
distance_udf = udf(distance, DoubleType())


# Function to recommend locations based on a category and within a certain distance
def location_recommender(df, user_location, category_user, max_distance):
    # Filter by category if specified
    if category_user:
        df = df.filter(df.categories.contains(category_user))

    # Add a new column 'distance' to the DataFrame using the distance UDF
    df = df.withColumn("distance", distance_udf(
        lit(user_location[0]), lit(user_location[1]),
        col("latitude"), col("longitude")
    ))

    # Filter by distance
    df = df.filter(col("distance") <= max_distance)

    return df


# Main function
def main(category_user):
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("YelpRecommender") \
        .getOrCreate()

    # Define user location and max distance (in km)
    user_location = (36.1027496, -115.1686673)  # Example coordinates
    max_distance = 5  # Example max distance

    # Clear the Elasticsearch index if needed
    clearElasticSearch("yelpreco")

    # Read data from Elasticsearch
    df = readElasticSearch(spark, "yelpraw", "restaurant")

    # Apply location recommender
    df_recommended = location_recommender(df, user_location, category_user, max_distance)

    # Filter top entries by the number of stars
    df_recommended = df_recommended.orderBy(desc("stars"))

    # Copy unique data and limit the number of results
    df_unique = copyUniqueData(df, "business_id", 5)

    # Print results
    printResult(df_recommended)

    # Remap data for Elasticsearch and index it
    remap_es(df_unique, "yelpreco", "restaurant")


if __name__ == '__main__':
    if len(sys.argv) == 2:
        category_user = sys.argv[1]
        print("User Given category", category_user)
    else:
        category_user = ""
        print("No user preferred category is given. Show top 5 trending restaurants")
    main(category_user)
