import os

from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.utils import AnalysisException

os.environ['PYSPARK_SUBMIT_ARGS'] = ('--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0,'
                                     'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,'
                                     'org.elasticsearch:elasticsearch-spark-30_2.12:8.11.3 pyspark-shell')

# Initialize Elasticsearch client
es = Elasticsearch("http://localhost:9200")


def remap_elastic(rec):
    # Assuming 'rec' is a dictionary representing the JSON object
    # The key could be a document ID or a constant if the ID is included in the record itself
    content = ('key', rec)
    return content


def writeElasticSearch(df, epoch_id):
    try:
        df.write.format("org.elasticsearch.spark.sql") \
            .option("es.resource", "yelpraw/restaurant") \
            .mode("append") \
            .save()
    except Exception as e:
        print(f"Error writing to Elasticsearch: {e}")


def main():
    try:
        # Configure Spark
        spark = SparkSession.builder \
            .appName("YelpConsumer") \
            .getOrCreate()

        spark.conf.set("es.nodes.wan.only", "false")

        # Define the schema of the data
        schema = StructType([
            StructField("business_id", StringType(), True),
            StructField("full_address", StringType(), True),
            StructField("stars", StringType(), True),
            StructField("categories", StringType(), True),
            StructField("name", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("text", StringType(), True)
        ])

        # Create Kafka Direct Stream
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "yelp-stream") \
            .load()

        # Parse JSON data and write to Elasticsearch
        parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("parsed_value"))

        parsed_df.writeStream.foreachBatch(writeElasticSearch).start()

        # Start the Spark Streaming Context
        spark.streams.awaitAnyTermination()

    except AnalysisException as e:
        print(f"Error reading from Kafka or parsing JSON data: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == '__main__':
    main()
