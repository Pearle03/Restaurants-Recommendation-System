import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType
from elasticsearch import Elasticsearch

# Constants
KAFKA_TOPIC = 'yelp-stream'
KAFKA_BROKER = 'localhost:9092'
ES_INDEX = 'yelpraw'
ES_TYPE = 'restaurant'
ES_RESOURCE = f'{ES_INDEX}/{ES_TYPE}'

# Initialize Elasticsearch
es = Elasticsearch(["http://localhost:9200"])


def write_to_elasticsearch(df):
    # Convert DataFrame to RDD
    rdd = df.rdd

    if not rdd.isEmpty():
        try:
            rdd.saveAsNewAPIHadoopFile(path='-',
                                       outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
                                       keyClass="org.apache.hadoop.io.NullWritable",
                                       valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
                                       conf={"es.resource": ES_RESOURCE})
        except Exception as e:
            print(f"Error writing to Elasticsearch: {e}")


def main():
    spark = SparkSession.builder \
        .appName("YelpConsumer") \
        .master("local[2]") \
        .getOrCreate()

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    # Assuming the Kafka value is a string of JSON
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), StringType()).alias("json"))

    # Process and write the stream to Elasticsearch
    query = parsed_df.writeStream \
        .foreachBatch(write_to_elasticsearch) \
        .start()

    query.awaitTermination()


if __name__ == '__main__':
    main()
