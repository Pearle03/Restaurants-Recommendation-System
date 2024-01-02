import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType
from elasticsearch import Elasticsearch

# Initialize findspark to make Spark available in the Python environment
findspark.init()

# Constants for Kafka and Elasticsearch configurations
KAFKA_TOPIC = 'yelp-stream'  # Kafka topic to subscribe to
KAFKA_BROKER = 'localhost:9092'  # Address of the Kafka broker
ES_INDEX = 'yelpraw'  # Elasticsearch index name
ES_TYPE = 'restaurant'  # Type within the Elasticsearch index
ES_RESOURCE = f'{ES_INDEX}/{ES_TYPE}'  # Full resource path for Elasticsearch

# Initialize Elasticsearch client
es = Elasticsearch(["http://localhost:9200"])


def write_to_elasticsearch(df):
    """
    Writes a Spark DataFrame to Elasticsearch.
    Args:
        df (DataFrame): DataFrame to write to Elasticsearch
    """
    # Convert DataFrame to RDD for writing to Elasticsearch
    rdd = df.rdd

    # Check if RDD is not empty and then proceed to write to Elasticsearch
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
    """
    Main function to read data from Kafka and write to Elasticsearch.
    """
    # Initialize SparkSession for the application
    spark = SparkSession.builder \
        .appName("YelpConsumer") \
        .master("local[2]") \
        .getOrCreate()

    # Read streaming data from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    # Parse the JSON data from Kafka and convert it to DataFrame
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), StringType()).alias("json"))

    # Process each batch of data and write it to Elasticsearch
    query = parsed_df.writeStream \
        .foreachBatch(write_to_elasticsearch) \
        .start()

    # Await termination of the query to keep the application running
    query.awaitTermination()


if __name__ == '__main__':
    # Execute the main function when the script is run
    main()
