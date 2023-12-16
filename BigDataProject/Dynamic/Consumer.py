import json
import findspark

findspark.init()

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from elasticsearch import Elasticsearch

# Constants
KAFKA_TOPIC = 'yelp-stream'
KAFKA_BROKER = 'localhost:9092'
ES_INDEX = 'yelpraw'
ES_TYPE = 'restaurant'
ES_RESOURCE = f'{ES_INDEX}/{ES_TYPE}'

# Initialize Elasticsearch
es = Elasticsearch()


def write_to_elasticsearch(rdd):
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
    conf = SparkConf().setMaster("local[2]").setAppName("YelpConsumer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)

    kstream = KafkaUtils.createDirectStream(ssc, topics=[KAFKA_TOPIC],
                                            kafkaParams={"metadata.broker.list": KAFKA_BROKER})

    parsed_json = kstream.map(lambda message: json.loads(message[1]))
    parsed_json.foreachRDD(write_to_elasticsearch)

    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    main()
