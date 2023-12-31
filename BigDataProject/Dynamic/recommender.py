import argparse
import math
from pyspark import SparkConf, SparkContext


def read_and_filter_elasticsearch(sc, category_user):
    es_query = f"{category_user}" if category_user else "*:*"
    rdd = sc.newAPIHadoopRDD(
        inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf={"es.resource": "yelpraw/restaurant", "es.query": es_query}
    )
    return rdd


def location_recommender(yelpData, user_location=(36.1027496, -115.1686673), max_distance=5):
    if yelpData is None:
        return None
    rcvd_data = yelpData[1]
    destination = (float(rcvd_data.get("latitude")), float(rcvd_data.get("longitude")))
    dist_location = distance(user_location, destination)
    if dist_location < max_distance:
        return rcvd_data
    return None


def remap_to_es_format(rec):
    if rec is None:
        return None
    location = f"{rec['latitude']},{rec['longitude']}"
    return {
        "businessId": rec["Business_Id"],
        "name": rec["name"],
        "full_address": rec["full_address"],
        "categories": rec["categories"],
        "stars": rec["stars"],
        "location": location
    }


def distance(origin, destination):
    radius = 6371  # km
    dlat = math.radians(destination[0] - origin[0])
    dlon = math.radians(destination[1] - origin[1])
    a = (math.sin(dlat / 2) ** 2 + math.cos(math.radians(origin[0]))
         * math.cos(math.radians(destination[0])) * math.sin(dlon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return radius * c


def main(category_user):
    conf = SparkConf().setMaster("local[2]").setAppName("YelpRecommender")
    sc = SparkContext(conf=conf)

    rdd_data = read_and_filter_elasticsearch(sc, category_user)
    parsed_mapped_data = rdd_data.filter(lambda data: location_recommender(data))
    sorted_data = parsed_mapped_data.top(150, key=lambda a: a[1]["stars"])
    topn_data = [remap_to_es_format(data) for data in sorted_data[:5]]

    for rec in topn_data:
        print(f"{rec['name']} - {rec['full_address']} - {rec['stars']}")

    # Write to Elasticsearch (if needed)
    # ...


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Yelp Restaurant Recommender')
    parser.add_argument('--category', type=str, default="", help='Preferred restaurant category')
    args = parser.parse_args()

    main(args.category)
