import csv
import json
from threading import Thread
from time import sleep

from kafka import KafkaProducer

topic = 'yelp-stream'
csv_path = "VegasRestaurantData.csv"

# Initialize Kafka Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')


class Streamer(Thread):
    def __init__(self, threadID, name):
        super().__init__()
        self.threadID = threadID
        self.name = name

    def run(self):
        print(f"Starting {self.name}")
        try:
            csv_to_json(csv_path)
            with open("VegasRestaurantData.json", 'r') as json_data:
                for line in json_data:
                    print("Sending data via kafka")
                    send_kafka(line)
                    sleep(1)
        except IOError as e:
            print(f"Error reading file: {e}")
        print(f"Exiting {self.name}")


def send_kafka(message):
    try:
        producer.send(topic, message.encode())
    except Exception as e:
        print(f"Error sending message to Kafka: {e}")


def csv_to_json(csv_path):
    try:
        with open(csv_path, 'r', encoding='utf-8') as csvfile, open('VegasRestaurantData.json', 'w') as jsonfile:
            fieldnames = (
                'business_id', 'full_address', 'stars', 'categories',
                'name', 'latitude', 'longitude', "text")
            
            reader = csv.DictReader(csvfile, fieldnames)
            for row in reader:
                json.dump(row, jsonfile)
                jsonfile.write('\n')
    except IOError as e:
        print(f"Error reading/writing file: {e}")


if __name__ == '__main__':
    streamer_thread = Streamer(1, "Streamer-Thread")
    streamer_thread.start()
    streamer_thread.join()
