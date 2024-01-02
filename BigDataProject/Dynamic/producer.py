import csv
import json
from threading import Thread
from time import sleep
from kafka import KafkaProducer

# Topic for Kafka messaging and CSV file path
topic = 'yelp-stream'
csv_path = "RestaurantData.csv"

# Initialize a Kafka Producer for message streaming
producer = KafkaProducer(bootstrap_servers='localhost:9092')


class Streamer(Thread):
    """
    A class that extends Thread, responsible for streaming data from a JSON file to Kafka.
    """

    def __init__(self, threadID, name):
        super().__init__()
        self.threadID = threadID
        self.name = name

    def run(self):
        """Main execution method for the thread."""
        print(f"Starting {self.name}")
        try:
            # Convert CSV data to JSON format
            csv_to_json(csv_path)
            with open("RestaurantData.json", 'r') as json_data:
                # Read each line (JSON object) and send it to Kafka
                for line in json_data:
                    print("Sending data via kafka")
                    send_kafka(line)
                    sleep(1)  # Sleep for a second between sends
        except IOError as e:
            print(f"Error reading file: {e}")
        print(f"Exiting {self.name}")


def send_kafka(message):
    """
    Sends a message to a Kafka topic.
    Args:
        message (str): The message to be sent.
    """
    try:
        # Send the message to the Kafka topic
        producer.send(topic, message.encode())
    except Exception as e:
        print(f"Error sending message to Kafka: {e}")


def csv_to_json(csv_path):
    """
    Converts CSV data to JSON format.
    Args:
        csv_path (str): Path to the CSV file.
    """
    try:
        # Open the CSV file and a new JSON file for output
        with open(csv_path, 'r', encoding='utf-8') as csvfile, open('RestaurantData.json', 'w') as jsonfile:
            fieldnames = (
                'business_id', 'full_address', 'stars', 'categories',
                'name', 'latitude', 'longitude', "text")
            reader = csv.DictReader(csvfile, fieldnames)
            # Write each CSV row as a JSON object to the JSON file
            for row in reader:
                json.dump(row, jsonfile)
                jsonfile.write('\n')
    except IOError as e:
        print(f"Error reading/writing file: {e}")


if __name__ == '__main__':
    # Initialize and start the streaming thread
    streamer_thread = Streamer(1, "Streamer-Thread")
    streamer_thread.start()
    streamer_thread.join()  # Wait for the thread to complete
