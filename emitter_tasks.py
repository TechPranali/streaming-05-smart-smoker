"""
This script reads temperature data from 'smoker-temps.csv' every 30 seconds and sends it to specific RabbitMQ queues for each temperature channel.
"""

import pika
import csv
import time
from datetime import datetime
import struct

def send_data_to_queue(server, queue_name, content):
    """Connects to RabbitMQ and sends encoded data to a specified queue."""
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(server))
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
        channel.basic_publish(exchange="", routing_key=queue_name, body=content)
        print(f"Data sent to {queue_name}: {content}")
    finally:
        connection.close()

def process_data():
    """Reads temperature data from CSV and sends it to the appropriate RabbitMQ queues."""
    with open('smoker-temps.csv', newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip the header row
        for row in reader:
            # Correct the date parsing to match the format in your CSV file
            time_stamp = datetime.strptime(row[0], '%m/%d/%y %H:%M:%S').timestamp()
            # Process each temperature sensor data
            for index, temp in enumerate(row[1:], 1):
                if temp:
                    queue_name = f"0{index}-smoker" if index == 1 else f"0{index}-food-A" if index == 2 else f"0{index}-food-B"
                    encoded_message = struct.pack('!df', time_stamp, float(temp))
                    send_data_to_queue("localhost", queue_name, encoded_message)
            time.sleep(30)

if __name__ == "__main__":
    process_data()
