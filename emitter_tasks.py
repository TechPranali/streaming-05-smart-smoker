"""
This script reads temperature data from 'smoker-temps.csv' every 30 seconds and sends it to RabbitMQ queues for each temperature channel.
"""

import pika
import csv
import time
from datetime import datetime
import struct
import webbrowser

def open_rabbitmq_admin():
    """Prompt to open RabbitMQ Admin interface."""
    response = input("Do you want to open the RabbitMQ Admin site? (y/n): ").strip().lower()
    if response == 'y':
        webbrowser.open_new("http://localhost:15672/")

def push_to_queue(host, queue, message):
    """Establish connection to RabbitMQ and push message to specified queue."""
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        channel = connection.channel()
        channel.queue_declare(queue=queue, durable=True)
        channel.basic_publish(exchange="", routing_key=queue, body=message)
        print(f"Sent data to {queue}: {message}")
    finally:
        connection.close()

def process_csv_and_send():
    """Read temperature data from CSV and send to respective RabbitMQ queues."""
    with open('smoker-temps.csv', newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip the header row
        for row in reader:
            timestamp = datetime.strptime(row[0], '%m/%d/%y %H:%M:%S').timestamp()
            for idx, temp in enumerate(row[1:], start=1):
                if temp:
                    queue = f"0{idx}-smoker" if idx == 1 else f"0{idx}-food-A" if idx == 2 else f"0{idx}-food-B"
                    encoded_data = struct.pack('!df', timestamp, float(temp))
                    push_to_queue("localhost", queue, encoded_data)
            time.sleep(30)

if __name__ == "__main__":
    open_rabbitmq_admin()
    process_csv_and_send()
