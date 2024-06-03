"""
This script reads messages from RabbitMQ queues and processes them, implementing alerts based on temperature data.
"""

import pika
import struct
import sys
from datetime import datetime
from collections import deque

# Define deques for storing recent temperature readings
smoker_temps = deque(maxlen=5)
food_a_temps = deque(maxlen=20)
food_b_temps = deque(maxlen=20)

def handle_smoker_queue(ch, method, properties, body):
    """Handle messages from the smoker queue."""
    timestamp, temp = struct.unpack('!df', body)
    formatted_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    print(f"Received from smoker: {formatted_time}, Temperature: {temp}F")
    smoker_temps.append(temp)

    if len(smoker_temps) == smoker_temps.maxlen and (smoker_temps[0] - temp) >= 15:
        print("Alert: Smoker temperature dropped by 15F or more in the last 2.5 minutes!")

    ch.basic_ack(delivery_tag=method.delivery_tag)

def handle_food_a_queue(ch, method, properties, body):
    """Handle messages from the food A queue."""
    timestamp, temp = struct.unpack('!df', body)
    formatted_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    print(f"Received from food A: {formatted_time}, Temperature: {temp}F")
    food_a_temps.append(temp)

    if len(food_a_temps) == food_a_temps.maxlen and (max(food_a_temps) - min(food_a_temps)) <= 1:
        print("Alert: Food A temperature change is 1F or less in the last 10 minutes!")

    ch.basic_ack(delivery_tag=method.delivery_tag)

def handle_food_b_queue(ch, method, properties, body):
    """Handle messages from the food B queue."""
    timestamp, temp = struct.unpack('!df', body)
    formatted_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    print(f"Received from food B: {formatted_time}, Temperature: {temp}F")
    food_b_temps.append(temp)

    if len(food_b_temps) == food_b_temps.maxlen and (max(food_b_temps) - min(food_b_temps)) <= 1:
        print("Alert: Food B temperature change is 1F or less in the last 10 minutes!")

    ch.basic_ack(delivery_tag=method.delivery_tag)

def start_consuming(queue, callback_func):
    """Set up the RabbitMQ consumer."""
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=queue, durable=True)
    channel.basic_consume(queue=queue, on_message_callback=callback_func, auto_ack=False)
    print(f"Listening for messages on {queue}. Press CTRL+C to exit.")
    channel.start_consuming()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python consumer.py <queue_name>")
        sys.exit(1)

    queue_name = sys.argv[1]

    if queue_name == "01-smoker":
        start_consuming(queue_name, handle_smoker_queue)
    elif queue_name == "02-food-A":
        start_consuming(queue_name, handle_food_a_queue)
    elif queue_name == "03-food-B":
        start_consuming(queue_name, handle_food_b_queue)
