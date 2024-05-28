import pika
import struct
import sys
from datetime import datetime

def callback(ch, method, properties, body):
    """Processes and prints data received from RabbitMQ queues."""
    timestamp, temp = struct.unpack('!df', body)
    formatted_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    print(f"Received from {method.routing_key}: {formatted_time}, Temperature: {temp}F")
    ch.basic_ack(delivery_tag=method.delivery_tag)

def start_listening(queue_name):
    """Sets up RabbitMQ consumer connections to listen to a specific queue."""
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
    
    print(f"Listening for messages on {queue_name}. Press CTRL+C to exit.")
    channel.start_consuming()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python listener.py <queue_name>")
        sys.exit(1)
    queue_name = sys.argv[1]
    start_listening(queue_name)
