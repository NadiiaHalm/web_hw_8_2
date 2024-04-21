import json

import pika
from faker import Faker

from models import Customer

fake = Faker()

EXCHANGE = "hw_8"
QUEUE = "email_queue"

credentials = pika.PlainCredentials('guest', 'guest')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials))
channel = connection.channel()

channel.exchange_declare(exchange=EXCHANGE, exchange_type='direct')
channel.queue_declare(queue=QUEUE, durable=True)
channel.queue_bind(exchange=EXCHANGE, queue=QUEUE)


def main(num_customers):
    for i in range(num_customers):
        customer = Customer(
            fullname=fake.name(),
            email=fake.email(),
            send_email=False
        )
        customer.save()
        message = {"customer_id": str(customer.id)}
        channel.basic_publish(exchange=EXCHANGE, routing_key=QUEUE, body=json.dumps(message).encode())
    print("All contacts created and messages sent to RabbitMQ")
    connection.close()


if __name__ == '__main__':
    main(10)
