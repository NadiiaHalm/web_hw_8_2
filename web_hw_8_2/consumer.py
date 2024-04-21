import json
import os
import sys
from bson import ObjectId

import pika

from models import Customer

QUEUE = "email_queue"


def main():
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials))
    channel = connection.channel()

    channel.queue_declare(queue=QUEUE, durable=True)

    def callback(ch, method, properties, body):
        message = json.loads(body.decode())
        contact_id = ObjectId(message['customer_id'])
        contact = Customer.objects.get(id=contact_id)
        print(f"Sending email to {contact.email}")

        contact.email_sent = True
        contact.save()
        print(f"Contact {contact_id} marked as sent")

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE, on_message_callback=callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
