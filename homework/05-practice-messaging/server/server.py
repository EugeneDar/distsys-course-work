import logging
from threading import Thread

import pika
import time
import json

from flask import Flask, request
from typing import List, Optional

from config import IMAGES_ENDPOINT, DATA_DIR


class Server:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.counter = 0
        self.processed_images = {}

        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port))

        self.task_channel = connection.channel()
        self.task_channel.queue_declare(queue='task_queue', durable=True)
        self.task_channel.confirm_delivery()

        other_connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port))
        result_channel = other_connection.channel()
        result_channel.queue_declare(queue='result_queue', durable=True)
        result_channel.basic_qos(prefetch_count=1)

        def listen_worker_messages():
            def callback(channel, method, properties, body):
                print(f'Received: {body.decode()}')
                message = json.loads(body.decode())

                self.processed_images[message['id']] = message['result']

                channel.basic_ack(delivery_tag=method.delivery_tag)

            result_channel.basic_consume(queue='result_queue', on_message_callback=callback)
            result_channel.start_consuming()

        thread = Thread(target=listen_worker_messages)
        thread.start()

    def store_image(self, image: str) -> int:
        message_id = self.counter
        self.counter += 1

        message = str({
            'id': message_id,
            'image': image,
        }).replace('\'', '\"')

        while True:
            try:
                self.task_channel.basic_publish(
                    exchange='',
                    routing_key='task_queue',
                    body=message,
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                    )
                )
            except Exception as e:
                print("retransmit")
                time.sleep(1)
            else:
                break

        return message_id

    def get_processed_images(self) -> List[int]:
        return list(self.processed_images.keys())

    def get_image_description(self, image_id: str) -> Optional[str]:
        raise NotImplementedError


def create_app() -> Flask:
    """
    Create flask application
    """
    app = Flask(__name__)

    server = Server('rabbitmq', 5672)

    @app.route(IMAGES_ENDPOINT, methods=['POST'])
    def add_image():
        body = request.get_json(force=True)
        image_id = server.store_image(body['image_url'])
        return {"image_id": image_id}

    @app.route(IMAGES_ENDPOINT, methods=['GET'])
    def get_image_ids():
        image_ids = server.get_processed_images()
        return {"image_ids": image_ids}

    @app.route(f'{IMAGES_ENDPOINT}/<string:image_id>', methods=['GET'])
    def get_processing_result(image_id):
        result = server.get_image_description(image_id)
        if result is None:
            return "Image not found.", 404
        else:
            return {'description': result}

    return app


app = create_app()

if __name__ == '__main__':
    logging.basicConfig()
    app.run(host='0.0.0.0', port=5000)
