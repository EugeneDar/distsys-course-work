from caption import get_image_caption
import pika
import time
import json


if __name__ == '__main__':
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', port=5672))

    result_channel = connection.channel()
    result_channel.queue_declare(queue='result_queue', durable=True)
    # result_channel.confirm_delivery()

    task_channel = connection.channel()
    task_channel.queue_declare(queue='task_queue', durable=True)

    def callback(channel, method, properties, body):
        print(" [x] Received %r" % body.decode())
        print(" [x] Done")

        input_message = json.loads(body.decode())

        result = get_image_caption(input_message['image'])

        output_message = str({
            'id': input_message['id'],
            'result': result,
        }).replace('\'', '\"')

        while True:
            try:
                result_channel.basic_publish(
                    exchange='',
                    routing_key='result_queue',
                    body=output_message,
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                    )
                )
            except Exception as e:
                time.sleep(1)
            else:
                break

        # TODO do it before
        channel.basic_ack(delivery_tag=method.delivery_tag)  # Send ack

    task_channel.basic_qos(prefetch_count=1)
    task_channel.basic_consume(queue='task_queue', on_message_callback=callback)

    print(' [*] Waiting for messages.')
    task_channel.start_consuming()
