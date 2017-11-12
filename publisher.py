#!/usr/bin/env python
import pika
import logging
import sys
import time


LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

class ExamplePublisher(object):

    def __init__(self, exchange_name, queue_name):
        self._exchange_name = exchange_name
        self._queue_name = queue_name

        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self._connection = connection
        channel = connection.channel()

        channel.exchange_declare(exchange=exchange_name,
                                exchange_type='fanout')
        self._channel = channel
        properties = pika.BasicProperties(reply_to=queue_name)
                                        #content_type='application/json',
                                        #headers=hdrs)
        
    def broadcast(self, message):
        self._channel.basic_publish(exchange=self._exchange_name,
                            routing_key='',
                            body=message)
        LOGGER.info('Broadcasted message : %s', message)

    def send_ack(self, dest_queue, message):
        self._channel.basic_publish(exchange='',
                            routing_key=dest_queue,
                            body=message)

    def close_connection(self):
        self._connection.close()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("usage: python publisher.py its_exchange_name its_queue_name")
    else:
        exchange_name = sys.argv[1]
        queue_name = sys.argv[2]
        ExamplePublisher(exchange_name, queue_name)
