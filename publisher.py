#!/usr/bin/env python
import pika
import logging
import sys
import time


LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

class Publisher(object):

    def __init__(self, exchange_name, queue_name):
        self._exchange_name = exchange_name
        self._queue_name = queue_name

        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self._connection = connection
        channel = connection.channel()

        channel.exchange_declare(exchange=exchange_name,
                                exchange_type='fanout')
        self._channel = channel

        # lamport
        self._site_id = int(exchange_name[1:])
        
    def send_REQUEST(self, time):
        message = "{!s},{!s}".format(self._site_id,time)
        self._channel.basic_publish(exchange=self._exchange_name,
                            routing_key='',
                            body=message,
                            properties=pika.BasicProperties(reply_to=self._queue_name,type="REQUEST"))
        LOGGER.info('Broadcasted message : %s type REQUEST', message)

    def close_connection(self):
        self._connection.close()


def main(exchange_name, queue_name):
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    pub = Publisher(exchange_name, queue_name)
    try:
        pub.send_REQUEST(0)
        # time.sleep(2)
        # pub.send_RELEASE(3)
    except KeyboardInterrupt:
        pub.close_connection()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("usage: python publisher.py its_exchange_name its_queue_name")
    else:
        main(sys.argv[1], sys.argv[2])
