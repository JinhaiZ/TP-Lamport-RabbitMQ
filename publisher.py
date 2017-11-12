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
        properties = pika.BasicProperties(reply_to=queue_name)
                                        #content_type='application/json',
                                        #headers=hdrs)
        self._properties = properties
        
    def broadcast(self, message):
        self._channel.basic_publish(exchange=self._exchange_name,
                            routing_key='',
                            body=message,
                            properties=self._properties)
        LOGGER.info('Broadcasted message : %s', message)

    def send_ack(self, dest_queue, message):
        self._channel.basic_publish(exchange='',
                            routing_key=dest_queue,
                            body=message)

    def close_connection(self):
        self._connection.close()


def main(exchange_name, queue_name):
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    pub = Publisher(exchange_name, queue_name)
    count = 1
    try:
        while (True):
            pub.broadcast("msg # {!s} from {}".format(count,exchange_name))
            #pub.send_ack("Q2", "ACK from{}".format(exchange_name))
            time.sleep(2)
            count += 1
    except KeyboardInterrupt:
        pub.close_connection()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("usage: python publisher.py its_exchange_name its_queue_name")
    else:
        main(sys.argv[1], sys.argv[2])
