#!/usr/bin/env python
import pika
import sys
import time

def main(exchange_name):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange=exchange_name,
                            exchange_type='fanout')
    count = 1
    while (True):
        message = "msg {} from {}".format(str(count), exchange_name)
        channel.basic_publish(exchange=exchange_name,
                            routing_key='',
                            body=message)
        print(" [x] Sent %r" % message)
        count += 1
        time.sleep(2)

if __name__ == '__main__':
    exchange_name = sys.argv[1]
    main(exchange_name)
