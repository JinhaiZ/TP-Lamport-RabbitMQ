#!/usr/bin/env python
import logging
import pika
import sys
import json
import time
from multiprocessing import Process, Pipe
from consumer import ExampleConsumer
from publisher import Publisher

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)


def start_consumer(queue_name, exchange_names):
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    example = ExampleConsumer(queue_name, exchange_names)
    try:
        example.run()
    except KeyboardInterrupt:
        example.stop()


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("usage: python site.py its_queue_name binding_exchange_name1,binding_exchange_name2 its_exchange_name")
    else:
        p = Process(target=start_consumer, args=(sys.argv[1], sys.argv[2].split(',')))
        p.start()
        queue_name = sys.argv[2]
        exchange_name = sys.argv[3]
        pub = Publisher(exchange_name, queue_name)

        p.join()