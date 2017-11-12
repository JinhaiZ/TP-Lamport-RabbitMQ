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

class Site(object):
    
    def __init__(self, site_id, site_number):
        self._its_queue_name = "Q{!s}".format(site_id)
        self._its_exchange_name = "X{!s}".format(site_id)
        exchange_names = []
        for i in range(1, site_number+1):
            if i != site_id:
                exchange_names.append("X{!s}".format(i))
        self._binding_exchange_names = exchange_names
        print(self._its_queue_name, self._its_exchange_name,self._binding_exchange_names)

    def start_consumer(self):
        logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
        example = ExampleConsumer(self._its_queue_name, self._binding_exchange_names)
        try:
            example.run()
        except KeyboardInterrupt:
            example.stop()


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("usage: python site.py its_id total_count_of_sites")
    else:
        site = Site(int(sys.argv[1]), int(sys.argv[2]))
        p = Process(target=site.start_consumer, args=())
        p.start()
        # queue_name = sys.argv[2]
        # exchange_name = sys.argv[3]
        # pub = Publisher(exchange_name, queue_name)

        p.join()
