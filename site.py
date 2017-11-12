#!/usr/bin/env python
import logging
import pika
import sys
import json
import time
from multiprocessing import Process, Pipe, Value, Array, Lock
from multiprocessing.managers import BaseManager 
from consumer import ExampleConsumer
from publisher import Publisher
from requestQ import RequestQueue

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

class requestQManager(BaseManager):  
    pass

class Site(object):
    
    def __init__(self, site_id, site_number):
        'parse parameter, create sharing object between process'
        self._its_queue_name = "Q{!s}".format(site_id)
        self._its_exchange_name = "X{!s}".format(site_id)
        exchange_names = []
        for i in range(1, site_number+1):
            if i != site_id:
                exchange_names.append("X{!s}".format(i))
        self._binding_exchange_names = exchange_names
        print(self._its_queue_name, self._its_exchange_name,self._binding_exchange_names)
        # declare sharing object
        requestQManager.register('RequestQueue', RequestQueue, exposed = ['add_request', 'pop_request', 'peek_request', 'size'])  
        self._lock = Lock()
        self._logical_time = Value('i', 0)
        self._replys = Array('i', [0 for i in range(site_number)], lock=self._lock)
        self._mymanager = requestQManager()
        self._mymanager.start()
        self._requestQ = self._mymanager.RequestQueue()       

    def start_consumer(self):
        example = ExampleConsumer(self._its_queue_name,
            self._binding_exchange_names, self._lock, self._logical_time,
            self._requestQ, self._replys)
        try:
            example.run()
        except KeyboardInterrupt:
            example.stop()

    def run_consumer_process(self):
        self._p = Process(target=self.start_consumer, args=())
        self._p.start()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("usage: python site.py its_id total_count_of_sites")
    else:
        logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
        site = Site(int(sys.argv[1]), int(sys.argv[2]))
        #p = Process(target=site.start_consumer, args=())
        site.run_consumer_process()
        # queue_name = sys.argv[2]
        # exchange_name = sys.argv[3]
        # pub = Publisher(exchange_name, queue_name)

        site._p.join()
