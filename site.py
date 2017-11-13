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
        if type(site_id) is str:
            site_id = int(site_id)
        if type(site_number) is str:
            site_number = int(site_number)
        self._site_id = site_id
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

    def start_publisher(self):
        self._publisher = Publisher(self._its_exchange_name, self._its_queue_name)

    def request_for_critical_section(self):
        self._requestQ.add_request(self._site_id, self._logical_time.value)
        self._publisher.send_REQUEST(self._logical_time.value)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("usage: python site.py its_id total_count_of_sites")
    else:
        logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
        site = Site(sys.argv[1], sys.argv[2])
        #p = Process(target=site.start_consumer, args=())
        site.run_consumer_process()
        site.start_publisher()
        if sys.argv[1] == '1':
            time.sleep(1)
            site.request_for_critical_section()
        site._p.join()
