from multiprocessing import Process, Condition, Lock  
from multiprocessing.managers import BaseManager  
import time, os  
from requestQ import RequestQueue

lock = Lock()  

class requestQManager(BaseManager):  
    pass  

requestQManager.register('RequestQueue', RequestQueue, exposed = ['add_request', 'pop_request', 'seek_request'])  

def consume(requestQ):
    time.sleep(2)
    lock.acquire()
    for i in range(3):
        print requestQ.pop_request()
    lock.release()  


def main():  
    mymanager = requestQManager()
    mymanager.start()

    requestQ = mymanager.RequestQueue()
    consumer = Process(target = consume, args =(requestQ,))
    consumer.start()

    lock.acquire()  
    requestQ.add_request(3,0)
    requestQ.add_request(1,1)
    requestQ.add_request(2,1)
    lock.release()  
    
    consumer.join()

main()
