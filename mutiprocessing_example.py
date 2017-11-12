from multiprocessing import Process, Condition, Lock, Value  
from multiprocessing.managers import BaseManager  
import time, os  
from requestQ import RequestQueue

lock = Lock()  

class requestQManager(BaseManager):  
    pass

requestQManager.register('RequestQueue', RequestQueue, exposed = ['add_request', 'pop_request', 'peek_request'])  

def consume(requestQ, logical_time):
    time.sleep(2)
    lock.acquire()
    for i in range(3):
        print requestQ.peek_request()
        print requestQ.pop_request()
    lock.release() 
    # while(True):
    #     lock.acquire()
    #     logical_time.value += 1
    #     lock.release()
    #     time.sleep(1) 


def main():  
    mymanager = requestQManager()
    mymanager.start()

    requestQ = mymanager.RequestQueue()
    logical_time = Value('i', 0)
    consumer = Process(target = consume, args =(requestQ,logical_time))
    consumer.start()

    lock.acquire()  
    requestQ.add_request(3,0)
    requestQ.add_request(1,1)
    requestQ.add_request(2,1)
    print "time: ",logical_time.value
    lock.release()  
    # while(True):
    #     print "time: ",logical_time.value
    #     time.sleep(2)
    consumer.join()
    

main()
