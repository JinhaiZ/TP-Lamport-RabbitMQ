from multiprocessing import Process
import os
import time

def info(title):
    print title
    print 'module name:', __name__
    if hasattr(os, 'getppid'):  # only available on Unix
        print 'parent process:', os.getppid()
    print 'process id:', os.getpid()

def f(name):
    info('function f')
    print 'hello', name
    while(True):
        print("listen msg")
        time.sleep(1)

if __name__ == '__main__':
    info('main line')
    p = Process(target=f, args=('consumer',))
    p.start()
    
    while(True):
        print("send msg")
        time.sleep(3)
    p.join()