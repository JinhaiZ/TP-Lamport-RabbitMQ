#!/usr/bin/env python
from heapq import heappush, heappop

class RequestQueue(object):

    def __init__(self):
        self._pq = []                         # list of entries arranged in a heap
        self._entry_finder = {}               # mapping of tasks to entries
        self._REMOVED = '<removed-task>'      # placeholder for a removed task

    def add_request(self, site, time):
        'Add a new request from a site, no duplicate request from a same site'
        if type(site) is str:
            site = int(site)
        if type(time) is str:
            time = int(time)    
        if site in self._entry_finder:
            return
        entry = [time, site]
        self._entry_finder[site] = entry
        heappush(self._pq, entry)

    def remove_request(self, site):
        'Mark an existing request from a site as REMOVED. Raise KeyError if not found.'
        entry = self._entry_finder.pop(site)
        entry[-1] = self._REMOVED

    def pop_request(self):
        'Remove and return the lowest logical time request. Raise KeyError if empty.'
        while self._pq:
            time, site = heappop(self._pq)
            if site is not self._REMOVED:
                del self._entry_finder[site]
                return site
        raise KeyError('pop from an empty priority queue')

    def peek_request(self):
        'Peek the lowest logical time request. Raise KeyError if empty.'
        while self._pq:
            if len(self._pq) < 1:
                raise KeyError('pop from an empty priority queue')
                return
            time, site = self._pq[0]
            if site is self._REMOVED:
                heappop(self._pq)
                del self._entry_finder[site]
            else:
                return site
        raise KeyError('pop from an empty priority queue')
    
    def size(self):
        return len(self._pq)

if __name__ == "__main__":
    requestQ = RequestQueue()
    requestQ.add_request(3,0)
    requestQ.add_request(1,1)
    requestQ.add_request(2,1)
    for i in range(3):
       print requestQ.peek_request()
       print requestQ.pop_request()

