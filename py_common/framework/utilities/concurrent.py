# coding:utf-8
__author__ = 'HuangZhi'

import threading
import gevent
from collections import deque as Queue


class WorkerPool(object):
    def __init__(self, worker_count, auto_start=False, real_thread=True):
        super(WorkerPool, self).__init__()
        self.job_queue = Queue()
        self.job_map = {}
        self.job_sem = threading.Semaphore(value=0) if real_thread else gevent.lock.Semaphore(value=0)
        self.threads = [(threading.Thread(target=self._worker) if real_thread else gevent.Greenlet(run=self._worker)) \
                        for i in range(worker_count)]

        if auto_start:
            self.start()

    def start(self):
        for t in self.threads:
            t.start()

    def addJob(self, proc, args=None, kwargs=None, key=None):
        job = {
            'key': key,
            'proc': proc,
            'args': args if args else (),
            'kwargs': kwargs if kwargs else {}
        }
        if key is not None:
            self.job_map[key] = job
        self.job_queue.append(job)
        self.job_sem.release()

    def getJobResult(self, key):
        return self.job_map[key]['result']

    def purge(self):
        if len(self.threads):
            for i in range(len(self.threads)):
                self.job_sem.release()
            for t in self.threads:
                t.join()
        else:
            self.job_sem.release()
            self._worker()

    def _worker(self):
        while True:
            self.job_sem.acquire()
            try:
                job = self.job_queue.popleft()
            except IndexError:
                #print 'worker exited'
                return
            #print 'worker working'
            job['result'] = job['proc'](*job['args'], **job['kwargs'])

