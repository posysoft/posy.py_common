# coding:utf-8
__author__ = '. at 13-8-6'
from gevent.lock import BoundedSemaphore


class LockPool(object):
    """
    互斥锁池
    """
    def __init__(self, pool_size):
        self.lock = BoundedSemaphore(pool_size)
        self.ready_queue = []
        self.busy_queue = []
        self.pool_size = pool_size

    def get_lock(self):
        self.lock.acquire()
        if self.ready_queue:
            lock = self.ready_queue.pop()
        elif len(self.ready_queue) + len(self.busy_queue) < self.pool_size:
            lock = BoundedSemaphore()
        else:
            lock = None
        if lock:
            self.busy_queue.append(lock)
            return MutexLock(self, lock)
        self.lock.release()

    def put_lock(self, lock):
        self.ready_queue.append(lock)
        self.busy_queue.remove(lock)
        self.lock.release()


class MutexLock(object):
    """
    互斥锁
    """
    def __init__(self, lock_pool, lock):
        self.lock_pool = lock_pool
        self.lock = lock

    def acquire(self):
        self.lock.acquire()

    def release(self):
        self.lock.release()
        self.lock_pool.put_lock(self.lock)