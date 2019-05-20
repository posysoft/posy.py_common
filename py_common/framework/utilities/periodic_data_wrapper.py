# coding:utf-8
__author__ = '. at 13-8-6'

from framework.framework import Framework
from framework.utilities.redis_wrapper import RedisProxy
from framework.utilities.lock_pool_wrapper import LockPool
import time


class PeriodicDataManager(object):
    """
    周期性变化redis数据管理器
    redis数据类型需为：string
    """
    def __init__(self, key_format, lock_pool_size):
        """
        key_format : str  "promotion_{role_id}_{prop_type}"
        lock_pool_size : int
        """
        self.conn_pool = Framework.retrieveGlobal('rds')
        self.key_format = key_format
        self.lock_pool = LockPool(lock_pool_size)
        self.data_lock = {}
        self.redis = RedisProxy(self.conn_pool)

    # 获取redis的key
    def get_keys(self, key_points):
        """
        key_points : dict
        """
        rd_key = self.key_format.format(**key_points)
        with self.conn_pool.fetchConnection() as rd_conn:
            keys = rd_conn.keys(rd_key)
            return keys

    # 获取数据
    def get_value(self, key_points):
        """
        key_points : dict
        """
        rd_key = self.key_format.format(**key_points)
        with self.conn_pool.fetchConnection() as rd_conn:
            v = rd_conn.get(rd_key)
            return v

    # 获取多key数据
    def get_values(self, keys):
        """
        keys : list of key
        """
        if not keys:
            return {}
        with self.conn_pool.fetchConnection() as rd_conn:
            vs = rd_conn.mget(*keys)
            d = {}
            for i in xrange(len(keys)):
                key = keys[i]
                val = vs[i]
                d[key] = val
            return d

    # 设置值
    def set_value(self, key_points, value, period=None, start=None):
        """
        key_points : dict
        value
        period : int sec
        start : int sec
        """
        rd_key = self.key_format.format(**key_points)
        with self.conn_pool.fetchConnection() as rd_conn:
            rd_conn.set(rd_key, value)
            if period:
                if start:
                    period = max(0, int(start + period - time.time()))
                rd_conn.expire(rd_key, period)
        return True

    # 删除键值
    def delete_keys(self, *args):
        rd_keys = []
        for key_points in args:
            rd_key = self.key_format.format(**key_points)
            rd_keys.append(rd_key)
        with self.conn_pool.fetchConnection() as rd_conn:
            rd_conn.delete(*rd_keys)

    # 获取过期时间
    def get_expire_time(self, key_points):
        rd_key = self.key_format.format(**key_points)
        with self.conn_pool.fetchConnection() as rd_conn:
            remain = rd_conn.ttl(rd_key)
            return remain

    def lrange(self, key_points, start, stop):
        rd_key = self.key_format.format(**key_points)
        return self.redis.lrange(rd_key, start, stop)

    # 获取数据互斥锁
    def _acquire(self, key):
        lock = self.data_lock.get(key, None)
        if not lock:
            lock = self.lock_pool.get_lock()
            self.data_lock[key] = lock
        lock.acquire()

    # 释放互斥锁
    def _release(self, key):
        lock = self.data_lock.pop(key, None)
        if not lock:
            return True
        lock.release()

    def acquire_key(self, key_points):
        rd_key = self.key_format.format(**key_points)
        self._acquire(rd_key)

    def release_key(self, key_points):
        rd_key = self.key_format.format(**key_points)
        self._release(rd_key)

    def increase_counter(self, key_points, inc_count, limit, period_time, start=None):
        rd_key = self.key_format.format(**key_points)
        self._acquire(rd_key)
        try:
            with self.conn_pool.fetchConnection() as rd_conn:
                cur_count = rd_conn.get(rd_key)
                if not cur_count:
                    cur_count = 0
                    rd_conn.set(rd_key, cur_count)
                    if start:
                        period_time = max(0, int(start + period_time - time.time()))
                    rd_conn.expire(rd_key, period_time)
                else:
                    cur_count = int(cur_count)
                diff = limit - cur_count - inc_count
                if diff < 0:
                    return diff
                rd_conn.incr(rd_key, inc_count)
                return diff
        finally:
            self._release(rd_key)

    def reset_counter(self, key_points, value, period_time, start=None):
        rd_key = self.key_format.format(**key_points)
        self._acquire(rd_key)
        try:
            with self.conn_pool.fetchConnection() as rd_conn:
                rd_conn.set(rd_key, value)
                if start:
                    period_time = max(0, int(start + period_time - time.time()))
                rd_conn.set(rd_key, period_time)
        finally:
            self._release(rd_key)
        return True