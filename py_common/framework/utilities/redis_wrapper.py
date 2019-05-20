# coding:utf-8
__author__ = 'HuangZhi'


from gevent import monkey
# patches stdlib (including socket and ssl modules) to cooperate with other greenlets
monkey.patch_all()

import redis
from gevent.lock import BoundedSemaphore as Lock
from framework.utilities.lock_pool_wrapper import LockPool
import time
from framework.utilities import little_tools


class RedisConnPool():
    def __init__(self, max_conn=10, host='127.0.0.1,', port=6379, db=0):
        self.pool_ready = []
        self.pool_busy = []
        self.host = host
        self.port = port
        self.db = db
        self.max_conn = max_conn
        self.lock = Lock(self.max_conn)

    def setServer(self, host=None, port=None, db=None):
        self.host = host if (host is not None) else self.host
        self.port = port if (port is not None) else self.port
        self.db = db if (db is not None) else self.db

    def setMaxConn(self, max_conn):
        self.max_conn = max_conn
        self.lock = Lock(self.max_conn)

    def fetchConnection(self, batch_mode=False):
        rd_conn = None
        self.lock.acquire()
        if len(self.pool_ready) > 0:
            rd_conn = self.pool_ready.pop()
        elif (len(self.pool_ready) + len(self.pool_busy)) < self.max_conn:
            rd_conn = self._newConnection()
        if rd_conn is not None:
            self.pool_busy.append(rd_conn)
            return RedisConn(self, rd_conn, batch_mode)
        self.lock.release()
        return None

    def _newConnection(self):
        return redis.Redis(host=self.host, port=self.port, db=self.db)

    def _freeConnection(self, rd_conn):
        self.pool_busy.remove(rd_conn)
        self.pool_ready.insert(0, rd_conn)
        self.lock.release()


class RedisConn(object):
    def __init__(self, pool, rd_conn, batch_mode):
        self.pool = pool
        self.rd_conn = rd_conn
        self.rd_pipeline = self.rd_conn.pipeline() if batch_mode else None

    def __getattr__(self, item):
        if self.rd_pipeline is None:
            return self.rd_conn.__getattribute__(item)
        else:
            return self.rd_pipeline.__getattribute__(item)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        if self.rd_pipeline is not None:
            self.rd_pipeline.reset()
        self.pool._freeConnection(self.rd_conn)


class RedisProxy(object):
    """
    redis 操作代理
    """
    def __init__(self, rd_conn_pool):
        self.conn_pool = rd_conn_pool
        self.lock_pool = LockPool(rd_conn_pool.max_conn)
        self.key_locks = {}

    def acquire(self, key):
        lock = self.key_locks.get(key)
        if not lock:
            lock = self.lock_pool.get_lock()
            self.key_locks[key] = lock
        lock.acquire()

    def release(self, key):
        lock = self.key_locks.pop(key)
        if not lock:
            return None
        lock.release()

    def get_connection(self):
        return self.conn_pool.fetchConnection()

    def pipeline(self):
        self.get_connection().pipeline()

    def delete(self, *names):
        with self.conn_pool.fetchConnection() as conn:
            return conn.delete(*names)

    def expireat(self, name, when):
        with self.conn_pool.fetchConnection() as conn:
            return conn.expireat(name, when)

    def expire(self, name, time):
        with self.conn_pool.fetchConnection() as conn:
            return conn.expire(name, time)

    def ttl(self, name):
        with self.conn_pool.fetchConnection() as conn:
            return conn.ttl(name)

    def get(self, name):
        with self.conn_pool.fetchConnection() as conn:
            return conn.get(name)

    def set(self, name, value):
        with self.conn_pool.fetchConnection() as conn:
            return conn.set(name, value)

    def mget(self, keys, *args):
        with self.conn_pool.fetchConnection() as conn:
            return conn.mget(keys, *args)

    def zadd(self, name, *args, **kwargs):
        with self.conn_pool.fetchConnection() as conn:
            return conn.zadd(name, *args, **kwargs)

    def zincrby(self, name, value, increment):
        with self.conn_pool.fetchConnection() as conn:
            return conn.zincrby(name, value, increment)

    def zrank(self, name, value):
        with self.conn_pool.fetchConnection() as conn:
            return conn.zrank(name, value)

    def zscore(self, name, value):
        with self.conn_pool.fetchConnection() as conn:
            return conn.zscore(name, value)

    def zcard(self, name):
        with self.conn_pool.fetchConnection() as conn:
            return conn.zcard(name)

    def zrem(self, name, *values):
        with self.conn_pool.fetchConnection() as conn:
            return conn.zrem(name, *values)

    def zcount(self, name, min_score, max_score):
        with self.conn_pool.fetchConnection() as conn:
            return conn.zcount(name, min_score, max_score)

    def zrange(self, name, start, stop, desc=False, withscore=False, score_cast_func=int):
        with self.conn_pool.fetchConnection() as conn:
            return conn.zrange(name, start, stop, desc, withscore, score_cast_func)

    def zrevrange(self, name, start, stop, withscore=False, score_cast_func=int):
        with self.conn_pool.fetchConnection() as conn:
            return conn.zrevrange(name, start, stop, withscore, score_cast_func)

    def zremrangebyscore(self, name, min_score, max_score):
        with self.conn_pool.fetchConnection() as conn:
            return conn.zremrangebyscore(name, min_score, max_score)

    def lrange(self, name, start, stop):
        with self.conn_pool.fetchConnection() as conn:
            return conn.lrange(name, start, stop)

    def rpush(self, name, *values):
        with self.conn_pool.fetchConnection() as conn:
            return conn.rpush(name, *values)

    def lpop(self, name):
        with self.conn_pool.fetchConnection() as conn:
            return conn.lpop(name)

    def llen(self, name):
        with self.conn_pool.fetchConnection() as conn:
            return conn.llen(name)

    def lset(self, name, index, value):
        with self.conn_pool.fetchConnection() as conn:
            return conn.lset(name, index, value)

    def sadd(self, name, *values):
        with self.conn_pool.fetchConnection() as conn:
            return conn.sadd(name, *values)

    def sismember(self, name, value):
        with self.conn_pool.fetchConnection() as conn:
            return conn.sismember(name, value)

    def hget(self, name, key):
        with self.conn_pool.fetchConnection() as conn:
            return conn.hget(name, key)

    def hset(self, name, key, value):
        with self.conn_pool.fetchConnection() as conn:
            return conn.hset(name, key, value)

    def hmset(self, name, mapping):
        with self.conn_pool.fetchConnection() as conn:
            return conn.hmset(name, mapping)

    def hmget(self, name, keys, *args):
        with self.conn_pool.fetchConnection() as conn:
            return conn.hmget(name, keys, *args)

    def hgetall(self, name):
        with self.conn_pool.fetchConnection() as conn:
            return conn.hgetall(name)

    def hdel(self, name, *keys):
        with self.conn_pool.fetchConnection() as conn:
            return conn.hdel(name, *keys)