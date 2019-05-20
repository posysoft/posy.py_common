# coding:utf-8
__author__ = 'HuangZhi'

from thrift.transport import TSocket
from time_driver import time_driver
from framework.hbase.hbase import Hbase
from framework.hbase.hbase.ttypes import *
import time
from gevent.queue import PriorityQueue


class HbaseConnPool():
    def __init__(self, host='127.0.0.1', port=9090, max_conn=100, timeout=30):
        self.log = None
        self.host = host
        self.port = int(port)
        self.pool_size = max_conn
        self.timeout = timeout
        self.conn_pool = PriorityQueue(self.pool_size)
        self.conn_count = 0
        self.fetchers = 0
        self.waiting = 0
        interval = timeout / 2 or timeout
        time_driver.looping_call(interval, interval, self.clear_timeout_connection)

    def set_log(self, log):
        self.log = log

    def _fetchConnection(self):
        transport = TSocket.TSocket(self.host, self.port)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()
        conn = HbaseConn(self, client, transport)
        return conn

    def fetchConnection(self):
        self.waiting += 1
        if self.log:
            self.log.debug('qsize %s, fetchers %s, conn count %s, waiting %s' % (self.conn_pool.qsize(), self.fetchers, self.conn_count, self.waiting))
        if self.conn_pool.qsize() <= 0 and self.conn_count < self.pool_size:
            try:
                self.conn_count += 1
                conn = self._fetchConnection()
            except Exception, e:
                self.conn_count -= 1
                self.waiting -= 1
                raise e
        else:
            conn = self.conn_pool.get()
        conn.last_fetch_time = time.time()
        self.fetchers += 1
        self.waiting -= 1
        return conn

    def putConnection(self, conn, exc):
        errmsg = ''
        if not exc[-1]:
            self.conn_pool.put(conn)
        else:
            try:
                conn.close()
            except Exception, e:
                if self.log:
                    self.log.exception(e.message)
                else:
                    print e.message
            del conn
            self.conn_count -= 1
            errmsg = ', error: %s' % exc[1]
        self.fetchers -= 1
        if self.log:
            self.log.debug('put conn back, qsize %s, fetchers %s, conn count %s, waiting %s%s' %
                           (self.conn_pool.qsize(), self.fetchers, self.conn_count, self.waiting, errmsg))

    def clear_timeout_connection(self):
        expire_time = time.time() - self.timeout
        for i in xrange(-1, -self.conn_pool.qsize() - 1, -1):
            try:
                conn = self.conn_pool.queue[i]
                if conn.last_fetch_time < expire_time:
                    self.conn_pool.queue.remove(conn)
                    self.conn_count -= 1
                    conn.close()
                    if self.log:
                        self.log.debug('recyle conn, qsize %s, fetchers %s, conn count %s, waiting %s' %
                           (self.conn_pool.qsize(), self.fetchers, self.conn_count, self.waiting))
                else:
                    break
            finally:
                pass


class HbaseConn(object):
    def __init__(self, pool, client, transport):
        self.pool = pool
        self.client = client
        self.transport = transport
        self.last_fetch_time = 0

    def __getattr__(self, item):
        if item in ():
            return getattr(self.client, item)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.putConnection(self, (exc_type, exc_val, exc_tb))

    def __lt__(self, other):
        return self.last_fetch_time > other.last_fetch_time

    def close(self):
        self.transport.close()

    def scan(self, table, by_cell=False, startRow=None, stopRow=None,
             timestamp=None, columns=None, caching=100, filterString=None, sortColumns=None, attributes=None):
        scan = TScan(startRow, stopRow, timestamp, columns, caching, filterString, sortColumns)
        scanner = self.client.scannerOpenWithScan(table, scan, attributes)
        while True:
            rs = self.client.scannerGetList(scanner, caching if caching else 100)
            if not rs:
                break
            for r in rs:
                if by_cell:
                    for cv in r.columns.iteritems():
                        yield (r.row, cv[0], cv[1].value, cv[1].timestamp)
                else:
                    yield (r.row, dict(((cv[0], cv[1].value) for cv in r.columns.iteritems())))
        self.client.scannerClose(scanner)

    def get(self, table, key, column, cf=None, attributes=None):
        cvs = self.get_mc(table, key, (column,), cf, attributes)
        return cvs.get(column, None)

    def set(self, table, key, column, value, cf=None, attributes=None):
        return self.set_mc(table, key, ((column, value),), cf, attributes)

    def remove(self, table, key, column, cf=None, attributes=None):
        return self.remove_mc(table, key, (column,), cf, attributes)

    def get_ver(self, table, key, column, numVersions, attributes=None, value_only=True):
        rs_mv = self.client.getVer(table, key, column, numVersions, attributes)
        if rs_mv:
            return [{'timestamp': rs.timestamp, 'value': rs.value} for rs in rs_mv] if not value_only else \
                [rs.value for rs in rs_mv]
        return []

    def get_mc(self, table, key, columns=None, cf=None, attributes=None, value_only=True):
        rs = self.client.getRowWithColumns(table, key,
                                           [cf+':'+col if cf else col for col in columns] if columns else None,
                                           attributes)
        if rs:
            cf_len = len(cf)+1 if cf else 0
            if not value_only:
                return dict(((cv[0][cf_len:], {'timestamp': cv[1].timestamp, 'value': cv[1].value})
                                 for cv in rs[0].columns.iteritems()))
            else:
                return dict(((cv[0][cf_len:], cv[1].value) for cv in rs[0].columns.iteritems()))
        return {}

    def get_mr(self, table, keys, columns=None, cf=None, attributes=None, value_only=True):
        columns = columns if columns else []
        cf = '%s:' % cf if cf else ''
        clms = ['%s%s' % (cf, clm) for clm in columns]
        rs = self.client.getRowsWithColumns(table, keys, clms, attributes)
        rows = {}
        for rw in rs:
            clmos = {}
            for clm, cell in rw.columns.iteritems():
                clmos[clm] = cell.value if value_only else {'timestamp': cell.timestamp, 'value': cell.value}
            rows[rw.row] = clmos
        return rows

    def set_mc(self, table, key, cv_pairs, cf=None, attributes=None):
        return self.client.mutateRow(table, key, [
            Mutation(column=cf+':'+cv[0] if cf else cv[0], value=cv[1]) for cv in cv_pairs
        ], attributes)

    def remove_mc(self, table, key, columns, cf=None, attributes=None):
        return self.client.mutateRow(table, key, [
            Mutation(column=cf+':'+col if cf else col, isDelete=True) for col in columns
        ], attributes)

    def remove_row(self, table, row, attributes=None):
        return self.client.deleteAllRow(table, row, attributes)

    def list_tables(self):
        tables = self.client.getTableNames()
        return tables

    def list_columns(self, table):
        cols = self.client.getColumnDescriptors(table)
        return cols
