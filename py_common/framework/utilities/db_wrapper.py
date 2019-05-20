# coding:utf-8
__author__ = 'HuangZhi'


import gevent
from gevent import monkey
# patches stdlib (including socket and ssl modules) to cooperate with other greenlets
monkey.patch_all()
#monkey.patch_socket()

import mysql.connector
import types
from gevent.lock import BoundedSemaphore as Lock

# refresh connections for each 5 minutes
CONN_REFRESH_INTERVAL = 300


class DBConnPool():
    def __init__(self, max_conn=10, host='127.0.0.1', port=3306, user='root', pwd='', charset=33):
        self.pool_ready = []
        self.host = host
        self.port = port
        self.user = user
        self.pwd = pwd
        self.charset = charset
        self.max_conn = max_conn
        self.lock = Lock(self.max_conn)
        gevent.spawn(self._procRefreshConn)

    def _procRefreshConn(self):
        while True:
            gevent.sleep(CONN_REFRESH_INTERVAL)
            conns = []
            while len(self.pool_ready) > 0:
                conn = self._fetchConnection()
                if not conn.is_connected():
                    #print 'database connection disconnected'
                    conn = None
                conns.append(conn)
            while len(conns) > 0:
                self._freeConnection(conns.pop())

    def setServer(self, host=None, port=None, user=None, pwd=None, charset=None):
        self.host = host if (host is not None) else self.host
        self.port = port if (port is not None) else self.port
        self.user = user if (user is not None) else self.user
        self.pwd = pwd if (pwd is not None) else self.pwd
        self.charset = charset if (charset is not None) else self.charset

    def setMaxConn(self, max_conn):
        self.max_conn = max_conn
        self.lock = Lock(self.max_conn)

    def fetchConnection(self):
        db_conn = self._fetchConnection()
        return DBConn(self, db_conn) if db_conn else None

    def _fetchConnection(self):
        db_conn = None
        self.lock.acquire()
        if len(self.pool_ready) > 0:
            db_conn = self.pool_ready.pop()
        else:
            db_conn = self._newConnection()
        if db_conn is not None:
            return db_conn
        self.lock.release()
        return None

    def _newConnection(self):
        return mysql.connector.connect(
            host=self.host, port=self.port, user=self.user, password=self.pwd, charset=self.charset, autocommit=True)

    def _freeConnection(self, db_conn):
        if db_conn:
            self.pool_ready.insert(0, db_conn)
        self.lock.release()


class DBConn(object):
    def __init__(self, pool, db_conn):
        self.pool = pool
        self.db_conn = db_conn
        self.db_cursor = db_conn.cursor(buffered=True)

    def __getattr__(self, item):
        if item == 'column_names':
            return self.db_cursor.column_names
        if item == 'statement':
            return self.db_cursor.statement
        if item == 'row_count':
            return self.db_cursor.rowcount
        if 'fetch_all' == item:
            return self.db_cursor.fetchall
        if 'fetch_one' == item:
            return self.db_cursor.fetchone
        raise AttributeError

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.db_cursor.close()
        self.pool._freeConnection(self.db_conn)

    def execute(self, sql, params=None):
        if params is not None:
            sql = sql.encode('utf-8') % self._processParams(params)
        print 'execute sql:"%s"' % sql
        return self.db_cursor.execute(sql)

    def fetch_row(self, value_only=True):
        r = self.db_cursor.fetchone()
        if r is None:
            return None
        if value_only:
            return r
        return dict(((f, r[i]) for i,f in enumerate(self.db_cursor.column_names)))

    def iter_rows(self, value_only=True):
        while True:
            r = self.fetch_row(value_only)
            if r is None:
                break
            yield r

    def _processParams(self, params):
        try:
            to_mysql = self.db_conn.converter.to_mysql
            escape = self.db_conn.converter.escape
            quote = self.db_conn.converter.quote
            res = {}
            for k, v in params.items():
                c = v
                c = to_mysql(c)

                if k[:1] == '_':
                    c = escape(c)
                    c = '`' + c + '`'
#                elif (type(c) in types.StringTypes):\
                elif k[:1] == '+':
                    pass
                else:
                    c = escape(c)
                    c = quote(c)
                res[k] = c
        except StandardError, e:
            raise e
        else:
            return res
        return None

    def select_row(self, db, tb, condition, fields=None):
        field_clauses = ('`'+'`,`'.join(fields)+'`') if fields else '*'
        sql = 'SELECT %s FROM `%s`.`%s` WHERE ' % (field_clauses, db, tb)

        where_clauses = []
        for f, v in condition.items():
            where_clauses.append('`' + f + '`=%(' + f + ')s')
        sql += ' AND '.join(where_clauses)
        self.execute(sql, condition)
        return self.row_count

    def insert_row(self, db, tb, **fvs):
        sql = 'INSERT `%s`.`%s` SET ' % (db, tb)
        clauses = []
        for f in fvs.keys():
            clauses.append('`' + f + '`=%(' + f + ')s')
        sql += ','.join(clauses)
        self.execute(sql, fvs)
        return self.row_count

    def replace_row(self, db, tb, **fvs):
        sql = 'REPLACE `%s`.`%s` SET ' % (db, tb)
        clauses = []
        for f in fvs.keys():
            clauses.append('`' + f + '`=%(' + f + ')s')
        sql += ','.join(clauses)
        self.execute(sql, fvs)
        return self.row_count

    def update_row(self, db, tb, condition, **fvs):
        params = dict(fvs)
        sql = 'UPDATE `%s`.`%s` SET ' % (db, tb)

        set_clauses = []
        for f in fvs.keys():
            set_clauses.append('`' + f + '`=%(' + f + ')s')
        sql += ','.join(set_clauses)

        if condition:
            where_clauses = []
            for f, v in condition.items():
                where_clauses.append('`' + f + '`=%(where.' + f + ')s')
                params['where.'+f] = v
            sql += ' WHERE ' + ' AND '.join(where_clauses)
        self.execute(sql, params)
        return self.row_count

    def delete_row(self, db, tb, condition):
        sql = 'DELETE FROM `%s`.`%s` WHERE ' % (db, tb)

        where_clauses = []
        for f, v in condition.items():
            where_clauses.append('`' + f + '`=%(' + f + ')s')
        sql += ' AND '.join(where_clauses)
        self.execute(sql, condition)
        return self.row_count

class AutoIncreaser(object):
    def __init__(self, key, db_pool, db_name, table_name, key_name='key', value_name='value',
                 init_value=0, need_lock=True, alloc_sector=10):
        self.key = key
        self.db_pool = db_pool
        self.db_name = db_name
        self.table_name = table_name
        self.key_name = key_name
        self.value_name = value_name
        self.init_value = init_value
        self.need_lock = need_lock
        self.alloc_sector = alloc_sector
        self.lock = None
        self.number_base = -1
        self.number_offset = 0

    def generateNumber(self):
        if self.lock is None:
            self.lock = Lock(1)

        save_value = None
        next_value = None
        self.lock.acquire()

        if self.number_base >= 0:
            next_value = self.number_base + self.number_offset
            if self.number_offset < self.alloc_sector:
                self.number_offset += 1
            else:
                self.number_base += self.alloc_sector
                self.number_offset = 1
                save_value = next_value + self.alloc_sector
        else:
            self.number_base = self.init_value
            with self.db_pool.fetchConnection() as conn:
                conn.execute("select %(_v)s from %(_d)s.%(_t)s where %(_k)s=%(k)s;",
                             {'_d': self.db_name, '_t': self.table_name, '_k': self.key_name, '_v': self.value_name,
                              'k': self.key})
                if conn.row_count > 0:
                    self.number_base = conn.fetch_all()[0][0]
            next_value = self.number_base
            self.number_offset = 1
            save_value = next_value + self.alloc_sector

        if save_value is not None:
            with self.db_pool.fetchConnection() as conn:
                conn.execute("replace %(_d)s.%(_t)s set %(_k)s=%(k)s, %(_v)s=%(v)s;",
                             {'_d': self.db_name, '_t': self.table_name, '_k': self.key_name, '_v': self.value_name,
                              'k': self.key, 'v': save_value})

        self.lock.release()
        return next_value


