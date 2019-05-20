# coding:utf-8
__author__ = 'HuangZhi'

from framework.utilities.little_tools import ImageIdentifier
import time
from .http_handler import HttpHandler


rfc1123_date_format = '%a, %d %b %Y %H:%M:%S GMT'


class HbaseHandler(HttpHandler):
    def __init__(self, req_path, table, key, col, cont_type, target_type='folder'):
        super(HbaseHandler, self).__init__()
        self.hbase = None
        self.req_path = req_path
        self.hb_table = table
        self.hb_key = key
        self.hb_col = col
        self.cont_type = cont_type
        self.target_type = target_type
        self.log = None

    def onHttpRequest(self, req, res):
        p = {'req_name': req['rel_path'].replace('/', '.')}
        table = self.hb_table % p
        key = self.hb_key % p
        col = self.hb_col % p

        try:
            with self.hbase.fetchConnection() as hb:
                rs = hb.get_mc(table, key, [col], value_only=False)[col]
            # 判断请求的If-Modified-Since头
            ims = req['env'].get('HTTP_IF_MODIFIED_SINCE', 0)
            timestamp = rs['timestamp']/1000
            gmt = time.gmtime(timestamp)
            ts = time.strftime(rfc1123_date_format, gmt)
            if ims == ts:
                return 304
            # 设置响应头Last-Modified
            res_headers = res.get('headers', {})
            res_headers['Last-Modified'] = ts
            res['headers'] = res_headers
            content = rs['value']

            cont_type = self.cont_type
            if cont_type == 'image':
                cont_type = ImageIdentifier.identifyImageFormat(content)

            res['content'] = content
            res['content_type'] = cont_type
            if self.log:
                self.log.info("http request hbase cell: table='%s', key='%s', col='%s', type='%s'" % (table, key, col, cont_type if cont_type else "unknown"))
        except BaseException, e:
            if self.log:
                self.log.info("hbase cell not found: table='%s', key='%s', col='%s'" % (table, key, col))
            return 404
        return 200

    def setHBase(self, hbase):
        self.hbase = hbase
        return self

    def setLog(self, log):
        self.log = log
        return self

    def __register(self, server):
        path = self.req_path
        if self.target_type == 'folder':
            path += '/*' if path else '*'
        if self.log:
            self.log.info("register hbase handler: path=%s, table=%s, col=%s, key=%s, type=%s" %
                          (path, self.hb_table, self.hb_col, self.hb_key, self.cont_type))
        server.registerHandler(path, self.onHttpRequest, {'res_code': '404'})
        return self

    register = __register

    @classmethod
    def batchRegister(cls, links, hbase, server=None, log=None):
        for r in links:
            cls(*r).setHBase(hbase).setLog(log).register(server)


class DomainHbaseHandler(HbaseHandler):
    """
    hbase image handler with domain
    """
    def __init__(self, req_path, table_fmt, key_fmt, col_fmt, cont_type, target_type='folder'):
        super(DomainHbaseHandler, self).__init__(req_path, table_fmt, key_fmt, col_fmt, cont_type, target_type)
        self.table_fmt = table_fmt
        self.key_fmt = key_fmt
        self.col_fmt = col_fmt

    def request(self, req, res, table, key, col):
        try:
            with self.hbase.fetchConnection() as hb:
                rs = hb.get_mc(table, key, [col], value_only=False)[col]
            # 判断请求的If-Modified-Since头
            ims = req['env'].get('HTTP_IF_MODIFIED_SINCE', 0)
            timestamp = rs['timestamp']/1000
            gmt = time.gmtime(timestamp)
            ts = time.strftime(rfc1123_date_format, gmt)
            if ims == ts:
                return 304
            # 设置响应头Last-Modified
            res_headers = res.get('headers', {})
            res_headers['Last-Modified'] = ts
            res['headers'] = res_headers
            content = rs['value']

            cont_type = self.cont_type
            if cont_type == 'image':
                cont_type = ImageIdentifier.identifyImageFormat(content)

            res['content'] = content
            res['content_type'] = cont_type
            if self.log:
                self.log.info("http request hbase cell: table='%s', key='%s', col='%s', type='%s'" % (table, key, col, cont_type if cont_type else "unknown"))
        except BaseException, e:
            if self.log:
                self.log.info("hbase cell not found: table='%s', key='%s', col='%s'" % (table, key, col))
            return 404
        return 200

    def onHttpRequest(self, req, res):
        domain, pdt_id = req['rel_path'].split('/')
        params = {
            'domain': domain,
            'req_name': pdt_id
        }
        table = self.table_fmt % params
        key = self.key_fmt % params
        col = self.col_fmt % params
        return self.request(req, res, table, key, col)