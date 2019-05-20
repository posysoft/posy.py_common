# coding:utf-8
__author__ = 'HuangZhi'

from framework.framework import Framework
from framework.utilities.little_tools import ImageIdentifier, FileExtIdentifier
import os
import glob
import time
import re
from framework.http_service.http_handler import HttpHandler



class StaticHandler(HttpHandler):
    rfc1123_date_format = '%a, %d %b %Y %H:%M:%S GMT'

    def __init__(self, req_path, loc_path, target_type='folder'):
        super(StaticHandler, self).__init__()
        self.req_path = req_path.strip()
        self.loc_path = loc_path.strip()
        self.target_type = target_type.strip()
        self.log = None

    def onHttpRequest(self, req, res):
        path = self.loc_path
        if self.target_type == 'browse':
            path += '' if '*' in path else ('*' if path[-1] == '/' else '/*')
            try:
                content = '''
                    <html>
                        <head>
                            <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
                            <title>File list</title>
                        </head>
                        <body>
                            <table border="1" cellspacing="0" cellpadding="5px">
                                <tr>
                                    <td style="font-weight:900" align="center">File List:</td>
                                </tr>
                    '''
                files = [os.path.basename(f) for f in glob.glob(path)]
                files.sort()
                for f in files:
                    content += '<tr><td><a href="'+self.req_path+'/'+f+'">'+f+'</a></td></tr>\n'
                content += '''
                            </table>
                        </body>
                    </html>
                    '''
                res['content'] = content.encode('utf-8')
                res['content_type'] = 'text/html'
                if self.log:
                    self.log.info("http request browse folder: %s" % (path, ))
            except BaseException, e:
                self.log.info("browse folder not found: '%s'" % path)
                return 404
        else:
            if req['rel_path']:
                path += '/' + req['rel_path'].decode('utf-8')
                abs_loc_path = os.path.abspath(self.loc_path.strip())
                abs_path = os.path.abspath(path)
                if not abs_path.startswith(abs_loc_path):
                    return 400
            try:
                # 判断请求的If-Modified-Since头
                ims = req['env'].get('HTTP_IF_MODIFIED_SINCE', 0)
                mt = time.gmtime(os.path.getmtime(path))
                mts = time.strftime(self.rfc1123_date_format, mt)
                if ims == mts:
                    return 304
                with open(path, 'rb') as f:
                    content = f.read()
                # 设置响应头Last-Modified
                res_headers = res.get('headers', {})
                res_headers['Last-Modified'] = mts
                res['headers'] = res_headers
                cont_type = FileExtIdentifier.identifyExtFormat(path)
                cont_type = cont_type if cont_type else ImageIdentifier.identifyImageFormat(content)
                cont_type = cont_type if cont_type else "application/octect-stream".encode('utf8')

                res['content'] = content
                res['content_type'] = cont_type
                if self.log:
                    self.log.info("http request static file: %s (%s)" % (path, cont_type if cont_type else "unknown"))
            except BaseException, e:
                if self.log:
                    self.log.info("static file not found: '%s'" % path)
                return 404
        return 200

    def setLog(self, log):
        self.log = log
        return self

    def register(self, path=None, server=None):
        path = self.req_path
        if self.target_type == 'folder':
            path += '/*' if path else '*'
        server = server if server else Framework.retrieveGlobal('http_server')
        server.registerHandler(path, self.onHttpRequest, {'res_code': '404'})
        self.log.info('register static handler: path=%s, local=%s' % (path, self.loc_path))
        return self

    @staticmethod
    def batchRegister(links, server=None, log=None):
        for r in links:
            StaticHandler(*r).setLog(log).register(server)
