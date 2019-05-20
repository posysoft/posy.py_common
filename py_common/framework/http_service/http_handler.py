# coding:utf-8
__author__ = 'lilx 14-12-8'

import urllib
from Cookie import SimpleCookie


class HttpHandler(object):
    """
    basic http handler
    """
    DEFAULT_RES = {
        'res_code': '400',
        'content_type': 'text/plain',
        'content': "Bad Request",
        'headers': {'Cache-Control': 'no-cache'}
    }

    def __init__(self):
        self.log = None

    def on_prepare(self, req):
        req['content'] = None

        if req['method'] == 'POST' and req['input_length']:
            req['content'] = self.onDecodeRequest(req['input_stream'], req['input_length'], req)
        else:
            req['content'] = self.onDecodeRequest(None, 0, req)

        req['cookies'] = self.doCookieProcess(req)

    def _onHttpRequest(self, req, res):
        if self.log:
            try:
                self.log.debug('json request: %s, %s' % (req['path'], req['content'].keys()))
            except:
                pass
        r = self.onHttpRequest(req, res)  # code, content, content_type
#        if not isinstance(res['content'], types.StringTypes):
        if isinstance(r, (tuple, list)):
            res['res_code'] = r[0]
            res['content'], res['content_type'] = self.onEncodeResponse(
                r[1],
                r[2] if len(r) > 2 else res['content_type'],
                req['env']
            )
        elif isinstance(r, (int, basestring, long)):
            res['res_code'] = r
        if self.log:
            res_content = res.get('content')
            if not res_content:
                res_content = ''
            elif len(res_content) < 1024:
                try:
                    res_content = res_content.decode('utf8')
                except Exception, e:
                    self.log.error(e.message)
                    res_content = '[content decode error]'
            else:
                res_content = '[long content]'

            self.log.debug(u'json response: %s, %s, %s' % (req['path'], str(res['res_code']), res_content))

    def on_finish(self, req):
        # http response headers
        res = req['res']
        res_cookies = res['headers'].get('cookies', None)
        if res_cookies:
            cookies = []
            for k, v in res_cookies.iteritems():
                cookies.append(('Set-Cookie', v.OutputString()))
            res['headers']['cookies'] = cookies

    def doCookieProcess(self, req):
        cks = SimpleCookie(req['cookie_string'])
        cookies = {}
        for k, m in cks.iteritems():
            cookies[k] = m.value
        return cookies

    def onDecodeRequest(self, input_stream, length, req):
        if input_stream is not None:
            content = input_stream.read(length)
        else:
            content = req['params']
        return content

    def onEncodeResponse(self, content, content_type, env):
        return content, content_type

    def onHttpRequest(self, req, res):
        pass

    def getDefaultRes(self):
        return self.__class__.DEFAULT_RES

    def setLog(self, log):
        self.log = log
        return self

    def register(self, path, server):
        server.registerHandler(path, self._onHttpRequest, self.getDefaultRes())
        if self.log:
            self.log.info('register http handler: path=%s, handler class=%s' % (path, self.__class__.__name__))
        return self

    @staticmethod
    def get_query_param(params, param, default=None):
        s = params.get(param, None)
        if not s:
            return default
        if len(s) > 1:
            return s
        return s[0]


class EchoJsonHandler(HttpHandler):
    def onHttpRequest(self, req, res):
        return 200, req['content']
