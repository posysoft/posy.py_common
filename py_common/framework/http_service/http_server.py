# coding:utf-8
__author__ = 'HuangZhi'

from gevent.wsgi import WSGIServer as WebServer
#from wsgiref.simple_server import make_server as WebServer
import urlparse
import copy


class HttpServer:
    RESULT_CODES = {
        '200': ['OK', 'text/plain', '200 OK'],
        '400': ['Bad Request', 'text/plain', '400 Bad Request'],
        '401': ['Unauthorized', 'text/plain', '401 Unauthorized'],
        '403': ['Forbidden', 'text/plain', '403 Forbidden'],
        '404': ['Not Found', 'text/plain', '404 Not Found'],
        '500': ['Internal Server Error', 'text/plain', '500 Internal Server Error'],
        '501': ['Not Implemented', 'text/plain', '501 Not Implemented'],
        '304': ['Not Modified', 'text/plain', '']
    }
#    DEFAULT_RES_404 = {
#        'res_code': '404'
#        'res_string': RESULT_CODES['404'],
#        'content': '<html><head><meta charset="UTF-8" /><title>你妹！</title></head><body>不知道你想要什么。。。</body></html>',
#        'content_type': 'text/html'
#        'headers': {
#                    'Content-Type': 'text/html'
#        },
#    }

    def __init__(self, addr, port, desc=None):
        self.description = desc.encode('utf-8') if desc else None
        self.addr = addr
        self.port = port
        self._server = WebServer((addr, port), self.onHttpRequest)
        self._handlers = {}
        self.registerHandler('*', None, {'res_code': '404'})
        self.middleware = []

    def register_middleware(self, middleware):
        if middleware not in self.middleware:
            self.middleware.append(middleware)

    def registerHandler(self, path, processor, default_res=None):
        self._handlers[path] = {
            'path': path,
            'processor': processor,
            'default_res': default_res
        }

    def onHttpRequest(self, environ, start_response):
        path = environ['PATH_INFO'].strip('/')
        base_path = ''
        rel_path = ''
        path_parts = path.split('/')

        handler = self._handlers.get(path, None)
        if not handler:
            # 对path从后面逐段缩短匹配
            # rel_path为匹配handler path后面部分
            for i in range(len(path_parts)-1, -1, -1):
                _path = '/'.join(path_parts[0:i] + ['*'])
                handler = self._handlers.get(_path, None)
                if handler:
                    base_path = '/'.join(path_parts[0:i])
                    rel_path = '/'.join(path_parts[i:])
                    break

        params = urlparse.parse_qs(environ['QUERY_STRING'], True)
        req_type = environ.get('CONTENT_TYPE')
        req_type = req_type.split(';')[0].strip() if req_type else req_type
        req = {
            'method': environ['REQUEST_METHOD'],
            'path': path,
            'base_path': base_path,
            'rel_path': rel_path,
            'path_parts': path_parts,
            'env': environ,
            'params': params,
            'input_length': int(environ.get('CONTENT_LENGTH', 0)),
            'input_stream': environ.get('wsgi.input', None),
            'cookie_string': environ.get('HTTP_COOKIE', ''),
            'content_type': req_type,
            'res': None,       # response data
            'session': None,   # session
            'cookies': None    # request cookies
            #'response': start_response
        }

        res = copy.deepcopy(handler.get('default_res', {'res_code': '501'}))
        # transmit the res to req
        req['res'] = res
        res['headers'] = res.get('headers', {})
        if self.description:
            res['headers']['Server'] = self.description

        processor = handler.get('processor', None)
        if processor:
            inst = getattr(processor, '__self__', None) or processor
            if getattr(inst, 'on_prepare', None):
                inst.on_prepare(req)
            for mw in self.middleware:
                method = getattr(mw, 'on_request', None)
                if method:
                    method(req)
            r = processor(req, res)
            for mw in self.middleware:
                method = getattr(mw, 'on_response', None)
                if method:
                    method(req)
            if getattr(inst, 'on_finish', None):
                inst.on_finish(req)
            res['res_code'] = str(r) if r else res['res_code']

        res['res_code'] = str(res['res_code'])
        res['res_string'] = res.get('res_string', self.__class__.RESULT_CODES[res['res_code']][0])
        res['content'] = res.get('content', self.__class__.RESULT_CODES[res['res_code']][2])
        res['headers']['Content-Type'] = \
            res.get('content_type', res['headers'].get('Content-Type', self.__class__.RESULT_CODES[res['res_code']][1]))
        res['headers']['Content-Length'] = res['headers'].get('Content-Length', str(len(res['content'])))

        headers = []
        for k, v in res['headers'].iteritems():
            if 'cookies' == k:
                headers.extend(v)
            else:
                headers.append((k, v))
        start_response(res['res_code']+' '+res['res_string'], headers)
        return res['content'],  # <- DO NOT REMOVE this COMMA!!!

    def serve_forever(self, stop_timeout=None):
        self._server.serve_forever(stop_timeout)

    def quit_serving(self):
        self._server.close()

    def start(self):
        self._server.start()
