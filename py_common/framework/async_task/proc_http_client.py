# coding:utf-8
__author__ = 'HuangZhi'

import logging
from framework.async_task.async_task import BasicAsyncProcessor
from framework.utilities.http_client import doHttpRequest


class HttpClientProcessor(BasicAsyncProcessor):
    def __init__(self, log=None, url=None, urls=None, encode=None, method=None):
        super(HttpClientProcessor, self).__init__(log)
        self.url = url
        self.urls = urls
        self.encode = encode
        self.method = method
        if not self.log:
            self.log = logging

    def onTask(self, req, priority):
        url = req.get('url', '')
        url = url if url.find('http://') == 0 else (self.url + url)
        return self.postHttpRequest(url,
                                    req.get('params', {}), req.get('encode'), req.get('method'), req.get('decode'))

    def onEnter(self, index):
        super(HttpClientProcessor, self).onEnter(index)
        if self.urls and not self.url:
            urls = []
            for u in self.urls:
                if isinstance(u, (list, tuple)):
                    urls.extend(u[0] for i in range(u[1] if len(u) > 1 else 1))
                else:
                    urls.append(u)
            self.url = urls[index % len(urls)]

    def postHttpRequest(self, url, params, encode=None, method=None, decode=None):
        code, res = doHttpRequest(url, params,
                                  encode if encode else (self.encode if self.encode else 'url'),
                                  method if method else (self.method if self.method else 'post'),
                                  decode)

        if code != 200:
            if self.log:
                self.log.info('http request to "%s" failed: %s, %s' % (url, str(code), res))

        return code, res
