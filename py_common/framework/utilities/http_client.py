# coding:utf-8
__author__ = 'HuangZhi'
import urllib, urllib2, httplib
import json
import base64


def doHttpRequest(url, params, encode='url', method='post', decode=None, headers=None):
    url_req = None
    if method == 'post':
        url_req = urllib2.Request(url.encode('utf-8'))
        req_data = ''
        if encode == 'url':
            req_data = urllib.urlencode(params)
        elif encode == 'json':
            req_data = json.dumps(params)
            url_req.add_header('Content-Type', "application/json")
        elif encode == 'multipart':
            BOUNDARY = "------------------------7dc2fd5c0894"
            url_req.add_header('Content-Type', "multipart/form-data; boundary=" + BOUNDARY)
            req_data = ''
            for n, v in params.iteritems():
                f = None
                t = None
                if isinstance(v, dict):
                    f = v.get('filename')
                    t = v.get('type')
                    v = v['content']

                req_data += '--' + BOUNDARY + '\r\n' + \
                            'Content-Disposition: form-data; name="' + n + '"' + \
                            (('; filename="' + f + '"') if f else '') + \
                            (('\r\nContent-Type: ' + t) if t else '') + \
                            '\r\n\r\n' + v + '\r\n'
            req_data += "--" + BOUNDARY + "--\r\n"
        url_req.add_data(req_data)
    elif method == 'get':
        if params:
            req_data = urllib.urlencode(params)
            url = url.encode('utf-8') + '?' + req_data
        url_req = urllib2.Request(url.encode('utf-8'))

    try:
        if headers:
            for k, v in headers.items():
                url_req.add_header(k, v)
        res = urllib2.urlopen(url_req)
        r = res.read()

        if decode == 'json':
            r = json.loads(r)
        return 200, r
    except httplib.HTTPException, e:
        return -1, 'HTTP error'
    except urllib2.HTTPError, e:
        return e.code, e.msg
    except IOError, e:
        return -1, 'Connection failed'


def doRpcRequest(url, req, attachments=None):
    params = req
    encode = 'json'
    if attachments:
        params = dict(attachments)
        if req:
            params['request'] = req
        encode = 'multipart'
    return doHttpRequest(url, params, encode, 'post', 'json')


def doAsyncHttpRequest(at, pool, params, encode=None, method=None, decode=None, url=None, priority=0, key=None, stub=None):
    req = {'params': params}
    if url is not None:
        req['url'] = url
    if encode is not None:
        req['encode'] = encode
    if method is not None:
        req['method'] = method
    if decode is not None:
        req['decode'] = decode

    stub = at.postTask(pool, req, priority=priority, key=key, stub=stub)
    return stub


def doAsyncHttpRequestForResult(at, pool, params, encode=None, method=None, decode=None, url=None, priority=0):
    stub = doAsyncHttpRequest(at, pool, params, encode, method, decode, url, priority)
    stub.waitForResult()
    return stub.getResult()


def doAsyncRpcRequest(at, pool, req, attachments=None, url=None, priority=0, key=None, stub=None):
    params = req
    encode = 'json'
    if attachments:
        params = dict(attachments)
        if req is not None:
            params['request'] = json.dumps(req)
        encode = 'multipart'

    return doAsyncHttpRequest(at, pool, params, encode=encode, method='post', decode='json',
                              url=url, priority=priority, key=key, stub=stub)


def doAsyncRpcRequestForResult(at, pool, req, attachments=None, url=None, priority=0):
    stub = doAsyncRpcRequest(at, pool, req, attachments, url, priority)
    stub.waitForResult()
    return stub.getResult()


def decodeDataUrl(url):
        res = None
        try:
            head, data = url.split(',', 1)
            heads = head.split(':', 1)[1].split(';')
            if 'base64' in heads:
                res = base64.b64decode(data)
        except BaseException, e:
            pass
        return res
