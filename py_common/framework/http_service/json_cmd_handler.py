# coding:utf-8
__author__ = 'HuangZhi'

from framework.framework import Framework
from framework.http_service.http_handler import HttpHandler
from framework.http_service.json_template import JsonTemplates
from framework.utilities.little_tools import md5_hash

import json
import cgi
import time
import traceback


class HttpCmdDispatcher(HttpHandler):
    DEFAULT_RES = {
        'res_code': '400',
        'content_type': 'application/json',
        'content': json.dumps({'result': 'BAD_REQUEST'}),
        'headers': {'Cache-Control': 'no-cache'}
    }

    def __init__(self):
        super(HttpCmdDispatcher, self).__init__()
        self.setLog(Framework.retrieveGlobal('log'))
        self.auth = None
        self.commands = {}
        self.handlers = {}
        self.template_path = None
        self.template_local = None
        self.templates = None

    def setTemplates(self, path, local):
        self.template_path = path
        self.template_local = local
        self.templates = JsonTemplates(self.template_local)

    def on_prepare(self, req):
        super(HttpCmdDispatcher, self).on_prepare(req)
        content = req.get('content', {})
        request = content.get('request', {})
        flags = content.get('flags', {})
        flags.update(request.get('flags', {}))
        cookies = flags.get('cookies', {})
        req['cookies'].update(cookies)

    def onHttpRequest(self, req, res):
        if not self.doCheckPermission(req):
            return 403, {'result': 'FORBIDDEN'}

        content = req['content']
        if 'template_name' in content:
            template = req['env'].get('template')
            if template is None:
                self.log.info('Unknown http template: name=%s' % req['content']['template_name'])
                return 404, {'result': 'UNKNOWN_TEMPLATE'}
            handler = self.commands.get(template.getOperation())
            if not handler:
                handler = self.handlers.get(template.getProcessor())
            if not handler:
                self.log.info('Unsupported http template: name=%s, operation=%s, processor=%s' %
                              (req['content']['template_name'], template.getOperation(), template.getProcessor()))
                return 404, {'result': 'UNSUPPORTED_TEMPLATE'}
        else:
            handler = self.commands.get(req['content']['operation'])
            if not handler:
                self.log.info('Unknown http command: operation=%s' % req['content']['operation'])
                return 404, {'result': 'UNKNOWN_OPERATION'}

        return handler[0](req, res)

    def onDecodeRequest(self, input_stream, length, req):
        content = {'request': 'null', 'auth_info': {'ip': req['env']['REMOTE_ADDR']}}
        try:
            if req['base_path'] == self.template_path:
                content['template_name'] = req['rel_path']
                template = self.templates.fetchTemplate(content['template_name'])
                if template:
                    req['env']['template'] = template
                    content['operation'] = template.getOperation()
                    content['flags'] = template.getFlags()
            else:
                content['operation'] = req['rel_path']

            sign_name = req['env'].get('HTTP_SIGN_NAME', req['params'].get('sign_name', [None])[0])
            req_id = req['env'].get('HTTP_REQ_ID', req['params'].get('req_id', [None])[0])
            req_ts = req['env'].get('HTTP_REQ_TS', req['params'].get('req_ts', [None])[0])
            signature = req['env'].get('HTTP_SIGNATURE', req['params'].get('signature', [None])[0])
            if sign_name and req_id and req_ts and signature:
                content['auth_info'].update({
                    'sign_name': sign_name,
                    'req_id': req_id,
                    'req_ts': req_ts,
                    'signature': signature
                })

            content['request'] = req['params'].get('request', ['null'])[0]
            if input_stream is not None:
                if req['content_type'] == 'application/json':
                    content['request'] = input_stream.read(length)
                else:
    #            if content_type == 'multipart/form-data':
                    fields = cgi.FieldStorage(input_stream, environ=req['env'])
                    for field in fields:
                        content[field] = fields.getvalue(field)

            content['request'] = json.loads(content['request'])
        except:
            content['request'] = None
            self.log.info('Http command decode failed')
        return content

    def onEncodeResponse(self, content, content_type, env):
        template = env.get('template')
        if template is not None:
            return template.join(content), template.getContentType()
        else:
            if content_type == 'application/json':
                return json.dumps(content), content_type
        return content, content_type

    def doCheckPermission(self, req):
        if not self.auth:
            return True
        auth_info = req['content']['auth_info']
        for ip in self.auth.get('trusted_ips', []):
            if ip[-1] != '.':
                if ip == auth_info['ip']:
                    return True
            else:
                if auth_info['ip'][0: len(ip)] == ip:
                    return True

        if 'sign_name' not in auth_info:
            return False
        perm_info = self.auth.get('signatures', {}).get(auth_info['sign_name'])
        if not perm_info:
            return False
        auth_timeout = float(self.auth.get('sign_timeout'))
        if not (-auth_timeout <= (float(auth_info['req_ts']) - time.time()) <= auth_timeout):
            return False

        correct_sign = ':'.join(
            [auth_info['sign_name'], auth_info['req_id'], auth_info['req_ts'], perm_info['secret'],
             req['content']['operation']]).encode('ascii')
        correct_sign = md5_hash(correct_sign)
        if not (auth_info['signature'].upper() == correct_sign.upper()):
            return False
        perms = perm_info['perms']
        if ('_all' in perms) or (req['content']['operation']) in perms:
            return True
        return False

    def setAuthorizeInfo(self, info):
        self.auth = info

    def registerCmdHandler(self, name, handler):
        self.handlers[name] = [handler]
        self.log.info("register rpc handler: %s" % name)

    def loadCommands(self, commands):
        if commands is None:
            self.commands = dict(self.handlers.items())
            for c in self.commands.keys():
                self.log.info("register rpc command: %s, handler=%s" % (c, c))
        else:
            for c in commands:
                if isinstance(c, dict):
                    self.commands[c['command']] = self.handlers[c['handler']]
                    self.log.info("register rpc command: %s, handler=%s" % (c['command'], c['handler']))
                else:
                    self.commands[c] = self.handlers[c]
                    self.log.info("register rpc command: %s, handler=%s" % (c, c))


class CmdBasicHandler(object):
    def __init__(self, log=None):
        super(CmdBasicHandler, self).__init__()
        self.log = log

    def onHttpRequest(self, req, res):
        return self.onRequest(req['content'])

    def onRequest(self, request):
        """
        :param request: req['content'] that is the req data
        :return:
        """
        return 501, {'result': 'NOT_IMPLEMENTED'}

    def setLog(self, log):
        self.log = log
        return self

    def register(self, cmd_handler, dispatch):
        """
        :param cmd_handler: rpc command handler name
        :param dispatch: rpc server
        """
        dispatch.registerCmdHandler(cmd_handler, self.onHttpRequest)
        return self


class SimpleCmdHandler(CmdBasicHandler):
    """
    simple rpc command handler with req object
    """
    def __init__(self, log=None):
        super(SimpleCmdHandler, self).__init__(log)

    def onRequest(self, request):
        try:
            return 200, self.doRequest(request['operation'], request['request'])
        except BaseException, e:
            self.log.error('operation [' + request['operation'] + '] processing failed')
            self.log.error(traceback.format_exc())
            return 200, {'result': 'BAD_REQUEST'}

    def doRequest(self, ope, req):
        """
        :param ope: operation name
        :param req: request params
        :return:
        """
        return 501, {'result': 'NOT_IMPLEMENTED'}


class CmdAsyncTaskHandler(CmdBasicHandler):
    def __init__(self, at, pool, log=None):
        super(CmdAsyncTaskHandler, self).__init__(log)
        self.at = at
        self.pool = pool
        self.pre_result = None

    def setPreResult(self, res):
        self.pre_result = res
        return self

    def preResponse(self):
        return self.pre_result

    def onHttpRequest(self, req, res):
        """
        :param command: req['content'] that is the post data
        :return:
        """
        stub = self.at.postTask(self.pool, req)

        pre_result = self.preResponse()
        if pre_result is not None:
            return 200, pre_result
        else:
            stub.waitForResult()
            return 200, stub.getResult()

