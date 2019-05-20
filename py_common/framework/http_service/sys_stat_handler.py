# coding:utf-8
__author__ = 'HuangZhi'

import json
from framework.framework import Framework as FW


class SysStatHandler(object):
    def __init__(self, req_path):
        super(SysStatHandler, self).__init__()
        self.req_path = req_path
        self.log = None

    def onHttpRequest(self, req, res):
        req_parts = req['rel_path'].strip('/').split('/',)
        params = req['params']
        res['content_type'] = 'application/json'
        r = None
        rc = 200

        try:
            if req_parts[0] == '':
                r = {
                    'app_info': FW.retrieveGlobal('app_info'),
                    'list': ['async_task']
                }
            elif req_parts[0] == 'async_task':
                at = FW.retrieveGlobal('async_tasks')
                if len(req_parts) == 1:
                    r = {
                        'list': at.getPoolList()
                    }
                else:
                    pool_name = req_parts[1]
                    reset = params.get('reset', ['false'])[0] == 'true'
                    r = at.getPoolStatistics(pool_name, reset=reset)
            elif req_parts[0] == '_all':
                r = {'app_info': FW.retrieveGlobal('app_info')}
                at = FW.retrieveGlobal('async_tasks')
                if at is not None:
                    r['async_tasks'] = []
                    reset = params.get('reset', ['false'])[0] == 'true'
                    for pool_name in at.getPoolList():
                        r['async_tasks'].append(at.getPoolStatistics(pool_name, reset=reset))
        except:
            pass

        res['content'] = json.dumps(r, indent=2)
        return rc

    def setLog(self, log):
        self.log = log
        return self

    def register(self, server):
        path = self.req_path
        if self.log:
            self.log.info("register statistic interface: path=%s" % path)
        server.registerHandler(path, self.onHttpRequest, {'res_code': '400'})
        server.registerHandler(path + '/*', self.onHttpRequest, {'res_code': '400'})
        return self
